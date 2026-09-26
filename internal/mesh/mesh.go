package mesh

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/relaymesh/relaymesh/internal/discovery"
	"github.com/relaymesh/relaymesh/internal/forwarding"
	"github.com/relaymesh/relaymesh/internal/node"
	"github.com/relaymesh/relaymesh/internal/routing"
	"github.com/relaymesh/relaymesh/internal/transport"
)

const (
	DefaultLatency   = 10.0
	DefaultBandwidth = 100.0
	DefaultLoss      = 0.0
	syncInterval     = 500 * time.Millisecond
)

type Config struct {
	NodeID              node.NodeID
	Address             string
	DiscoveryPort       uint16
	DataPort            uint16
	KnownDiscoveryPorts []uint16
	HeartbeatInterval   time.Duration
	PeerTimeout         time.Duration
}

type Stats struct {
	PeerCount int
	Routes    int
	Sent      uint64
	Forwarded uint64
	Delivered uint64
	Dropped   uint64
}

// MeshNode wires discovery -> routing -> forwarding -> transport into a
// working data plane. This is the Phase 1 v1 core: real UDP packets,
// real multi-hop relay (A -> B -> C), real Dijkstra next-hop lookups.
type MeshNode struct {
	config    Config
	logger    *slog.Logger
	discovery *discovery.Discovery
	transport *transport.UDPTransport
	router    *routing.Router
	forwarder *forwarding.Forwarder

	delivered chan forwarding.DeliveredPacket
	sent      atomic.Uint64
	seq       atomic.Uint64

	mu     sync.Mutex
	closed bool
}

func New(cfg Config, logger *slog.Logger) *MeshNode {
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 5 * time.Second
	}
	if cfg.PeerTimeout == 0 {
		cfg.PeerTimeout = 30 * time.Second
	}

	tr := transport.NewUDP(cfg.NodeID)
	router := routing.New(cfg.NodeID, logger.With("component", "routing"))
	fwd := forwarding.New(cfg.NodeID, tr, logger.With("component", "forwarding"))

	m := &MeshNode{
		config:    cfg,
		logger:    logger,
		transport: tr,
		router:    router,
		forwarder: fwd,
		delivered: make(chan forwarding.DeliveredPacket, 100),
	}
	fwd.OnDelivered(func(p forwarding.DeliveredPacket) {
		select {
		case m.delivered <- p:
		default:
			m.logger.Warn("delivered channel full, dropping notification", "packet_id", p.ID)
		}
	})

	discCfg := discovery.DiscoveryConfig{
		NodeID:      cfg.NodeID,
		Address:     cfg.Address,
		Port:        cfg.DiscoveryPort,
		DataPort:    cfg.DataPort,
		Interval:    cfg.HeartbeatInterval,
		PeerTimeout: cfg.PeerTimeout,
		KnownPorts:  cfg.KnownDiscoveryPorts,
	}
	m.discovery = discovery.New(discCfg, logger.With("component", "discovery"))
	m.discovery.OnPeerLost(func(id node.NodeID) {
		m.router.RemoveLink(cfg.NodeID, id)
		m.forwarder.RemoveFromForwardingTable(id)
		m.syncForwardingTable()
	})

	return m
}

func (m *MeshNode) Start(ctx context.Context) error {
	if err := m.transport.Listen(fmt.Sprintf(":%d", m.config.DataPort)); err != nil {
		return fmt.Errorf("transport listen: %w", err)
	}
	if err := m.discovery.Start(ctx); err != nil {
		m.transport.Close()
		return fmt.Errorf("discovery start: %w", err)
	}

	go m.syncLoop(ctx)
	go m.receiveLoop(ctx)

	m.logger.Info("mesh node started",
		"node_id", m.config.NodeID,
		"discovery_port", m.config.DiscoveryPort,
		"data_port", m.config.DataPort,
	)
	return nil
}

func (m *MeshNode) Stop() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	m.mu.Unlock()

	_ = m.discovery.Stop()
	return m.transport.Close()
}

func (m *MeshNode) ID() node.NodeID { return m.config.NodeID }

func (m *MeshNode) Discovery() *discovery.Discovery { return m.discovery }

func (m *MeshNode) Router() *routing.Router { return m.router }

func (m *MeshNode) Forwarder() *forwarding.Forwarder { return m.forwarder }

// Delivered returns a channel receiving packets destined for this node.
func (m *MeshNode) Delivered() <-chan forwarding.DeliveredPacket { return m.delivered }

// OnDelivered registers an additional delivery callback alongside the channel.
func (m *MeshNode) OnDelivered(cb func(forwarding.DeliveredPacket)) {
	m.forwarder.OnDelivered(func(p forwarding.DeliveredPacket) {
		select {
		case m.delivered <- p:
		default:
		}
		cb(p)
	})
}

// Send transmits payload to dest via the computed mesh route.
func (m *MeshNode) Send(dest node.NodeID, payload []byte) error {
	if dest == m.config.NodeID {
		return fmt.Errorf("cannot send to self")
	}
	nextHop, ok := m.router.GetNextHop(dest)
	if !ok {
		return fmt.Errorf("no route to %s", dest)
	}
	seq := m.seq.Add(1)
	pkt := forwarding.NewPacket(m.config.NodeID, dest, payload, forwarding.DefaultTTL)
	pkt.Sequence = seq
	// Path starts empty; Forwarder appends each handling node,
	// beginning with the originator, so HasVisited never matches self here.
	if err := m.forwarder.Forward(pkt, nextHop); err != nil {
		return fmt.Errorf("forward to %s via %s: %w", dest, nextHop, err)
	}
	m.sent.Add(1)
	m.logger.Info("packet sent", "dest", dest, "next_hop", nextHop, "seq", seq)
	return nil
}

func (m *MeshNode) Stats() Stats {
	fs := m.forwarder.Stats()
	return Stats{
		PeerCount: m.discovery.PeerCount(),
		Routes:    len(m.router.GetTable().GetAllDestinations()),
		Sent:      m.sent.Load(),
		Forwarded: fs.Forwarded,
		Delivered: fs.Delivered,
		Dropped:   fs.Dropped,
	}
}

func (m *MeshNode) syncLoop(ctx context.Context) {
	ticker := time.NewTicker(syncInterval)
	defer ticker.Stop()
	m.sync()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.sync()
		}
	}
}

// sync is the heart of Phase 1 wiring:
//  1. discovered peers -> transport connections + direct graph links
//  2. neighbors' neighbor lists -> indirect graph links (2-hop visibility)
//  3. Dijkstra results -> forwarding table next-hops
func (m *MeshNode) sync() {
	self := m.config.NodeID
	peers := m.discovery.Snapshot()

	for _, p := range peers {
		dataAddr := peerDataAddress(p, m.config.Address)
		if err := m.transport.Connect(p.ID, dataAddr); err != nil {
			m.logger.Debug("transport connect failed", "peer", p.ID, "addr", dataAddr, "error", err)
			continue
		}
		m.router.UpdateLink(self, p.ID, DefaultLatency, DefaultBandwidth, DefaultLoss)

		for _, remote := range p.KnownPeers {
			if remote == self {
				continue
			}
			m.router.UpdateLink(p.ID, remote, DefaultLatency, DefaultBandwidth, DefaultLoss)
		}
	}

	// Drop learned links nobody has re-advertised: the path behind
	// them is gone, so routes must be recomputed without them.
	m.router.PruneStaleLinks(m.config.PeerTimeout)
	m.syncForwardingTable()
}

func (m *MeshNode) syncForwardingTable() {
	dests := m.router.GetTable().GetAllDestinations()
	want := make(map[node.NodeID]node.NodeID, len(dests))
	for _, dest := range dests {
		if nextHop, ok := m.router.GetNextHop(dest); ok {
			want[dest] = nextHop
			m.forwarder.UpdateForwardingTable(dest, nextHop)
		}
	}
	for dest, nextHop := range m.forwarder.GetForwardingTable().GetAll() {
		if w, ok := want[dest]; !ok || w != nextHop {
			m.forwarder.RemoveFromForwardingTable(dest)
		}
	}
}

func (m *MeshNode) receiveLoop(ctx context.Context) {
	for {
		type result struct {
			pkt  *transport.Packet
			from node.NodeID
		}
		ch := make(chan result, 1)
		go func() {
			pkt, from, _ := m.transport.Receive()
			if pkt != nil {
				ch <- result{pkt: pkt, from: from}
			}
		}()
		select {
		case <-ctx.Done():
			return
		case r := <-ch:
			m.forwarder.HandleReceived(r.pkt, r.from)
		}
	}
}

func peerDataAddress(p *discovery.Peer, selfAddr string) string {
	host := p.Address
	if host == "" || host == "0.0.0.0" {
		host = "127.0.0.1"
		_ = selfAddr
	}
	port := p.DataPort
	if port == 0 {
		port = p.Port
	}
	return fmt.Sprintf("%s:%d", host, port)
}
