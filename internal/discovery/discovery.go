package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/relaymesh/relaymesh/internal/node"
)

type Discovery struct {
	nodeID        node.NodeID
	address       string
	port          uint16
	dataPort      uint16
	conn          *net.UDPConn
	peers         *PeerList
	logger        *slog.Logger
	interval      time.Duration
	peerTimeout   time.Duration
	sequence      uint64
	knownPorts    []uint16
	mu            sync.RWMutex
	stopCh        chan struct{}
	onPeer        func(*Peer)
	onPeerLost    func(node.NodeID)
	remoteAdvertise map[node.NodeID]uint16 // peer -> public data port (srflx)
}

type DiscoveryConfig struct {
	NodeID      node.NodeID
	Address     string
	Port        uint16
	DataPort    uint16
	Interval    time.Duration
	PeerTimeout time.Duration
	KnownPorts  []uint16
}

type HeartbeatMessage struct {
	Type             string                  `json:"type"`
	NodeID           node.NodeID             `json:"node_id"`
	Address          string                  `json:"address"`
	Port             uint16                  `json:"port"`
	DataPort         uint16                  `json:"data_port"`
	Sequence         uint64                  `json:"sequence"`
	Capabilities     []Capability            `json:"capabilities"`
	KnownPeers       []node.NodeID           `json:"known_peers"`
	KnownRemotePeers map[node.NodeID]uint16  `json:"known_remote_peers,omitempty"`
	Timestamp        time.Time               `json:"timestamp"`
}

const MaxPacketSize = 4096

func New(config DiscoveryConfig, logger *slog.Logger) *Discovery {
	return &Discovery{
		nodeID:          config.NodeID,
		address:         config.Address,
		port:            config.Port,
		dataPort:        config.DataPort,
		peers:           NewPeerList(),
		logger:          logger,
		interval:        config.Interval,
		peerTimeout:     config.PeerTimeout,
		knownPorts:      config.KnownPorts,
		stopCh:          make(chan struct{}),
		remoteAdvertise: make(map[node.NodeID]uint16),
	}
}

// OnPeer registers a callback invoked when a new peer is discovered.
func (d *Discovery) OnPeer(cb func(*Peer)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onPeer = cb
}

// OnPeerLost registers a callback invoked when a peer times out.
func (d *Discovery) OnPeerLost(cb func(node.NodeID)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onPeerLost = cb
}

// AdvertiseRemote tells discovery that peer has a publicly reachable
// (server-reflexive) data-plane port. Heartbeats will include this in a
// KnownRemotePeers list so LAN peers can also route to it, and the peer
// entry is created/kept alive here so the cleanup loop does not evict it
// while signaling keeps refreshing it.
func (d *Discovery) AdvertiseRemote(peer node.NodeID, publicDataPort uint16) {
	d.mu.Lock()
	d.remoteAdvertise[peer] = publicDataPort
	d.mu.Unlock()
}

// AddCandidate injects a peer learned out-of-band (via signaling). The
// peer is kept alive by repeated calls from the signaling client's
// refresh loop; once refresh stops, the cleanup loop evicts it normally.
func (d *Discovery) AddCandidate(id node.NodeID, addr string, port, dataPort uint16) {
	d.mu.Lock()
	defer d.mu.Unlock()

	peer, exists := d.peers.Get(id)
	if !exists {
		peer = NewPeerWithData(id, addr, port, dataPort)
		d.peers.Add(peer)
		d.logger.Info("added remote candidate",
			"peer_id", id, "address", addr, "port", port, "data_port", dataPort)
		if d.onPeer != nil {
			cb := d.onPeer
			go cb(peer)
		}
	}
	peer.Address = addr
	peer.Port = port
	if dataPort != 0 {
		peer.DataPort = dataPort
	}
	// Touch LastSeen so cleanup does not evict before the next refresh.
	peer.LastSeen = time.Now()
}

func (d *Discovery) Start(ctx context.Context) error {
	d.logger.Info("starting discovery",
		"node_id", d.nodeID,
		"address", d.address,
		"port", d.port,
	)

	addr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf(":%d", d.port))
	if err != nil {
		return err
	}

	d.conn, err = net.ListenUDP("udp4", addr)
	if err != nil {
		return err
	}

	go d.listenLoop(ctx)
	go d.sendLoop(ctx)
	go d.cleanupLoop(ctx)

	return nil
}

func (d *Discovery) Stop() error {
	close(d.stopCh)
	if d.conn != nil {
		return d.conn.Close()
	}
	return nil
}

func (d *Discovery) listenLoop(ctx context.Context) {
	buf := make([]byte, MaxPacketSize)
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.stopCh:
			return
		default:
		}

		d.conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, _, err := d.conn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			return
		}

		var msg HeartbeatMessage
		if err := json.Unmarshal(buf[:n], &msg); err != nil {
			continue
		}

		if msg.NodeID == d.nodeID {
			continue
		}

		d.handleHeartbeat(msg)
	}
}

func (d *Discovery) handleHeartbeat(msg HeartbeatMessage) {
	d.mu.Lock()
	defer d.mu.Unlock()

	peer, exists := d.peers.Get(msg.NodeID)
	if !exists {
		dataPort := msg.DataPort
		if dataPort == 0 {
			dataPort = msg.Port
		}
		peer = NewPeerWithData(msg.NodeID, msg.Address, msg.Port, dataPort)
		d.peers.Add(peer)
		d.logger.Info("discovered peer",
			"peer_id", msg.NodeID,
			"address", msg.Address,
			"port", msg.Port,
			"data_port", dataPort,
		)
		if d.onPeer != nil {
			cb := d.onPeer
			go cb(peer)
		}
	}

	peer.Update(PeerMetrics{
		Sequence:   msg.Sequence,
		DataPort:   msg.DataPort,
		KnownPeers: msg.KnownPeers,
	})
	if msg.DataPort != 0 {
		peer.DataPort = msg.DataPort
	}

	// A remote (public) candidate for another node overrides the private
	// address we would otherwise use for its data plane.
	if msg.KnownRemotePeers != nil {
		for rid, rport := range msg.KnownRemotePeers {
			if rid == d.nodeID {
				continue
			}
			if rp, ok := d.peers.Get(rid); ok && rport != 0 {
				rp.DataPort = rport
			}
		}
	}
}

func (d *Discovery) sendLoop(ctx context.Context) {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.stopCh:
			return
		case <-ticker.C:
			d.mu.Lock()
			d.sequence++
			seq := d.sequence
			known := make([]node.NodeID, 0, len(d.peers.peers))
			for id := range d.peers.peers {
				known = append(known, id)
			}
			d.mu.Unlock()

			msg := HeartbeatMessage{
				Type:         "heartbeat",
				NodeID:       d.nodeID,
				Address:      d.address,
				Port:         d.port,
				DataPort:     d.dataPort,
				Sequence:     seq,
				Capabilities: []Capability{CapabilityRelay, CapabilityRelayMesh},
				KnownPeers:   known,
				Timestamp:    time.Now(),
			}
			if len(d.remoteAdvertise) > 0 {
				msg.KnownRemotePeers = make(map[node.NodeID]uint16, len(d.remoteAdvertise))
				for pid, pport := range d.remoteAdvertise {
					msg.KnownRemotePeers[pid] = pport
				}
			}

			data, err := json.Marshal(msg)
			if err != nil {
				continue
			}

			for _, port := range d.knownPorts {
				if port == d.port {
					continue
				}
				addr := fmt.Sprintf("%s:%d", d.address, port)
				udpAddr, err := net.ResolveUDPAddr("udp4", addr)
				if err != nil {
					continue
				}
				d.conn.WriteToUDP(data, udpAddr)
			}
		}
	}
}

func (d *Discovery) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(d.interval * 3)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.stopCh:
			return
		case <-ticker.C:
			d.mu.Lock()
			var lost []node.NodeID
			var lostCb func(node.NodeID)
			for _, peer := range d.peers.List() {
				if !peer.IsAlive(d.peerTimeout) {
					d.peers.Remove(peer.ID)
					lost = append(lost, peer.ID)
					d.logger.Info("peer removed (timeout)", "peer_id", peer.ID)
				}
			}
			lostCb = d.onPeerLost
			d.mu.Unlock()
			if lostCb != nil {
				for _, id := range lost {
					go lostCb(id)
				}
			}
		}
	}
}

func (d *Discovery) Peers() []*Peer {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.peers.List()
}

// Snapshot returns deep copies of known peers safe for use without holding the lock.
func (d *Discovery) Snapshot() []*Peer {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]*Peer, 0, d.peers.Count())
	for _, p := range d.peers.List() {
		cp := *p
		if p.KnownPeers != nil {
			kp := make([]node.NodeID, len(p.KnownPeers))
			copy(kp, p.KnownPeers)
			cp.KnownPeers = kp
		}
		if p.Capabilities != nil {
			caps := make([]Capability, len(p.Capabilities))
			copy(caps, p.Capabilities)
			cp.Capabilities = caps
		}
		out = append(out, &cp)
	}
	return out
}

func (d *Discovery) PeerCount() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.peers.Count()
}

func GetLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "0.0.0.0"
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return "0.0.0.0"
}
