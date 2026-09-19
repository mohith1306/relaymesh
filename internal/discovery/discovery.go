package discovery

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/relaymesh/relaymesh/internal/node"
)

type Discovery struct {
	nodeID       node.NodeID
	address      string
	port         uint16
	peers        *PeerList
	sender       *HeartbeatSender
	listener     *HeartbeatListener
	logger       *slog.Logger
	interval     time.Duration
	peerTimeout  time.Duration
	mu           sync.RWMutex
}

type DiscoveryConfig struct {
	NodeID       node.NodeID
	Address      string
	Port         uint16
	Interval     time.Duration
	PeerTimeout  time.Duration
	KnownPorts   []uint16
}

func New(config DiscoveryConfig, logger *slog.Logger) *Discovery {
	return &Discovery{
		nodeID:      config.NodeID,
		address:     config.Address,
		port:        config.Port,
		peers:       NewPeerList(),
		logger:      logger,
		interval:    config.Interval,
		peerTimeout: config.PeerTimeout,
	}
}

func (d *Discovery) Start(ctx context.Context) error {
	d.logger.Info("starting discovery",
		"node_id", d.nodeID,
		"address", d.address,
		"port", d.port,
	)

	d.sender = NewHeartbeatSender(d.nodeID, d.address, d.port, d.interval)
	if err := d.sender.Start([]uint16{9001, 9002, 9003, 9004, 9005}); err != nil {
		return err
	}

	d.listener = NewHeartbeatListener(d.nodeID, d.port)
	if err := d.listener.Start(); err != nil {
		return err
	}

	d.listener.OnHeartbeat(func(msg HeartbeatMessage) {
		d.handleHeartbeat(msg)
	})

	go d.listener.Listen()
	go d.sendLoop(ctx)
	go d.cleanupLoop(ctx)

	return nil
}

func (d *Discovery) Stop() error {
	d.logger.Info("stopping discovery", "node_id", d.nodeID)

	if d.sender != nil {
		if err := d.sender.Stop(); err != nil {
			d.logger.Warn("error stopping sender", "error", err)
		}
	}

	if d.listener != nil {
		if err := d.listener.Stop(); err != nil {
			d.logger.Warn("error stopping listener", "error", err)
		}
	}

	return nil
}

func (d *Discovery) handleHeartbeat(msg HeartbeatMessage) {
	d.mu.Lock()
	defer d.mu.Unlock()

	peer, exists := d.peers.Get(msg.NodeID)
	if !exists {
		peer = NewPeer(msg.NodeID, msg.Address, msg.Port)
		d.peers.Add(peer)
		d.logger.Info("discovered peer",
			"peer_id", msg.NodeID,
			"address", msg.Address,
			"port", msg.Port,
		)
	}

	peer.Update(PeerMetrics{
		Sequence: msg.Sequence,
	})
}

func (d *Discovery) sendLoop(ctx context.Context) {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := d.sender.Send(); err != nil {
				d.logger.Warn("failed to send heartbeat", "error", err)
			}
		}
	}
}

func (d *Discovery) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(d.interval * 2)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.cleanup()
		}
	}
}

func (d *Discovery) cleanup() {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, peer := range d.peers.List() {
		if !peer.IsAlive(d.peerTimeout) {
			d.peers.Remove(peer.ID)
			d.logger.Info("peer removed (timeout)",
				"peer_id", peer.ID,
				"last_seen", peer.LastSeen,
			)
		}
	}
}

func (d *Discovery) Peers() []*Peer {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.peers.List()
}

func (d *Discovery) PeerCount() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.peers.Count()
}

func (d *Discovery) GetPeer(id node.NodeID) (*Peer, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.peers.Get(id)
}
