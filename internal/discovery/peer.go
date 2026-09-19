package discovery

import (
	"fmt"
	"time"

	"github.com/relaymesh/relaymesh/internal/node"
)

type Capability string

const (
	CapabilityRelay   Capability = "relay"
	CapabilityGateway Capability = "gateway"
	CapabilityRelayMesh Capability = "relaymesh"
)

type Peer struct {
	ID           node.NodeID
	Address      string
	Port         uint16
	Latency      time.Duration
	Bandwidth    float64
	PacketLoss   float64
	LastSeen     time.Time
	Capabilities []Capability
	Sequence     uint64
}

func NewPeer(id node.NodeID, address string, port uint16) *Peer {
	return &Peer{
		ID:           id,
		Address:      address,
		Port:         port,
		LastSeen:     time.Now(),
		Capabilities: []Capability{CapabilityRelay, CapabilityRelayMesh},
	}
}

func (p *Peer) IsAlive(timeout time.Duration) bool {
	return time.Since(p.LastSeen) < timeout
}

func (p *Peer) Update(metrics PeerMetrics) {
	p.Latency = metrics.Latency
	p.Bandwidth = metrics.Bandwidth
	p.PacketLoss = metrics.PacketLoss
	p.LastSeen = time.Now()
	p.Sequence = metrics.Sequence
}

func (p *Peer) AddressString() string {
	return fmt.Sprintf("%s:%d", p.Address, p.Port)
}

type PeerMetrics struct {
	Latency    time.Duration
	Bandwidth  float64
	PacketLoss float64
	Sequence   uint64
}

type PeerList struct {
	peers map[node.NodeID]*Peer
}

func NewPeerList() *PeerList {
	return &PeerList{
		peers: make(map[node.NodeID]*Peer),
	}
}

func (pl *PeerList) Add(peer *Peer) {
	pl.peers[peer.ID] = peer
}

func (pl *PeerList) Get(id node.NodeID) (*Peer, bool) {
	peer, exists := pl.peers[id]
	return peer, exists
}

func (pl *PeerList) Remove(id node.NodeID) {
	delete(pl.peers, id)
}

func (pl *PeerList) List() []*Peer {
	peers := make([]*Peer, 0, len(pl.peers))
	for _, peer := range pl.peers {
		peers = append(peers, peer)
	}
	return peers
}

func (pl *PeerList) Count() int {
	return len(pl.peers)
}

func (pl *PeerList) Alive(timeout time.Duration) []*Peer {
	var alive []*Peer
	for _, peer := range pl.peers {
		if peer.IsAlive(timeout) {
			alive = append(alive, peer)
		}
	}
	return alive
}
