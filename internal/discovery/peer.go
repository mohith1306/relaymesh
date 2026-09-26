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
	DataPort     uint16
	Latency      time.Duration
	Bandwidth    float64
	PacketLoss   float64
	LastSeen     time.Time
	Capabilities []Capability
	Sequence     uint64
	KnownPeers   []node.NodeID
}

func NewPeer(id node.NodeID, address string, port uint16) *Peer {
	return NewPeerWithData(id, address, port, 0)
}

func NewPeerWithData(id node.NodeID, address string, port uint16, dataPort uint16) *Peer {
	return &Peer{
		ID:           id,
		Address:      address,
		Port:         port,
		DataPort:     dataPort,
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
	if metrics.DataPort != 0 {
		p.DataPort = metrics.DataPort
	}
	if metrics.KnownPeers != nil {
		p.KnownPeers = metrics.KnownPeers
	}
}

func (p *Peer) AddressString() string {
	return fmt.Sprintf("%s:%d", p.Address, p.Port)
}

// DataAddress returns the address:port for the mesh data plane.
// Falls back to the discovery port if no data port was advertised.
func (p *Peer) DataAddress() string {
	port := p.DataPort
	if port == 0 {
		port = p.Port
	}
	return fmt.Sprintf("%s:%d", p.Address, port)
}

type PeerMetrics struct {
	Latency    time.Duration
	Bandwidth  float64
	PacketLoss float64
	Sequence   uint64
	DataPort   uint16
	KnownPeers []node.NodeID
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
