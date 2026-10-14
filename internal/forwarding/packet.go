package forwarding

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/relaymesh/relaymesh/internal/node"
)

// packetSeq guarantees process-wide unique packet IDs. Timestamps alone
// are unsafe: clocks often resolve to microseconds, and two packets
// created in the same tick would share an ID and kill each other in
// the duplicate detector.
var packetSeq atomic.Uint64

type PacketID string

const DefaultTTL uint32 = 64

type Packet struct {
	ID          PacketID
	Source      node.NodeID
	Destination node.NodeID
	Payload     []byte
	Sequence    uint64
	TTL         uint32
	CreatedAt   time.Time
	HopCount    uint32
	// Path records every node that has handled the packet, in order,
	// starting with the source. Used for loop prevention and tracing.
	Path     []node.NodeID
	Priority uint32
}

func (p *Packet) HasVisited(id node.NodeID) bool {
	for _, n := range p.Path {
		if n == id {
			return true
		}
	}
	return false
}

func NewPacket(source, destination node.NodeID, payload []byte, ttl uint32) *Packet {
	return &Packet{
		ID:          PacketID(generateID(source)),
		Source:      source,
		Destination: destination,
		Payload:     payload,
		TTL:         ttl,
		CreatedAt:   time.Now(),
	}
}

func (p *Packet) DecrementTTL() bool {
	if p.TTL == 0 {
		return false
	}
	p.TTL--
	p.HopCount++
	return true
}

func (p *Packet) IsExpired() bool {
	return p.TTL == 0
}

func (p *Packet) IncrementSequence() {
	p.Sequence++
}

func generateID(source node.NodeID) string {
	return fmt.Sprintf("%s-%d-%d", source, time.Now().UnixNano(), packetSeq.Add(1))
}
