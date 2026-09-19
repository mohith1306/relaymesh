package forwarding

import (
	"sync"

	"github.com/relaymesh/relaymesh/internal/node"
)

type Queue struct {
	packets []*Packet
	mu      sync.Mutex
}

func NewQueue() *Queue {
	return &Queue{
		packets: make([]*Packet, 0),
	}
}

func (q *Queue) Enqueue(pkt *Packet) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.packets = append(q.packets, pkt)
}

func (q *Queue) Dequeue() *Packet {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.packets) == 0 {
		return nil
	}

	pkt := q.packets[0]
	q.packets = q.packets[1:]
	return pkt
}

func (q *Queue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.packets)
}

func (q *Queue) IsEmpty() bool {
	return q.Size() == 0
}

func (q *Queue) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.packets = make([]*Packet, 0)
}

func (q *Queue) Peek() *Packet {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.packets) == 0 {
		return nil
	}
	return q.packets[0]
}

type DuplicateDetector struct {
	seen map[PacketID]bool
	mu   sync.Mutex
}

func NewDuplicateDetector() *DuplicateDetector {
	return &DuplicateDetector{
		seen: make(map[PacketID]bool),
	}
}

func (dd *DuplicateDetector) IsDuplicate(pkt *Packet) bool {
	dd.mu.Lock()
	defer dd.mu.Unlock()

	if dd.seen[pkt.ID] {
		return true
	}
	dd.seen[pkt.ID] = true
	return false
}

func (dd *DuplicateDetector) Cleanup(maxSize int) {
	dd.mu.Lock()
	defer dd.mu.Unlock()

	if len(dd.seen) > maxSize {
		dd.seen = make(map[PacketID]bool)
	}
}

type ForwardingTable struct {
	entries map[node.NodeID]node.NodeID
	mu      sync.RWMutex
}

func NewForwardingTable() *ForwardingTable {
	return &ForwardingTable{
		entries: make(map[node.NodeID]node.NodeID),
	}
}

func (ft *ForwardingTable) Set(dest, nextHop node.NodeID) {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	ft.entries[dest] = nextHop
}

func (ft *ForwardingTable) Get(dest node.NodeID) (node.NodeID, bool) {
	ft.mu.RLock()
	defer ft.mu.RUnlock()
	nextHop, ok := ft.entries[dest]
	return nextHop, ok
}

func (ft *ForwardingTable) Remove(dest node.NodeID) {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	delete(ft.entries, dest)
}

func (ft *ForwardingTable) GetAll() map[node.NodeID]node.NodeID {
	ft.mu.RLock()
	defer ft.mu.RUnlock()

	result := make(map[node.NodeID]node.NodeID)
	for k, v := range ft.entries {
		result[k] = v
	}
	return result
}
