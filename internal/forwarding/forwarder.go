package forwarding

import (
	"log/slog"
	"sync"

	"github.com/relaymesh/relaymesh/internal/node"
	"github.com/relaymesh/relaymesh/internal/transport"
)

type DeliveredPacket struct {
	Source   node.NodeID
	Payload  []byte
	Sequence uint64
	ID       string
	From     node.NodeID
	Path     []node.NodeID
}

type Forwarder struct {
	nodeID            node.NodeID
	transport         transport.Transport
	queue             *Queue
	forwardingTable   *ForwardingTable
	duplicateDetector *DuplicateDetector
	onDelivered       func(DeliveredPacket)
	logger            *slog.Logger
	mu                sync.RWMutex
	forwarded         uint64
	delivered         uint64
	dropped           uint64
}

func New(nodeID node.NodeID, transport transport.Transport, logger *slog.Logger) *Forwarder {
	return &Forwarder{
		nodeID:            nodeID,
		transport:         transport,
		queue:             NewQueue(),
		forwardingTable:   NewForwardingTable(),
		duplicateDetector: NewDuplicateDetector(),
		logger:            logger,
	}
}

func (f *Forwarder) OnDelivered(cb func(DeliveredPacket)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onDelivered = cb
}

func (f *Forwarder) Forward(pkt *Packet, nextHop node.NodeID) error {
	if pkt.ID == "" {
		pkt.ID = PacketID(generateID())
	}
	if pkt.TTL == 0 {
		pkt.TTL = DefaultTTL
	}
	if f.duplicateDetector.IsDuplicate(pkt) {
		f.mu.Lock()
		f.dropped++
		f.mu.Unlock()
		f.logger.Debug("dropping duplicate packet",
			"packet_id", pkt.ID,
			"source", pkt.Source,
			"destination", pkt.Destination,
		)
		return nil
	}

	if pkt.HasVisited(f.nodeID) {
		f.mu.Lock()
		f.dropped++
		f.mu.Unlock()
		f.logger.Debug("dropping looping packet",
			"packet_id", pkt.ID,
			"source", pkt.Source,
			"destination", pkt.Destination,
			"path", pkt.Path,
		)
		return nil
	}

	if !pkt.DecrementTTL() {
		f.mu.Lock()
		f.dropped++
		f.mu.Unlock()
		f.logger.Debug("dropping packet with expired TTL",
			"packet_id", pkt.ID,
			"source", pkt.Source,
			"destination", pkt.Destination,
		)
		return nil
	}

	f.logger.Debug("forwarding packet",
		"packet_id", pkt.ID,
		"source", pkt.Source,
		"destination", pkt.Destination,
		"next_hop", nextHop,
		"ttl", pkt.TTL,
	)

	transportPkt := &transport.Packet{
		Source:      pkt.Source,
		Destination: pkt.Destination,
		Payload:     pkt.Payload,
		Sequence:    pkt.Sequence,
		ID:          string(pkt.ID),
		TTL:         pkt.TTL,
		Path:        append(append([]node.NodeID{}, pkt.Path...), f.nodeID),
		Priority:    pkt.Priority,
	}

	if err := f.transport.Send(nextHop, transportPkt); err != nil {
		return err
	}

	f.mu.Lock()
	f.forwarded++
	f.mu.Unlock()
	return nil
}

func visited(path []node.NodeID, id node.NodeID) bool {
	for _, n := range path {
		if n == id {
			return true
		}
	}
	return false
}

func (f *Forwarder) HandleReceived(pkt *transport.Packet, from node.NodeID) {
	f.logger.Debug("received packet",
		"source", pkt.Source,
		"destination", pkt.Destination,
		"from", from,
	)

	if pkt.Destination == f.nodeID {
		f.mu.Lock()
		f.delivered++
		cb := f.onDelivered
		f.mu.Unlock()
		fullPath := append(append([]node.NodeID{}, pkt.Path...), f.nodeID)
		f.logger.Info("packet delivered locally",
			"source", pkt.Source,
			"payload_size", len(pkt.Payload),
			"packet_id", pkt.ID,
			"path", fullPath,
		)
		if cb != nil {
			cb(DeliveredPacket{
				Source:   pkt.Source,
				Payload:  pkt.Payload,
				Sequence: pkt.Sequence,
				ID:       pkt.ID,
				From:     from,
				Path:     fullPath,
			})
		}
		return
	}

	if visited(pkt.Path, f.nodeID) {
		f.mu.Lock()
		f.dropped++
		f.mu.Unlock()
		f.logger.Debug("dropping looping packet",
			"packet_id", pkt.ID,
			"destination", pkt.Destination,
			"path", pkt.Path,
		)
		return
	}

	nextHop, exists := f.forwardingTable.Get(pkt.Destination)
	if !exists {
		f.mu.Lock()
		f.dropped++
		f.mu.Unlock()
		f.logger.Warn("no route to destination",
			"destination", pkt.Destination,
		)
		return
	}

	ttl := pkt.TTL
	if ttl == 0 {
		ttl = DefaultTTL
	}
	forwardPkt := &Packet{
		ID:          PacketID(pkt.ID),
		Source:      pkt.Source,
		Destination: pkt.Destination,
		Payload:     pkt.Payload,
		Sequence:    pkt.Sequence,
		TTL:         ttl,
		Path:        pkt.Path,
		Priority:    pkt.Priority,
	}
	if forwardPkt.ID == "" {
		forwardPkt.ID = PacketID(generateID())
	}

	if err := f.Forward(forwardPkt, nextHop); err != nil {
		f.logger.Error("failed to forward packet",
			"error", err,
			"destination", pkt.Destination,
		)
	}
}

func (f *Forwarder) UpdateForwardingTable(dest, nextHop node.NodeID) {
	f.forwardingTable.Set(dest, nextHop)
	f.logger.Debug("forwarding table updated",
		"destination", dest,
		"next_hop", nextHop,
	)
}

func (f *Forwarder) RemoveFromForwardingTable(dest node.NodeID) {
	f.forwardingTable.Remove(dest)
}

func (f *Forwarder) GetForwardingTable() *ForwardingTable {
	return f.forwardingTable
}

type ForwarderStats struct {
	Forwarded uint64
	Delivered uint64
	Dropped   uint64
}

func (f *Forwarder) Stats() ForwarderStats {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return ForwarderStats{
		Forwarded: f.forwarded,
		Delivered: f.delivered,
		Dropped:   f.dropped,
	}
}
