package forwarding

import (
	"log/slog"
	"sync"

	"github.com/relaymesh/relaymesh/internal/node"
	"github.com/relaymesh/relaymesh/internal/transport"
)

type Forwarder struct {
	nodeID           node.NodeID
	transport        transport.Transport
	queue            *Queue
	forwardingTable  *ForwardingTable
	duplicateDetector *DuplicateDetector
	logger           *slog.Logger
	mu               sync.RWMutex
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

func (f *Forwarder) Forward(pkt *Packet, nextHop node.NodeID) error {
	if f.duplicateDetector.IsDuplicate(pkt) {
		f.logger.Debug("dropping duplicate packet",
			"packet_id", pkt.ID,
			"source", pkt.Source,
			"destination", pkt.Destination,
		)
		return nil
	}

	if !pkt.DecrementTTL() {
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
	}

	return f.transport.Send(nextHop, transportPkt)
}

func (f *Forwarder) HandleReceived(pkt *transport.Packet, from node.NodeID) {
	f.logger.Debug("received packet",
		"source", pkt.Source,
		"destination", pkt.Destination,
		"from", from,
	)

	if pkt.Destination == f.nodeID {
		f.logger.Info("packet destined for this node",
			"source", pkt.Source,
			"payload_size", len(pkt.Payload),
		)
		return
	}

	nextHop, exists := f.forwardingTable.Get(pkt.Destination)
	if !exists {
		f.logger.Warn("no route to destination",
			"destination", pkt.Destination,
		)
		return
	}

	forwardPkt := &Packet{
		ID:          PacketID(pkt.Source),
		Source:      pkt.Source,
		Destination: pkt.Destination,
		Payload:     pkt.Payload,
		Sequence:    pkt.Sequence,
		TTL:         64,
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

func (f *Forwarder) GetForwardingTable() *ForwardingTable {
	return f.forwardingTable
}
