package transport

import (
	"io"

	"github.com/relaymesh/relaymesh/internal/node"
)

type Packet struct {
	Source      node.NodeID
	Destination node.NodeID
	Payload     []byte
	Sequence    uint64
	ID          string
	TTL         uint32
}

type Transport interface {
	Listen(addr string) error
	Connect(peer node.NodeID, addr string) error
	Send(peer node.NodeID, pkt *Packet) error
	Receive() (*Packet, node.NodeID, error)
	Close() error
	io.Closer
}
