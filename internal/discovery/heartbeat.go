package discovery

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/relaymesh/relaymesh/internal/node"
)

type HeartbeatMessage struct {
	Type         string       `json:"type"`
	NodeID       node.NodeID  `json:"node_id"`
	Address      string       `json:"address"`
	Port         uint16       `json:"port"`
	Sequence     uint64       `json:"sequence"`
	Capabilities []Capability `json:"capabilities"`
	Timestamp    time.Time    `json:"timestamp"`
}

type HeartbeatSender struct {
	nodeID    node.NodeID
	address   string
	port      uint16
	interval  time.Duration
	conns     []*net.UDPConn
	sequence  uint64
	knownPorts []uint16
	mu        sync.Mutex
}

func NewHeartbeatSender(nodeID node.NodeID, address string, port uint16, interval time.Duration) *HeartbeatSender {
	return &HeartbeatSender{
		nodeID:   nodeID,
		address:  address,
		port:     port,
		interval: interval,
	}
}

func (hs *HeartbeatSender) Start(knownPorts []uint16) error {
	hs.knownPorts = knownPorts

	for _, targetPort := range knownPorts {
		if targetPort == hs.port {
			continue
		}
		addr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("127.0.0.1:%d", targetPort))
		if err != nil {
			continue
		}
		conn, err := net.DialUDP("udp4", nil, addr)
		if err != nil {
			continue
		}
		hs.conns = append(hs.conns, conn)
	}

	return nil
}

func (hs *HeartbeatSender) Stop() error {
	for _, conn := range hs.conns {
		conn.Close()
	}
	return nil
}

func (hs *HeartbeatSender) Send() error {
	hs.mu.Lock()
	hs.sequence++
	seq := hs.sequence
	hs.mu.Unlock()

	msg := HeartbeatMessage{
		Type:         "heartbeat",
		NodeID:       hs.nodeID,
		Address:      hs.address,
		Port:         hs.port,
		Sequence:     seq,
		Capabilities: []Capability{CapabilityRelay, CapabilityRelayMesh},
		Timestamp:    time.Now(),
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	for _, conn := range hs.conns {
		conn.Write(data)
	}
	return nil
}

type HeartbeatListener struct {
	nodeID   node.NodeID
	port     uint16
	conn     *net.UDPConn
	callback func(HeartbeatMessage)
}

func NewHeartbeatListener(nodeID node.NodeID, port uint16) *HeartbeatListener {
	return &HeartbeatListener{
		nodeID: nodeID,
		port:   port,
	}
}

func (hl *HeartbeatListener) Start() error {
	addr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf(":%d", hl.port))
	if err != nil {
		return err
	}

	hl.conn, err = net.ListenUDP("udp4", addr)
	if err != nil {
		return err
	}

	return nil
}

func (hl *HeartbeatListener) Stop() error {
	if hl.conn != nil {
		return hl.conn.Close()
	}
	return nil
}

func (hl *HeartbeatListener) OnHeartbeat(callback func(HeartbeatMessage)) {
	hl.callback = callback
}

func (hl *HeartbeatListener) Listen() {
	buf := make([]byte, 4096)
	for {
		n, _, err := hl.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}

		var msg HeartbeatMessage
		if err := json.Unmarshal(buf[:n], &msg); err != nil {
			continue
		}

		if msg.NodeID == hl.nodeID {
			continue
		}

		if hl.callback != nil {
			hl.callback(msg)
		}
	}
}
