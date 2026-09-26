package transport

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/relaymesh/relaymesh/internal/node"
)

type UDPMessage struct {
	Source      node.NodeID `json:"source"`
	Destination node.NodeID `json:"destination"`
	Payload     []byte      `json:"payload"`
	Sequence    uint64      `json:"sequence"`
	ID          string      `json:"id"`
	TTL         uint32      `json:"ttl"`
	Timestamp   time.Time   `json:"timestamp"`
}

type UDPTransport struct {
	nodeID   node.NodeID
	conn     *net.UDPConn
	peers    map[node.NodeID]*net.UDPConn
	mu       sync.RWMutex
	recvChan chan *udpReceived
	localIP  string
}

type udpReceived struct {
	pkt  *Packet
	from node.NodeID
}

func NewUDP(nodeID node.NodeID) *UDPTransport {
	return &UDPTransport{
		nodeID:   nodeID,
		peers:    make(map[node.NodeID]*net.UDPConn),
		recvChan: make(chan *udpReceived, 100),
		localIP:  GetLocalIP(),
	}
}

func (u *UDPTransport) Listen(addr string) error {
	listenAddr := addr
	if addr == ":0" || addr == "" {
		listenAddr = fmt.Sprintf("%s:0", u.localIP)
	}

	udpAddr, err := net.ResolveUDPAddr("udp4", listenAddr)
	if err != nil {
		return fmt.Errorf("failed to resolve address: %w", err)
	}

	u.conn, err = net.ListenUDP("udp4", udpAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	go u.readLoop()
	return nil
}

func (u *UDPTransport) Connect(peer node.NodeID, addr string) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	if _, exists := u.peers[peer]; exists {
		return nil
	}

	udpAddr, err := net.ResolveUDPAddr("udp4", addr)
	if err != nil {
		return fmt.Errorf("failed to resolve peer address: %w", err)
	}

	conn, err := net.DialUDP("udp4", nil, udpAddr)
	if err != nil {
		return fmt.Errorf("failed to connect to peer: %w", err)
	}

	u.peers[peer] = conn

	return nil
}

func (u *UDPTransport) Send(peer node.NodeID, pkt *Packet) error {
	u.mu.RLock()
	conn, exists := u.peers[peer]
	u.mu.RUnlock()

	if !exists {
		return fmt.Errorf("peer %s not connected", peer)
	}

	msg := UDPMessage{
		Source:      pkt.Source,
		Destination: pkt.Destination,
		Payload:     pkt.Payload,
		Sequence:    pkt.Sequence,
		ID:          pkt.ID,
		TTL:         pkt.TTL,
		Timestamp:   time.Now(),
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal packet: %w", err)
	}

	_, err = conn.Write(data)
	if err != nil {
		return fmt.Errorf("failed to send packet: %w", err)
	}

	return nil
}

func (u *UDPTransport) Receive() (*Packet, node.NodeID, error) {
	received := <-u.recvChan
	return received.pkt, received.from, nil
}

func (u *UDPTransport) readLoop() {
	buf := make([]byte, 65536)
	for {
		n, remoteAddr, err := u.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}

		var msg UDPMessage
		if err := json.Unmarshal(buf[:n], &msg); err != nil {
			continue
		}

		if msg.Source == u.nodeID {
			continue
		}

		pkt := &Packet{
			Source:      msg.Source,
			Destination: msg.Destination,
			Payload:     msg.Payload,
			Sequence:    msg.Sequence,
			ID:          msg.ID,
			TTL:         msg.TTL,
		}

		u.recvChan <- &udpReceived{
			pkt:  pkt,
			from: msg.Source,
		}

		_ = remoteAddr
	}
}

func (u *UDPTransport) Close() error {
	u.mu.Lock()
	defer u.mu.Unlock()

	for _, conn := range u.peers {
		conn.Close()
	}

	if u.conn != nil {
		return u.conn.Close()
	}
	return nil
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
