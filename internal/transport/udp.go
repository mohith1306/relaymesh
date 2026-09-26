package transport

import (
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/relaymesh/relaymesh/api/proto"
	"github.com/relaymesh/relaymesh/internal/node"
)

// PacketVersion is the mesh wire-protocol version. Packets with a
// different version are dropped so old and new nodes fail loudly
// instead of misrouting.
const PacketVersion uint32 = 1

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

	msg := &pb.RelayPacket{
		Source:      string(pkt.Source),
		Destination: string(pkt.Destination),
		Sequence:    pkt.Sequence,
		Ttl:         pkt.TTL,
		Payload:     pkt.Payload,
		Timestamp:   time.Now().UnixNano(),
		Version:     PacketVersion,
		PacketId:    pkt.ID,
		HopCount:    uint32(len(pkt.Path)),
		Path:        nodeIDsToStrings(pkt.Path),
		Priority:    pkt.Priority,
	}

	data, err := proto.Marshal(msg)
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

		var msg pb.RelayPacket
		if err := proto.Unmarshal(buf[:n], &msg); err != nil {
			continue
		}

		if msg.Version != PacketVersion {
			continue
		}

		if node.NodeID(msg.Source) == u.nodeID {
			continue
		}

		pkt := &Packet{
			Source:      node.NodeID(msg.Source),
			Destination: node.NodeID(msg.Destination),
			Payload:     msg.Payload,
			Sequence:    msg.Sequence,
			ID:          msg.PacketId,
			TTL:         msg.Ttl,
			Path:        stringsToNodeIDs(msg.Path),
			Priority:    msg.Priority,
		}

		u.recvChan <- &udpReceived{
			pkt:  pkt,
			from: pkt.Source,
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

func nodeIDsToStrings(ids []node.NodeID) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, string(id))
	}
	return out
}

func stringsToNodeIDs(strs []string) []node.NodeID {
	out := make([]node.NodeID, 0, len(strs))
	for _, s := range strs {
		out = append(out, node.NodeID(s))
	}
	return out
}

func GetLocalIP() string {	addrs, err := net.InterfaceAddrs()
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
