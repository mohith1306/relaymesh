package transport

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/relaymesh/relaymesh/api/proto"
	"github.com/relaymesh/relaymesh/internal/identity"
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
	// psk, when non-empty, enables HMAC-SHA256 packet authentication:
	// outbound packets are signed, inbound packets without a valid tag
	// are dropped before reaching the forwarder.
	psk          []byte
	authFailures atomic.Uint64

	// Phase 10 secure mode (mutual auth + encryption).
	ident              *identity.Identity
	encryptMode        bool
	listenPort         uint16
	listenHost         string
	sessions           map[node.NodeID]*session
	sidIndex           map[[sidLen]byte]*session
	flights            map[node.NodeID]*handshakeFlight
	pins               map[node.NodeID]string
	attempts           map[node.NodeID]int
	cooldowns          map[node.NodeID]time.Time
	handshakeFailures  atomic.Uint64
	decryptFailures    atomic.Uint64
	replayRejects      atomic.Uint64
	plaintextRejects   atomic.Uint64
	secureRejects      atomic.Uint64
	identityMismatches atomic.Uint64

	// Phase 14: UDP hole punching.
	// remoteAddrs tracks the last observed source address per peer so
	// readLoop can detect an inbound packet from an address we have not
	// dialed yet (the punch landing) and re-bind the connected socket.
	remoteAddrs map[node.NodeID]*net.UDPAddr
}

// SetPSK enables (non-empty) or disables (empty) packet authentication.
func (u *UDPTransport) SetPSK(psk []byte) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if len(psk) == 0 {
		u.psk = nil
		return
	}
	u.psk = append([]byte{}, psk...)
}

// AuthFailures counts inbound packets dropped for bad/missing auth tags.
func (u *UDPTransport) AuthFailures() uint64 {
	return u.authFailures.Load()
}

// ListenAddr returns the address peers should reply to (advertised
// listen socket), empty before Listen.
func (u *UDPTransport) ListenAddr() string {
	u.mu.RLock()
	defer u.mu.RUnlock()
	if u.listenPort == 0 {
		return ""
	}
	return u.advertisedAddrLocked()
}

func computeTag(psk, data []byte) []byte {
	h := hmac.New(sha256.New, psk)
	h.Write(data)
	return h.Sum(nil)
}

// verifyTag checks the packet's HMAC over its untagged encoding using
// constant-time comparison. Missing tags fail closed.
func verifyTag(psk []byte, msg *pb.RelayPacket) bool {
	if len(msg.AuthTag) == 0 {
		return false
	}
	tag := msg.AuthTag
	msg.AuthTag = nil
	unsigned, err := proto.Marshal(msg)
	msg.AuthTag = tag
	if err != nil {
		return false
	}
	expected := computeTag(psk, unsigned)
	return subtle.ConstantTimeCompare(tag, expected) == 1
}

type udpReceived struct {
	pkt  *Packet
	from node.NodeID
}

func NewUDP(nodeID node.NodeID) *UDPTransport {
	return &UDPTransport{
		nodeID:      nodeID,
		peers:       make(map[node.NodeID]*net.UDPConn),
		recvChan:    make(chan *udpReceived, 100),
		localIP:     GetLocalIP(),
		sessions:    make(map[node.NodeID]*session),
		sidIndex:    make(map[[sidLen]byte]*session),
		flights:     make(map[node.NodeID]*handshakeFlight),
		pins:        make(map[node.NodeID]string),
		attempts:    make(map[node.NodeID]int),
		cooldowns:   make(map[node.NodeID]time.Time),
		remoteAddrs: make(map[node.NodeID]*net.UDPAddr),
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

	if la, ok := u.conn.LocalAddr().(*net.UDPAddr); ok {
		u.mu.Lock()
		u.listenPort = uint16(la.Port)
		if ip := la.IP.String(); ip != "" && ip != "0.0.0.0" && ip != "::" {
			u.listenHost = ip
		}
		u.mu.Unlock()
	}

	go u.readLoop()
	return nil
}

func (u *UDPTransport) Connect(peer node.NodeID, addr string) error {
	u.mu.Lock()

	if _, exists := u.peers[peer]; exists {
		u.mu.Unlock()
		// Session may still be missing (peer restart): kick a retry.
		u.startHandshake(peer)
		return nil
	}

	udpAddr, err := net.ResolveUDPAddr("udp4", addr)
	if err != nil {
		u.mu.Unlock()
		return fmt.Errorf("failed to resolve peer address: %w", err)
	}

	conn, err := net.DialUDP("udp4", nil, udpAddr)
	if err != nil {
		u.mu.Unlock()
		return fmt.Errorf("failed to connect to peer: %w", err)
	}

	u.peers[peer] = conn
	u.mu.Unlock()

	u.startHandshake(peer)
	return nil
}

func (u *UDPTransport) Send(peer node.NodeID, pkt *Packet) error {
	u.mu.RLock()
	conn, exists := u.peers[peer]
	secure := u.encryptMode && u.ident != nil
	var sess *session
	if secure {
		sess = u.sessions[peer]
	}
	psk := u.psk
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

	if secure && sess == nil {
		// Kick the handshake and fail this attempt; callers retry and
		// probes simply do not count as link evidence.
		u.startHandshake(peer)
		return fmt.Errorf("secure session with %s not established: %w", peer, errNoSession)
	}

	if len(psk) > 0 {
		unsigned, err := proto.Marshal(msg)
		if err != nil {
			return fmt.Errorf("failed to marshal packet: %w", err)
		}
		msg.AuthTag = computeTag(psk, unsigned)
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal packet: %w", err)
	}

	if secure {
		frame, err := sess.seal(data)
		if err != nil {
			return fmt.Errorf("failed to encrypt packet: %w", err)
		}
		if _, err := conn.Write(frame); err != nil {
			return fmt.Errorf("failed to send packet: %w", err)
		}
		return nil
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

		if isSecureFrame(buf[:n]) {
			u.handleSecureFrame(buf[:n])
			continue
		}

		// Encrypted nodes fail closed on plaintext data frames.
		if u.SecureEnabled() {
			u.plaintextRejects.Add(1)
			continue
		}

		var msg pb.RelayPacket
		if err := proto.Unmarshal(buf[:n], &msg); err != nil {
			continue
		}

		if msg.Version != PacketVersion {
			continue
		}

		u.mu.RLock()
		psk := u.psk
		u.mu.RUnlock()
		if len(psk) > 0 && !verifyTag(psk, &msg) {
			u.authFailures.Add(1)
			continue
		}

		if node.NodeID(msg.Source) == u.nodeID {
			continue
		}

		srcID := node.NodeID(msg.Source)

		// Phase 14: track the observed source address for this peer, and
		// if we have not dialed it yet (the punch landed), re-bind the
		// connected socket so replies go to the correct public mapping.
		if remoteAddr != nil {
			u.mu.Lock()
			if _, known := u.peers[srcID]; !known {
				// First packet from an address we did not dial: this is a
				// hole punch landing. Re-bind so subsequent traffic uses
				// the observed mapping.
				if conn, err := net.DialUDP("udp4", nil, remoteAddr); err == nil {
					u.peers[srcID] = conn
				}
			}
			u.remoteAddrs[srcID] = remoteAddr
			u.mu.Unlock()
		}

		pkt := &Packet{
			Source:      srcID,
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

// Punch sends a small probe from our listen socket directly to addr.
// Because it comes from the same local port the peer will later receive
// data on, it "opens" our NAT's outbound mapping so the peer's reply
// (and their own simultaneous punch) can land. It is safe to call
// before Connect; the readLoop will detect the reply and re-bind the
// connected socket to the observed address.
func (u *UDPTransport) Punch(addr string) error {
	if u.conn == nil {
		return fmt.Errorf("not listening")
	}
	udpAddr, err := net.ResolveUDPAddr("udp4", addr)
	if err != nil {
		return fmt.Errorf("failed to resolve punch addr: %w", err)
	}
	// Payload content is irrelevant; any datagram opens the NAT mapping.
	_, err = u.conn.WriteToUDP([]byte("punch"), udpAddr)
	return err
}

// Close closes the transport and all peer sockets.
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
