package transport

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/relaymesh/relaymesh/internal/node"
	"google.golang.org/protobuf/proto"

	pb "github.com/relaymesh/relaymesh/api/proto"
)

// RelayTransport is a WebSocket-based fallback used when UDP hole
// punching fails (symmetric NAT, CGNAT, hostile firewalls). It
// implements the full transport.Transport surface (including the
// auth/encryption control methods MeshNode calls directly) but carries
// each Packet as one JSON message over a single shared WSS connection
// to the relay-server.
//
// This is deliberately NOT the mesh's normal data plane — it is the
// "if punching failed, still make it work" path. Reliability is left to
// the TCP/WS layer underneath.
type RelayTransport struct {
	self node.NodeID

	mu       sync.RWMutex
	conn     *websocket.Conn
	peers    map[node.NodeID]struct{}
	recv     chan *Packet
	relayURL string
	closed   atomic.Bool

	psk            []byte
	authFailures   uint64
	logger         *slog.Logger
}

// RelayConfig configures a RelayTransport.
type RelayConfig struct {
	Self     node.NodeID
	RelayURL string // e.g. "wss://relaymesh-f93s.onrender.com/relay-ws"
	Logger   *slog.Logger
}

// NewRelay creates a RelayTransport that is not yet connected. Call
// Connect(nil, "") first to establish the WSS session, then Connect(peer,
// peerIDStr) for each remote peer (addr is ignored; the relay routes by
// node ID).
func NewRelay(cfg RelayConfig) *RelayTransport {
	return &RelayTransport{
		self:     cfg.Self,
		relayURL: cfg.RelayURL,
		logger:   cfg.Logger,
		peers:    make(map[node.NodeID]struct{}),
		recv:     make(chan *Packet, 256),
	}
}

func (t *RelayTransport) Listen(addr string) error {
	// No-op: the relay session is established in Connect, and there is no
	// local port to bind.
	return nil
}

// Connect establishes the WSS session on first call (peer ignored) and
// registers each peer ID the relay should route to us. Subsequent calls
// just record the peer.
func (t *RelayTransport) Connect(peer node.NodeID, addr string) error {
	if peer == "" {
		// First call: establish the WSS session.
		return t.ensureConn()
	}
	if err := t.ensureConn(); err != nil {
		return err
	}
	t.mu.Lock()
	t.peers[peer] = struct{}{}
	t.mu.Unlock()
	return nil
}

func (t *RelayTransport) ensureConn() error {
	t.mu.RLock()
	ok := t.conn != nil && !t.closed.Load()
	t.mu.RUnlock()
	if ok {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.conn != nil {
		return nil
	}

	dialer := websocket.Dialer{}
	conn, _, err := dialer.Dial(t.relayURL, nil)
	if err != nil {
		return fmt.Errorf("relay dial %s: %w", t.relayURL, err)
	}

	// Register with the relay: {type: "register", node_id, data: <base64>...}
	// For relay-mode we only need the node ID; actual packets are sent
	// with type "relay-data".
	reg := map[string]interface{}{
		"type":    "register",
		"node_id": string(t.self),
	}
	if err := conn.WriteJSON(reg); err != nil {
		conn.Close()
		return fmt.Errorf("relay register: %w", err)
	}

	t.conn = conn
	go t.readLoop(conn)
	t.logger.Info("relay transport connected", "url", t.relayURL, "self", t.self)
	return nil
}

func (t *RelayTransport) readLoop(conn *websocket.Conn) {
	for {
		var msg struct {
			Type    string `json:"type"`
			Source  string `json:"source"`
			Payload []byte `json:"payload"`
		}
		if err := conn.ReadJSON(&msg); err != nil {
			if !t.closed.Load() {
				t.logger.Warn("relay read error", "error", err)
			}
			return
		}
		if msg.Type != "relay-data" {
			continue
		}
		pkt := decodeRelayPayload(msg.Payload)
		if pkt == nil {
			continue
		}
		select {
		case t.recv <- pkt:
		default:
			// Drop if the mesh is not keeping up; WS already guarantees
			// delivery up to this point.
		}
	}
}

// Send wraps pkt in the relay's {type: "relay-data", target, payload}
// envelope. The payload is the same protobuf encoding UDPTransport uses,
// so MeshNode's handshake/encryption logic is untouched.
func (t *RelayTransport) Send(peer node.NodeID, pkt *Packet) error {
	t.mu.RLock()
	conn := t.conn
	t.mu.RUnlock()
	if conn == nil {
		return fmt.Errorf("relay transport not connected")
	}
	return conn.WriteJSON(map[string]interface{}{
		"type":    "relay-data",
		"target":  string(peer),
		"payload": encodeRelayPayload(pkt),
	})
}

func (t *RelayTransport) Receive() (*Packet, node.NodeID, error) {
	pkt, ok := <-t.recv
	if !ok {
		return nil, "", fmt.Errorf("relay transport closed")
	}
	return pkt, pkt.Source, nil
}

func (t *RelayTransport) Close() error {
	if !t.closed.CompareAndSwap(false, true) {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.conn != nil {
		t.conn.Close()
		t.conn = nil
	}
	close(t.recv)
	return nil
}

// --- auth/encryption control methods (parity with UDPTransport) ---

func (t *RelayTransport) SetPSK(psk []byte) {
	t.psk = psk
}

func (t *RelayTransport) AuthFailures() uint64 {
	return atomic.LoadUint64(&t.authFailures)
}

func (t *RelayTransport) ListenAddr() string {
	return t.relayURL
}

// encodeRelayPayload serializes a Packet to the same protobuf format
// UDPTransport puts on the wire, so the relay server does not need to
// understand mesh internals — it just forwards opaque bytes.
func encodeRelayPayload(pkt *Packet) []byte {
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
	b, err := proto.Marshal(msg)
	if err != nil {
		return nil
	}
	return b
}

func decodeRelayPayload(b []byte) *Packet {
	var msg pb.RelayPacket
	if err := proto.Unmarshal(b, &msg); err != nil {
		return nil
	}
	if msg.Version != PacketVersion {
		return nil
	}
	return &Packet{
		Source:      node.NodeID(msg.Source),
		Destination: node.NodeID(msg.Destination),
		Payload:     msg.Payload,
		Sequence:    msg.Sequence,
		ID:          msg.PacketId,
		TTL:         msg.Ttl,
		Path:        stringsToNodeIDs(msg.Path),
		Priority:    msg.Priority,
	}
}
