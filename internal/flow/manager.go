package flow

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/relaymesh/relaymesh/api/proto"
	"github.com/relaymesh/relaymesh/internal/node"
)

// Sender delivers a payload to a mesh peer (typically mesh.Send).
type Sender func(dest node.NodeID, payload []byte) error

// Manager owns client-side flows for one mesh node: it opens TCP/UDP
// flows through a gateway, runs the retransmit timers, and dispatches
// incoming flow frames. Register HandlePacket with
// mesh.AddPayloadHandler.
type Manager struct {
	self    node.NodeID
	send    Sender
	logger  *slog.Logger
	counter atomic.Uint64

	mu  sync.Mutex
	tcp map[string]*Conn
	udp map[string]*UDPFlow
}

func NewManager(self node.NodeID, send Sender, logger *slog.Logger) *Manager {
	return &Manager{
		self:   self,
		send:   send,
		logger: logger,
		tcp:    make(map[string]*Conn),
		udp:    make(map[string]*UDPFlow),
	}
}

// HandlePacket consumes flow frames addressed to this node. Register
// it with mesh.AddPayloadHandler.
func (m *Manager) HandlePacket(source node.NodeID, payload []byte) bool {
	kind, raw, ok := UnwrapEnvelope(payload)
	if !ok {
		return false
	}
	switch kind {
	case kindFlowAccept:
		var msg pb.FlowAccept
		if proto.Unmarshal(raw, &msg) == nil {
			if c := m.tcpByID(msg.FlowId); c != nil {
				c.onAccept()
			}
			if u := m.udpByID(msg.FlowId); u != nil {
				u.onAccept()
			}
		}
	case kindFlowReject:
		var msg pb.FlowReject
		if proto.Unmarshal(raw, &msg) == nil {
			if c := m.tcpByID(msg.FlowId); c != nil {
				c.onReject(msg.Error)
			}
			if u := m.udpByID(msg.FlowId); u != nil {
				u.onReject(msg.Error)
			}
		}
	case kindFlowData:
		var msg pb.FlowData
		if proto.Unmarshal(raw, &msg) == nil {
			if c := m.tcpByID(msg.FlowId); c != nil {
				c.touch()
				c.recv.onData(msg.Seq, msg.Chunk)
			}
		}
	case kindFlowAck:
		var msg pb.FlowAck
		if proto.Unmarshal(raw, &msg) == nil {
			if c := m.tcpByID(msg.FlowId); c != nil {
				c.touch()
				c.sender.onAck(msg.AckSeq)
			}
		}
	case kindFlowFIN:
		var msg pb.FlowFIN
		if proto.Unmarshal(raw, &msg) == nil {
			if c := m.tcpByID(msg.FlowId); c != nil {
				c.touch()
				c.onPeerFIN()
			}
		}
	case kindFlowRST:
		var msg pb.FlowRST
		if proto.Unmarshal(raw, &msg) == nil {
			c := m.tcpByID(msg.FlowId)
			u := m.udpByID(msg.FlowId)
			if c != nil {
				c.teardown(msg.Reason, false)
			}
			if u != nil {
				u.teardown()
			}
		}
	case kindFlowDatagram:
		var msg pb.FlowDatagram
		if proto.Unmarshal(raw, &msg) == nil {
			if u := m.udpByID(msg.FlowId); u != nil {
				u.deliver(msg.Data)
			}
		}
	}
	return true
}

// OpenTCP opens a reliable TCP flow through the gateway to
// host:port. The returned Conn implements net.Conn.
func (m *Manager) OpenTCP(ctx context.Context, gw node.NodeID, host string, port uint16) (*Conn, error) {
	flowID := fmt.Sprintf("%s-%d", m.self, m.counter.Add(1))
	c := newConn(m, flowID, gw)

	m.mu.Lock()
	m.tcp[flowID] = c
	m.mu.Unlock()

	open := &pb.FlowOpen{
		FlowId:     flowID,
		Udp:        false,
		TargetHost: host,
		TargetPort: uint32(port),
	}
	if err := m.sendFrame(gw, kindFlowOpen, open); err != nil {
		m.removeTCP(flowID)
		return nil, fmt.Errorf("send FlowOpen: %w", err)
	}

	if err := c.waitAccept(ctx); err != nil {
		c.teardown(err.Error(), true)
		return nil, err
	}
	return c, nil
}

// OpenUDP opens a best-effort datagram flow through the gateway. The
// call returns once the gateway has accepted (dialed) the flow.
func (m *Manager) OpenUDP(ctx context.Context, gw node.NodeID, host string, port uint16) (*UDPFlow, error) {
	flowID := fmt.Sprintf("%s-%d", m.self, m.counter.Add(1))
	f := newUDPFlow(m, flowID, gw)

	m.mu.Lock()
	m.udp[flowID] = f
	m.mu.Unlock()

	open := &pb.FlowOpen{
		FlowId:     flowID,
		Udp:        true,
		TargetHost: host,
		TargetPort: uint32(port),
	}
	if err := m.sendFrame(gw, kindFlowOpen, open); err != nil {
		f.teardown()
		return nil, fmt.Errorf("send FlowOpen: %w", err)
	}
	if err := f.waitAccept(ctx); err != nil {
		f.teardown()
		return nil, err
	}
	return f, nil
}

func (m *Manager) sendFrame(dest node.NodeID, kind byte, msg proto.Message) error {
	raw, err := Wrap(kind, msg)
	if err != nil {
		return err
	}
	return m.send(dest, raw)
}

func (m *Manager) tcpByID(id string) *Conn {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tcp[id]
}

func (m *Manager) udpByID(id string) *UDPFlow {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.udp[id]
}

func (m *Manager) removeTCP(id string) {
	m.mu.Lock()
	delete(m.tcp, id)
	m.mu.Unlock()
}

func (m *Manager) removeUDP(id string) {
	m.mu.Lock()
	delete(m.udp, id)
	m.mu.Unlock()
}

// Flows reports the number of open client-side flows (TCP+UDP).
func (m *Manager) Flows() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.tcp) + len(m.udp)
}

// Conn is the client end of a TCP flow. It implements net.Conn.
// SetDeadline family are no-ops; flow liveness is governed by the
// flow-layer idle timeout.
type Conn struct {
	mgr    *Manager
	flowID string
	gw     node.NodeID

	sender *reliableSender
	recv   *reliableReceiver

	recvMu     sync.Mutex
	recvBuf    bytes.Buffer
	recvCond   *sync.Cond
	readClosed bool

	acceptCh chan struct{}
	acceptMu sync.Mutex
	accepted bool
	rejected string

	closeOnce sync.Once
	done      chan struct{}
	idleAt    atomic.Int64
	finOnce   sync.Once
}

var _ net.Conn = (*Conn)(nil)

func newConn(m *Manager, flowID string, gw node.NodeID) *Conn {
	c := &Conn{
		mgr:      m,
		flowID:   flowID,
		gw:       gw,
		acceptCh: make(chan struct{}),
		done:     make(chan struct{}),
	}
	c.recvCond = sync.NewCond(&c.recvMu)
	c.sender = newReliableSender(flowID, func(seq uint32, chunk []byte) error {
		return m.sendFrame(gw, kindFlowData, &pb.FlowData{
			FlowId: flowID, Seq: seq, Chunk: chunk,
		})
	}, func(reason string) {
		c.teardown("sender dead: "+reason, true)
	})
	c.recv = newReliableReceiver(func(chunk []byte) {
		c.recvMu.Lock()
		c.recvBuf.Write(chunk)
		c.recvMu.Unlock()
		c.recvCond.Broadcast()
	}, func(ackSeq uint32) error {
		return m.sendFrame(gw, kindFlowAck, &pb.FlowAck{
			FlowId: flowID, AckSeq: ackSeq,
		})
	})
	c.touch()
	go c.writerLoop()
	return c
}

func (c *Conn) writerLoop() {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case now := <-ticker.C:
			dead, reason := c.sender.trySend(now)
			if dead {
				c.teardown(reason, true)
				return
			}
			if time.Since(c.lastTouch()) > IdleTimeout {
				c.teardown("idle timeout", true)
				return
			}
			// Wake finish() waiters so deadlines are rechecked.
			c.sender.mu.Lock()
			c.sender.cond.Broadcast()
			c.sender.mu.Unlock()
		}
	}
}

func (c *Conn) touch() { c.idleAt.Store(time.Now().UnixNano()) }

func (c *Conn) lastTouch() time.Time { return time.Unix(0, c.idleAt.Load()) }

func (c *Conn) onAccept() {
	c.acceptMu.Lock()
	c.accepted = true
	c.acceptMu.Unlock()
	close(c.acceptCh)
}

func (c *Conn) onReject(msg string) {
	c.acceptMu.Lock()
	c.rejected = msg
	c.acceptMu.Unlock()
	close(c.acceptCh)
}

func (c *Conn) waitAccept(ctx context.Context) error {
	timeout := AcceptTimeout
	if d, ok := ctx.Deadline(); ok {
		timeout = time.Until(d)
	}
	select {
	case <-c.acceptCh:
		c.acceptMu.Lock()
		defer c.acceptMu.Unlock()
		if c.rejected != "" {
			return fmt.Errorf("gateway rejected flow: %s", c.rejected)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(timeout):
		return fmt.Errorf("timeout waiting for FlowAccept")
	case <-c.done:
		return errFlowDead("closed")
	}
}

func (c *Conn) onPeerFIN() {
	c.recvMu.Lock()
	c.readClosed = true
	c.recvMu.Unlock()
	c.recvCond.Broadcast()
}

// Read implements net.Conn.
func (c *Conn) Read(p []byte) (int, error) {
	c.recvMu.Lock()
	defer c.recvMu.Unlock()
	for c.recvBuf.Len() == 0 {
		select {
		case <-c.done:
			if c.recvBuf.Len() == 0 {
				return 0, io.EOF
			}
		default:
		}
		if c.readClosed {
			return 0, io.EOF
		}
		c.recvCond.Wait()
	}
	return c.recvBuf.Read(p)
}

// Write implements net.Conn.
func (c *Conn) Write(p []byte) (int, error) {
	select {
	case <-c.done:
		return 0, errFlowDead("closed")
	default:
	}
	if err := c.sender.enqueue(p); err != nil {
		return 0, err
	}
	c.touch()
	return len(p), nil
}

// Close flushes pending data, sends FIN, and tears down locally.
func (c *Conn) Close() error {
	deadline := time.Now().Add(5 * time.Second)
	_ = c.sender.finish(deadline)
	c.finOnce.Do(func() {
		_ = c.mgr.sendFrame(c.gw, kindFlowFIN, &pb.FlowFIN{FlowId: c.flowID})
	})
	c.teardown("closed locally", false)
	return nil
}

func (c *Conn) teardown(reason string, notifyPeer bool) {
	c.closeOnce.Do(func() {
		if notifyPeer {
			_ = c.mgr.sendFrame(c.gw, kindFlowRST, &pb.FlowRST{
				FlowId: c.flowID, Reason: reason,
			})
		}
		close(c.done)
		c.recvMu.Lock()
		c.readClosed = true
		c.recvMu.Unlock()
		c.recvCond.Broadcast()
		c.sender.kill(reason)
		c.mgr.removeTCP(c.flowID)
		c.mgr.logger.Debug("tcp flow closed",
			"flow_id", c.flowID, "reason", reason)
	})
}

func (c *Conn) LocalAddr() net.Addr              { return nodeAddr("flow", string(c.mgr.self)) }
func (c *Conn) RemoteAddr() net.Addr             { return nodeAddr("gw", string(c.gw)) }
func (c *Conn) SetDeadline(time.Time) error      { return nil }
func (c *Conn) SetReadDeadline(time.Time) error  { return nil }
func (c *Conn) SetWriteDeadline(time.Time) error { return nil }

type flowAddr struct{ network, id string }

func (a flowAddr) Network() string { return a.network }
func (a flowAddr) String() string  { return a.id }

func nodeAddr(network, id string) net.Addr { return flowAddr{network, id} }

// UDPFlow is the client end of a datagram flow. Frames larger than
// ChunkSize are rejected.
type UDPFlow struct {
	mgr    *Manager
	flowID string
	gw     node.NodeID

	recv     chan []byte
	done     chan struct{}
	once     sync.Once
	idleAt   atomic.Int64

	acceptCh chan struct{}
	acceptMu sync.Mutex
	rejected string
}

func newUDPFlow(m *Manager, flowID string, gw node.NodeID) *UDPFlow {
	f := &UDPFlow{
		mgr:      m,
		flowID:   flowID,
		gw:       gw,
		recv:     make(chan []byte, 64),
		done:     make(chan struct{}),
		acceptCh: make(chan struct{}),
	}
	f.touch()
	go f.idleLoop()
	return f
}

func (f *UDPFlow) onAccept() {
	close(f.acceptCh)
}

func (f *UDPFlow) onReject(msg string) {
	f.acceptMu.Lock()
	f.rejected = msg
	f.acceptMu.Unlock()
	close(f.acceptCh)
}

func (f *UDPFlow) waitAccept(ctx context.Context) error {
	timeout := AcceptTimeout
	if d, ok := ctx.Deadline(); ok {
		timeout = time.Until(d)
	}
	select {
	case <-f.acceptCh:
		f.acceptMu.Lock()
		defer f.acceptMu.Unlock()
		if f.rejected != "" {
			return fmt.Errorf("gateway rejected flow: %s", f.rejected)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(timeout):
		return fmt.Errorf("timeout waiting for FlowAccept")
	case <-f.done:
		return errFlowDead("closed")
	}
}

func (f *UDPFlow) idleLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-f.done:
			return
		case <-ticker.C:
			if time.Since(time.Unix(0, f.idleAt.Load())) > IdleTimeout {
				f.teardown()
				_ = f.mgr.sendFrame(f.gw, kindFlowRST, &pb.FlowRST{
					FlowId: f.flowID, Reason: "idle timeout",
				})
				return
			}
		}
	}
}

func (f *UDPFlow) touch() { f.idleAt.Store(time.Now().UnixNano()) }

// SendDatagram sends one best-effort datagram through the gateway.
func (f *UDPFlow) SendDatagram(data []byte) error {
	if len(data) > ChunkSize {
		return fmt.Errorf("datagram %d exceeds chunk size %d", len(data), ChunkSize)
	}
	select {
	case <-f.done:
		return errFlowDead("closed")
	default:
	}
	f.touch()
	return f.mgr.sendFrame(f.gw, kindFlowDatagram, &pb.FlowDatagram{
		FlowId: f.flowID, Data: data,
	})
}

// RecvDatagram waits for the next datagram from the gateway.
func (f *UDPFlow) RecvDatagram(ctx context.Context) ([]byte, error) {
	select {
	case d := <-f.recv:
		f.touch()
		return d, nil
	case <-f.done:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *UDPFlow) deliver(data []byte) {
	f.touch()
	select {
	case f.recv <- data:
	default:
		f.mgr.logger.Warn("udp flow recv buffer full, dropping datagram",
			"flow_id", f.flowID)
	}
}

// Close tears down the datagram flow.
func (f *UDPFlow) Close() error {
	f.teardown()
	_ = f.mgr.sendFrame(f.gw, kindFlowFIN, &pb.FlowFIN{FlowId: f.flowID})
	return nil
}

func (f *UDPFlow) teardown() {
	f.once.Do(func() { close(f.done) })
	f.mgr.removeUDP(f.flowID)
}
