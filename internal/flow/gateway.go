package flow

import (
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/relaymesh/relaymesh/api/proto"
	"github.com/relaymesh/relaymesh/internal/node"
)

// GatewayHandler terminates flow frames on the gateway node: dials
// real internet TCP/UDP targets and pumps bytes both ways over the
// mesh. Register with mesh.AddPayloadHandler.
type GatewayHandler struct {
	self   node.NodeID
	send   Sender
	logger *slog.Logger
	sem    chan struct{}

	mu  sync.Mutex
	tcp map[string]*gwTCP
	udp map[string]*gwUDP
}

func NewGatewayHandler(self node.NodeID, send Sender, logger *slog.Logger) *GatewayHandler {
	return &GatewayHandler{
		self:   self,
		send:   send,
		logger: logger,
		sem:    make(chan struct{}, MaxFlows),
		tcp:    make(map[string]*gwTCP),
		udp:    make(map[string]*gwUDP),
	}
}

// HandlePacket consumes flow frames addressed to the gateway.
func (g *GatewayHandler) HandlePacket(source node.NodeID, payload []byte) bool {
	kind, raw, ok := UnwrapEnvelope(payload)
	if !ok {
		return false
	}
	switch kind {
	case kindFlowOpen:
		var msg pb.FlowOpen
		if proto.Unmarshal(raw, &msg) == nil {
			go g.handleOpen(source, &msg)
		}
	case kindFlowData:
		var msg pb.FlowData
		if proto.Unmarshal(raw, &msg) == nil {
			if f := g.tcpByID(msg.FlowId); f != nil {
				f.touch()
				f.recv.onData(msg.Seq, msg.Chunk)
			}
		}
	case kindFlowAck:
		var msg pb.FlowAck
		if proto.Unmarshal(raw, &msg) == nil {
			if f := g.tcpByID(msg.FlowId); f != nil {
				f.touch()
				f.sender.onAck(msg.AckSeq)
			}
		}
	case kindFlowFIN:
		var msg pb.FlowFIN
		if proto.Unmarshal(raw, &msg) == nil {
			if f := g.tcpByID(msg.FlowId); f != nil {
				f.touch()
				f.closeWrite()
			}
			if u := g.udpByID(msg.FlowId); u != nil {
				u.teardown("fin from client")
			}
		}
	case kindFlowRST:
		var msg pb.FlowRST
		if proto.Unmarshal(raw, &msg) == nil {
			if f := g.tcpByID(msg.FlowId); f != nil {
				f.teardown(msg.Reason)
			}
			if u := g.udpByID(msg.FlowId); u != nil {
				u.teardown(msg.Reason)
			}
		}
	case kindFlowDatagram:
		var msg pb.FlowDatagram
		if proto.Unmarshal(raw, &msg) == nil {
			if u := g.udpByID(msg.FlowId); u != nil {
				u.touch()
				_, _ = u.conn.Write(msg.Data)
			}
		}
	}
	return true
}

func (g *GatewayHandler) handleOpen(source node.NodeID, req *pb.FlowOpen) {
	if req.FlowId == "" || req.TargetHost == "" || req.TargetPort == 0 {
		g.reject(source, req.FlowId, "invalid FlowOpen")
		return
	}
	select {
	case g.sem <- struct{}{}:
	default:
		g.reject(source, req.FlowId, "gateway flow limit reached")
		return
	}

	target := net.JoinHostPort(req.TargetHost, fmt.Sprintf("%d", req.TargetPort))
	if req.Udp {
		g.openUDP(source, req.FlowId, target)
		return
	}
	g.openTCP(source, req.FlowId, target)
}

func (g *GatewayHandler) openTCP(source node.NodeID, flowID, target string) {
	conn, err := net.DialTimeout("tcp", target, DialTimeout)
	if err != nil {
		g.releaseSem()
		g.reject(source, flowID, fmt.Sprintf("dial %s: %v", target, err))
		return
	}

	f := &gwTCP{
		flowID: flowID,
		client: source,
		conn:   conn,
		done:   make(chan struct{}),
	}
	f.sender = newReliableSender(flowID, func(seq uint32, chunk []byte) error {
		return g.sendFrame(source, kindFlowData, &pb.FlowData{
			FlowId: flowID, Seq: seq, Chunk: chunk,
		})
	}, func(reason string) {
		f.teardown("sender dead: " + reason)
	})
	f.recv = newReliableReceiver(func(chunk []byte) {
		_, _ = conn.Write(chunk)
	}, func(ackSeq uint32) error {
		return g.sendFrame(source, kindFlowAck, &pb.FlowAck{
			FlowId: flowID, AckSeq: ackSeq,
		})
	})
	f.remove = func() { g.removeTCP(flowID) }
	f.sendRaw = func(raw []byte) error { return g.send(source, raw) }
	f.logger = g.logger
	f.touch()

	g.mu.Lock()
	if _, exists := g.tcp[flowID]; exists {
		g.mu.Unlock()
		conn.Close()
		g.releaseSem()
		g.reject(source, flowID, "duplicate flow id")
		return
	}
	g.tcp[flowID] = f
	g.mu.Unlock()

	if err := g.sendFrame(source, kindFlowAccept, &pb.FlowAccept{FlowId: flowID}); err != nil {
		f.teardown("accept send failed")
		return
	}
	g.logger.Info("tcp flow open", "flow_id", flowID, "client", source, "target", target)
	go f.pump()
}

func (g *GatewayHandler) openUDP(source node.NodeID, flowID, target string) {
	conn, err := net.DialTimeout("udp", target, DialTimeout)
	if err != nil {
		g.releaseSem()
		g.reject(source, flowID, fmt.Sprintf("dial %s: %v", target, err))
		return
	}
	f := &gwUDP{
		flowID: flowID,
		client: source,
		conn:   conn,
		done:   make(chan struct{}),
		remove: func() { g.removeUDP(flowID) },
		logger: g.logger,
	}
	f.touch()

	g.mu.Lock()
	if _, exists := g.udp[flowID]; exists {
		g.mu.Unlock()
		conn.Close()
		g.releaseSem()
		g.reject(source, flowID, "duplicate flow id")
		return
	}
	g.udp[flowID] = f
	g.mu.Unlock()

	g.logger.Info("udp flow open", "flow_id", flowID, "client", source, "target", target)
	if err := g.sendFrame(source, kindFlowAccept, &pb.FlowAccept{FlowId: flowID}); err != nil {
		f.teardown("accept send failed")
		return
	}
	go f.pump(g)
}

func (g *GatewayHandler) reject(source node.NodeID, flowID, msg string) {
	if err := g.sendFrame(source, kindFlowReject, &pb.FlowReject{
		FlowId: flowID, Error: msg,
	}); err != nil {
		g.logger.Debug("reject send failed", "flow_id", flowID, "error", err)
	}
}

func (g *GatewayHandler) sendFrame(dest node.NodeID, kind byte, msg proto.Message) error {
	raw, err := Wrap(kind, msg)
	if err != nil {
		return err
	}
	return g.send(dest, raw)
}

func (g *GatewayHandler) releaseSem() {
	select {
	case <-g.sem:
	default:
	}
}

func (g *GatewayHandler) tcpByID(id string) *gwTCP {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.tcp[id]
}

func (g *GatewayHandler) udpByID(id string) *gwUDP {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.udp[id]
}

func (g *GatewayHandler) removeTCP(id string) {
	g.mu.Lock()
	delete(g.tcp, id)
	g.mu.Unlock()
	g.releaseSem()
}

func (g *GatewayHandler) removeUDP(id string) {
	g.mu.Lock()
	delete(g.udp, id)
	g.mu.Unlock()
	g.releaseSem()
}

// Stats reports open gateway-side flows.
func (g *GatewayHandler) Stats() (tcp, udp int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.tcp), len(g.udp)
}

// gwTCP is one client's TCP flow on the gateway.
type gwTCP struct {
	flowID string
	client node.NodeID
	conn   net.Conn

	sender *reliableSender // gateway -> client (reliable)
	recv   *reliableReceiver

	closeOnce sync.Once
	done      chan struct{}
	idleAt    atomic.Int64
	remove    func()
	sendRaw   func([]byte) error
	logger    *slog.Logger
}

func (f *gwTCP) touch() { f.idleAt.Store(time.Now().UnixNano()) }

func (f *gwTCP) pump() {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	// internet conn -> mesh (reliable chunks)
	go func() {
		buf := make([]byte, ChunkSize)
		for {
			n, err := f.conn.Read(buf)
			if n > 0 {
				f.touch()
				if enqErr := f.sender.enqueue(buf[:n]); enqErr != nil {
					f.teardown("enqueue: " + enqErr.Error())
					return
				}
			}
			if err != nil {
				_ = f.sender.finish(time.Now().Add(5 * time.Second))
				if raw, werr := Wrap(kindFlowFIN, &pb.FlowFIN{FlowId: f.flowID}); werr == nil && f.sendRaw != nil {
					_ = f.sendRaw(raw)
				}
				f.teardown("remote closed")
				return
			}
		}
	}()

	for {
		select {
		case <-f.done:
			return
		case now := <-ticker.C:
			dead, reason := f.sender.trySend(now)
			if dead {
				f.teardown(reason)
				return
			}
			if time.Since(time.Unix(0, f.idleAt.Load())) > IdleTimeout {
				f.teardown("idle timeout")
				return
			}
			f.sender.mu.Lock()
			f.sender.cond.Broadcast()
			f.sender.mu.Unlock()
		}
	}
}

func (f *gwTCP) closeWrite() {
	if tc, ok := f.conn.(*net.TCPConn); ok {
		_ = tc.CloseWrite()
	} else {
		f.teardown("fin on non-tcp flow")
	}
}

func (f *gwTCP) teardown(reason string) {
	f.closeOnce.Do(func() {
		close(f.done)
		_ = f.conn.Close()
		f.sender.kill(reason)
		if f.remove != nil {
			f.remove()
		}
		if f.logger != nil {
			f.logger.Info("tcp flow closed", "flow_id", f.flowID, "reason", reason)
		}
	})
}

// gwUDP is one client's UDP flow on the gateway.
type gwUDP struct {
	flowID string
	client node.NodeID
	conn   net.Conn

	closeOnce sync.Once
	done      chan struct{}
	idleAt    atomic.Int64
	remove    func()
	logger    *slog.Logger
}

func (f *gwUDP) touch() { f.idleAt.Store(time.Now().UnixNano()) }

func (f *gwUDP) pump(g *GatewayHandler) {
	go func() {
		buf := make([]byte, 2048)
		for {
			n, err := f.conn.Read(buf)
			if n > 0 {
				f.touch()
				data := make([]byte, n)
				copy(data, buf[:n])
				if len(data) > ChunkSize {
					data = data[:ChunkSize]
				}
				if sendErr := g.sendFrame(f.client, kindFlowDatagram, &pb.FlowDatagram{
					FlowId: f.flowID, Data: data,
				}); sendErr != nil {
					f.teardown("send datagram: " + sendErr.Error())
					return
				}
			}
			if err != nil {
				f.teardown("remote closed")
				return
			}
		}
	}()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-f.done:
			return
		case <-ticker.C:
			if time.Since(time.Unix(0, f.idleAt.Load())) > IdleTimeout {
				f.teardown("idle timeout")
				return
			}
		}
	}
}

func (f *gwUDP) teardown(reason string) {
	f.closeOnce.Do(func() {
		close(f.done)
		_ = f.conn.Close()
		if f.remove != nil {
			f.remove()
		}
		if f.logger != nil {
			f.logger.Info("udp flow closed", "flow_id", f.flowID, "reason", reason)
		}
	})
}
