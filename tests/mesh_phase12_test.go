package test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/relaymesh/relaymesh/internal/flow"
	"github.com/relaymesh/relaymesh/internal/gateway"
	"github.com/relaymesh/relaymesh/internal/mesh"
	"github.com/relaymesh/relaymesh/internal/node"
)

// TestMeshPhase12FlowTunnel proves continuous TCP and UDP sessions
// through the mesh gateway — the flow tunnel that lets external apps
// (WhatsApp, browsers) ride on RelayMesh:
//
//	node-A -> node-B -> node-GW -> TCP/UDP endpoint ("the internet")
//	endpoint -> node-GW -> node-B -> node-A
//
// A only hears B, so every frame crosses the multi-hop path twice.
// The TCP payload is far larger than the mesh MTU, which forces the
// chunking + cumulative-ACK retransmit path.
func TestMeshPhase12FlowTunnel(t *testing.T) {
	logger := slog.Default()

	// Fake "internet": TCP echo (large) + UDP echo.
	tcpPort := freeTCPPort(t)
	tcpLn, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", tcpPort))
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	defer tcpLn.Close()
	go func() {
		for {
			c, err := tcpLn.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}(c)
		}
	}()

	udpPort := freeUDPPorts(t, 1)[0]
	udpAddr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: int(udpPort)}
	udpConn, err := net.ListenUDP("udp4", udpAddr)
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	defer udpConn.Close()
	go func() {
		buf := make([]byte, 2048)
		for {
			n, remote, err := udpConn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			_, _ = udpConn.WriteToUDP(append([]byte("echo:"), buf[:n]...), remote)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	ports := freeUDPPorts(t, 6)
	discA, discB, discG := ports[0], ports[1], ports[2]
	dataA, dataB, dataG := ports[3], ports[4], ports[5]

	newNode := func(id string, discPort, dataPort uint16, known []uint16) *mesh.MeshNode {
		return mesh.New(mesh.Config{
			NodeID:              node.NodeID(id),
			Address:             "127.0.0.1",
			DiscoveryPort:       discPort,
			DataPort:            dataPort,
			KnownDiscoveryPorts: known,
			HeartbeatInterval:   100 * time.Millisecond,
			PeerTimeout:         2 * time.Second,
		}, logger)
	}

	nodeA := newNode("node-A", discA, dataA, []uint16{discB})
	nodeB := newNode("node-B", discB, dataB, []uint16{discA, discG})
	gw := gateway.New(mesh.Config{
		NodeID:              "node-GW",
		Address:             "127.0.0.1",
		DiscoveryPort:       discG,
		DataPort:            dataG,
		KnownDiscoveryPorts: []uint16{discB},
		HeartbeatInterval:   100 * time.Millisecond,
		PeerTimeout:         2 * time.Second,
	}, logger.With("component", "gateway"))

	for _, n := range []*mesh.MeshNode{nodeA, nodeB} {
		if err := n.Start(ctx); err != nil {
			t.Fatalf("start %s: %v", n.ID(), err)
		}
		defer n.Stop()
	}
	if err := gw.Start(ctx); err != nil {
		t.Fatalf("start gateway: %v", err)
	}
	defer gw.Stop()

	// Drain B's delivered channel; it is only a relay hop here.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-nodeB.Delivered():
			}
		}
	}()

	// Client-side flow manager on A, wired into the mesh classifier.
	manager := flow.NewManager("node-A", nodeA.Send, logger.With("component", "flow"))
	nodeA.AddPayloadHandler(manager.HandlePacket)

	waitFor(t, 10*time.Second, "A to see B and B to see the gateway", func() bool {
		return hasPeer(nodeA, "node-B") && hasPeer(nodeB, "node-GW")
	})

	// Routes must exist before flow frames can be routed.
	waitFor(t, 10*time.Second, "A to have a route to the gateway", func() bool {
		_, ok := nodeA.Router().GetNextHop("node-GW")
		return ok
	})
	waitFor(t, 10*time.Second, "B to have a route to the gateway", func() bool {
		_, ok := nodeB.Router().GetNextHop("node-GW")
		return ok
	})

	t.Run("TCP", func(t *testing.T) {
		conn, err := manager.OpenTCP(ctx, "node-GW", "127.0.0.1", uint16(tcpPort))
		if err != nil {
			t.Fatalf("OpenTCP: %v", err)
		}
		defer conn.Close()

		// ~80KB patterned payload: far above the 1200B chunk / MTU,
		// so this exercises chunking, windowing, and retransmit. The
		// echo server streams (io.Copy), so no half-close is needed.
		payload := make([]byte, 80*1024)
		for i := range payload {
			payload[i] = byte(i % 251)
		}
		writeDone := make(chan error, 1)
		go func() {
			_, werr := conn.Write(payload)
			writeDone <- werr
		}()

		var received bytes.Buffer
		buf := make([]byte, 8192)
		for received.Len() < len(payload) {
			n, rerr := conn.Read(buf)
			if n > 0 {
				received.Write(buf[:n])
			}
			if rerr != nil {
				t.Fatalf("read after %d bytes: %v", received.Len(), rerr)
			}
		}
		if err := <-writeDone; err != nil {
			t.Fatalf("write: %v", err)
		}
		if !bytes.Equal(received.Bytes(), payload) {
			t.Fatalf("tcp echo mismatch: got %d bytes, content differs", received.Len())
		}
		tcpFlows, _ := gw.Flows().Stats()
		if tcpFlows != 1 {
			t.Fatalf("gateway tcp flows = %d, want 1", tcpFlows)
		}
	})

	t.Run("UDP", func(t *testing.T) {
		f, err := manager.OpenUDP(ctx, "node-GW", "127.0.0.1", udpPort)
		if err != nil {
			t.Fatalf("OpenUDP: %v", err)
		}
		defer f.Close()

		if err := f.SendDatagram([]byte("ping-123")); err != nil {
			t.Fatalf("SendDatagram: %v", err)
		}
		rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
		defer rcancel()
		resp, err := f.RecvDatagram(rctx)
		if err != nil {
			t.Fatalf("RecvDatagram: %v", err)
		}
		if string(resp) != "echo:ping-123" {
			t.Fatalf("udp echo = %q, want %q", resp, "echo:ping-123")
		}
	})

	t.Run("RejectUnreachable", func(t *testing.T) {
		// Nothing listens here; gateway must FlowReject, not hang.
		dead := freeTCPPort(t)
		rctx, rcancel := context.WithTimeout(ctx, 20*time.Second)
		defer rcancel()
		_, err := manager.OpenTCP(rctx, "node-GW", "127.0.0.1", uint16(dead))
		if err == nil {
			t.Fatalf("expected dial rejection for closed port")
		}
	})
}

func hasPeer(n *mesh.MeshNode, id string) bool {
	for _, p := range n.Discovery().Snapshot() {
		if string(p.ID) == id {
			return true
		}
	}
	return false
}
