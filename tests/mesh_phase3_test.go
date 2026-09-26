package test

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/relaymesh/relaymesh/internal/gateway"
	"github.com/relaymesh/relaymesh/internal/mesh"
	"github.com/relaymesh/relaymesh/internal/node"
)

// TestMeshPhase3Gateway proves internet egress through the mesh:
//
//	node-A -> node-B -> node-GW -> echo server ("the internet")
//	echo server -> node-GW -> node-B -> node-A
//
// A only hears B, so both the egress request and the reply must be
// relayed by B. The echo server stands in for an internet endpoint.
func TestMeshPhase3Gateway(t *testing.T) {
	logger := slog.Default()

	// Fake "internet": UDP endpoint that echoes with a prefix.
	echoAddr, err := net.ResolveUDPAddr("udp4", "127.0.0.1:19711")
	if err != nil {
		t.Fatalf("failed to resolve echo addr: %v", err)
	}
	echoConn, err := net.ListenUDP("udp4", echoAddr)
	if err != nil {
		t.Fatalf("failed to start echo server: %v", err)
	}
	defer echoConn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	go func() {
		buf := make([]byte, 65536)
		for {
			_ = echoConn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			n, remote, err := echoConn.ReadFromUDP(buf)
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					continue
				}
			}
			_, _ = echoConn.WriteToUDP(append([]byte("echo:"), buf[:n]...), remote)
		}
	}()

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

	nodeA := newNode("node-A", 9371, 19731, []uint16{9372})
	nodeB := newNode("node-B", 9372, 19732, []uint16{9371, 9373})
	gw := gateway.New(mesh.Config{
		NodeID:              "node-GW",
		Address:             "127.0.0.1",
		DiscoveryPort:       9373,
		DataPort:            19733,
		KnownDiscoveryPorts: []uint16{9372},
		HeartbeatInterval:   100 * time.Millisecond,
		PeerTimeout:         2 * time.Second,
	}, logger)

	for _, n := range []*mesh.MeshNode{nodeA, nodeB} {
		if err := n.Start(ctx); err != nil {
			t.Fatalf("failed to start %s: %v", n.ID(), err)
		}
		defer n.Stop()
	}
	if err := gw.Start(ctx); err != nil {
		t.Fatalf("failed to start gateway: %v", err)
	}
	defer gw.Stop()

	// A must learn the route to the gateway through B.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, ok := nodeA.Router().GetNextHop("node-GW"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("A never learned a route to the gateway (peers=%d)",
				nodeA.Stats().PeerCount)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("route A -> GW confirmed")

	// The relay needs its own route before it can forward.
	waitRouteTo(t, nodeB, "node-GW", 10*time.Second)

	// Round-trip data through the mesh to the "internet" and back.
	resp, err := gateway.SendEgressRequest(nodeA, "node-GW", "127.0.0.1", 19711, []byte("ping-internet"), 5*time.Second)
	if err != nil {
		t.Fatalf("egress request failed: %v", err)
	}
	if string(resp) != "echo:ping-internet" {
		t.Fatalf("wrong egress reply: %q", string(resp))
	}
	t.Logf("egress round-trip ok: %q", string(resp))

	// The gateway must have tracked the NAT flow.
	if n := gw.NAT().Count(); n < 1 {
		t.Fatalf("gateway tracked %d NAT flows, expected >= 1", n)
	}

	// B must have relayed both the request and the reply.
	if fwd := nodeB.Stats().Forwarded; fwd < 2 {
		t.Fatalf("B forwarded %d packets, expected >= 2 (request + reply)", fwd)
	}
	t.Logf("relay proven: B forwarded %d packets", nodeB.Stats().Forwarded)
}
