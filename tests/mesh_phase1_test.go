package test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/relaymesh/relaymesh/internal/mesh"
	"github.com/relaymesh/relaymesh/internal/node"
)

// TestMeshPhase1MultiHop proves the Phase 1 v1 wiring:
// discovery -> routing -> forwarding -> transport, with a real
// multi-hop relay A -> B -> C over UDP.
//
// Topology is restricted via KnownDiscoveryPorts so A only hears B,
// B hears A and C, and C only hears B. A must learn the B-C link
// from B's heartbeat neighbor list and route A -> B -> C.
func TestMeshPhase1MultiHop(t *testing.T) {
	logger := slog.Default()

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

	nodeA := newNode("node-A", 9331, 19331, []uint16{9332})
	nodeB := newNode("node-B", 9332, 19332, []uint16{9331, 9333})
	nodeC := newNode("node-C", 9333, 19333, []uint16{9332})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	for _, n := range []*mesh.MeshNode{nodeA, nodeB, nodeC} {
		if err := n.Start(ctx); err != nil {
			t.Fatalf("failed to start %s: %v", n.ID(), err)
		}
		defer n.Stop()
	}

	// Wait until A computes a route to C via B.
	deadline := time.Now().Add(10 * time.Second)
	var nextHop node.NodeID
	for {
		if nh, ok := nodeA.Router().GetNextHop("node-C"); ok {
			nextHop = nh
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("A never learned a route to C (peers=%d routes=%v)",
				nodeA.Stats().PeerCount, nodeA.Router().GetTable().GetAllDestinations())
		}
		time.Sleep(100 * time.Millisecond)
	}
	if nextHop != "node-B" {
		t.Fatalf("expected A -> C next hop to be node-B, got %s", nextHop)
	}
	t.Logf("route A -> C via %s confirmed", nextHop)

	// Send a real packet from A to C; it must be relayed by B.
	payload := []byte("hello-via-b")
	if err := nodeA.Send("node-C", payload); err != nil {
		t.Fatalf("A failed to send to C: %v", err)
	}

	select {
	case d := <-nodeC.Delivered():
		if string(d.Payload) != string(payload) {
			t.Fatalf("C received wrong payload: %q", string(d.Payload))
		}
		if d.Source != "node-A" {
			t.Fatalf("C received packet from wrong source: %s", d.Source)
		}
		t.Logf("C delivered packet from %s payload=%q", d.Source, string(d.Payload))
	case <-time.After(5 * time.Second):
		t.Fatalf("C never received packet from A (A stats=%+v B stats=%+v C stats=%+v)",
			nodeA.Stats(), nodeB.Stats(), nodeC.Stats())
	}

	// B must have actually forwarded the packet (proof of relay).
	if fwd := nodeB.Stats().Forwarded; fwd < 1 {
		t.Fatalf("B forwarded %d packets, expected >= 1 (no relay happened)", fwd)
	}
	t.Logf("multi-hop relay proven: A -> B -> C (B forwarded=%d)", nodeB.Stats().Forwarded)
}
