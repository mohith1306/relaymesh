package test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/relaymesh/relaymesh/internal/mesh"
	"github.com/relaymesh/relaymesh/internal/node"
	"github.com/relaymesh/relaymesh/internal/telemetry"
)

// TestMeshPhase5Telemetry proves dashboard/AI metrics come from the real
// data plane: after two nodes exchange probes and traffic, the
// telemetry collector must show measured RTT, zero loss, and per-peer
// entries — not zeros or simulated values.
func TestMeshPhase5Telemetry(t *testing.T) {
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

	nodeA := newNode("node-A", 9401, 20031, []uint16{9402})
	nodeB := newNode("node-B", 9402, 20032, []uint16{9401})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	for _, n := range []*mesh.MeshNode{nodeA, nodeB} {
		if err := n.Start(ctx); err != nil {
			t.Fatalf("failed to start %s: %v", n.ID(), err)
		}
		defer n.Stop()
	}

	collector := telemetry.NewCollector("node-A")

	// Exchange real traffic so there is something to measure.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, ok := nodeA.Router().GetNextHop("node-B"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("A never discovered B")
		}
		time.Sleep(100 * time.Millisecond)
	}
	for i := 0; i < 5; i++ {
		if err := nodeA.Send("node-B", []byte("telemetry-probe-traffic")); err != nil {
			t.Fatalf("send failed: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	// Drain B's deliveries so the channel never blocks the data plane.
	go func() {
		for range nodeB.Delivered() {
		}
	}()

	// Let probes accumulate, then sync and assert real measurements.
	time.Sleep(1500 * time.Millisecond)
	nodeA.SyncTelemetry(collector)

	m := collector.GetMetrics()
	if m.Latency <= 0 {
		t.Fatalf("expected measured RTT latency > 0, got %v", m.Latency)
	}
	if m.PacketLoss != 0 {
		t.Fatalf("expected zero loss on localhost, got %v", m.PacketLoss)
	}
	if m.Bandwidth != mesh.DefaultBandwidth {
		t.Fatalf("expected bandwidth %v, got %v", mesh.DefaultBandwidth, m.Bandwidth)
	}
	if m.Stability != 1.0 {
		t.Fatalf("expected stability 1.0 with all links up, got %v", m.Stability)
	}

	peers := collector.GetAllPeerMetrics()
	if len(peers) != 1 {
		t.Fatalf("expected 1 peer entry, got %d", len(peers))
	}
	if peers[0].PeerID != "node-B" {
		t.Fatalf("expected peer node-B, got %s", peers[0].PeerID)
	}
	if peers[0].Latency <= 0 {
		t.Fatalf("expected per-peer RTT > 0, got %v", peers[0].Latency)
	}

	fv := collector.ToFeatureVector()
	if len(fv) != 10 {
		t.Fatalf("expected 10-dim feature vector, got %d", len(fv))
	}
	if fv[0] <= 0 {
		t.Fatalf("feature vector latency must be measured, got %v", fv[0])
	}
	t.Logf("telemetry live: latency=%.3fms loss=%.3f stability=%.2f",
		m.Latency, m.PacketLoss, m.Stability)
}
