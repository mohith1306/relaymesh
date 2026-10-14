package test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/relaymesh/relaymesh/internal/ai"
	"github.com/relaymesh/relaymesh/internal/forwarding"
	"github.com/relaymesh/relaymesh/internal/mesh"
	"github.com/relaymesh/relaymesh/internal/node"
	"github.com/relaymesh/relaymesh/internal/telemetry"
)

// TestMeshPhase11AIAdaptsToDegradation is the M15 killer demonstration:
// a four-node diamond (A—B—C and A—D—C) where deterministic Dijkstra is
// loss-blind, live probe telemetry reports loss on A—B, and the real
// Python AI service — fed Go's candidate paths plus live per-link
// metrics — switches traffic to A—D—C while the routing table still
// says B. Degradation in, AI route out; forwarder never breaks.
func TestMeshPhase11AIAdaptsToDegradation(t *testing.T) {
	aiAddr, _ := startPythonAI(t)

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

	// Two loop-free paths A→C: via B (cheap, will degrade) and via D.
	nodeA := newNode("node-A", 9451, 20171, []uint16{9452, 9454})
	nodeB := newNode("node-B", 9452, 20172, []uint16{9451, 9453})
	nodeC := newNode("node-C", 9453, 20173, []uint16{9452, 9454})
	nodeD := newNode("node-D", 9454, 20174, []uint16{9451, 9453})

	// Pin link costs before traffic so deterministic routing reliably
	// prefers A—B—C (1ms vs 5ms): AI must later beat this preference
	// using loss the graph never sees.
	nodeA.SetLinkQuality("node-B", 1*time.Millisecond, 0)
	nodeA.SetLinkQuality("node-D", 5*time.Millisecond, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	for _, n := range []*mesh.MeshNode{nodeA, nodeB, nodeC, nodeD} {
		if err := n.Start(ctx); err != nil {
			t.Fatalf("failed to start %s: %v", n.ID(), err)
		}
		defer n.Stop()
	}
	for _, n := range []*mesh.MeshNode{nodeB, nodeD} {
		n := n
		go func() {
			for range n.Delivered() {
			}
		}()
	}

	// AI wiring on A: real gRPC client -> Python service, live telemetry.
	collector := telemetry.NewCollector("node-A")
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				nodeA.SyncTelemetry(collector)
			}
		}
	}()

	aiClient := ai.NewClient(aiAddr, logger.With("component", "ai"))
	if err := aiClient.Connect(ctx); err != nil {
		t.Fatalf("connect to AI service failed: %v", err)
	}
	defer aiClient.Close()
	aiRouter := ai.NewAIRouter("node-A", nodeA.Router(), aiClient, logger.With("component", "ai-router"))
	aiRouter.SetAIAvailable(true)
	aiRouter.SetCollector("node-A", collector)
	nodeA.SetAdvisor(aiRouter)

	// Deterministic route to C must be pinned to B before any AI say.
	if nh := waitRouteTo(t, nodeA, "node-C", 15*time.Second); nh != "node-B" {
		t.Fatalf("expected deterministic next hop node-B, got %s", nh)
	}
	waitFor(t, 10*time.Second, "telemetry peer metrics", func() bool {
		return len(collector.GetAllPeerMetrics()) == 2
	})

	// Phase 1 (healthy): AI agrees with deterministic — traffic via B.
	healthy := sendUntilDelivered(t, nodeA, nodeC, "node-C",
		[]byte("phase11-healthy"), 15*time.Second)
	assertPath(t, healthy, "node-A", "node-B", "node-C")
	q1, _ := aiRouter.Stats()
	if q1 < 1 {
		t.Fatalf("expected at least 1 AI query in healthy phase, got %d", q1)
	}

	// Phase 2: degrade A—B (0.4 loss). The routing graph never sees
	// loss; probe telemetry does — synced into the collector the AI
	// both scores with and invalidates its cache on.
	nodeA.SetLinkQuality("node-B", 1*time.Millisecond, 0.4)
	time.Sleep(400 * time.Millisecond) // several telemetry ticks

	degraded := sendUntilDelivered(t, nodeA, nodeC, "node-C",
		[]byte("phase11-degraded"), 20*time.Second)
	assertPath(t, degraded, "node-A", "node-D", "node-C")

	// The proof: deterministic routing still points at B — only the AI
	// layer reacted to the loss.
	if nh, ok := nodeA.Router().GetNextHop("node-C"); !ok || nh != "node-B" {
		t.Fatalf("deterministic next hop must stay node-B (loss-blind), got %s %v", nh, ok)
	}
	q2, served := aiRouter.Stats()
	if q2 <= q1 {
		t.Fatalf("expected re-query after degradation: queries before=%d after=%d", q1, q2)
	}
	if served < 2 {
		t.Fatalf("expected AI routes served across both phases, got %d", served)
	}
	t.Logf("M15 adaptation proven: healthy via B (queries=%d), degraded via D (queries=%d, served=%d), deterministic still B",
		q1, q2, served)
}

// assertPath checks the forwarder-recorded delivery path.
func assertPath(t *testing.T, d forwarding.DeliveredPacket, want ...node.NodeID) {
	t.Helper()
	if len(d.Path) != len(want) {
		t.Fatalf("delivered path %v, want %v", d.Path, want)
	}
	for i := range want {
		if d.Path[i] != want[i] {
			t.Fatalf("delivered path %v, want %v", d.Path, want)
		}
	}
}

// waitFor polls cond until it holds or timeout expires.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// startPythonAI launches the real Python AI service on a dedicated
// port and returns its address. Skips when no interpreter is available.
func startPythonAI(t *testing.T) (addr string, stop func()) {
	t.Helper()
	python := ""
	for _, cand := range []string{
		filepath.Join("..", ".venv", "bin", "python"),
		filepath.Join("..", ".venv", "bin", "python3"),
	} {
		if abs, err := filepath.Abs(cand); err == nil {
			if _, err := os.Stat(abs); err == nil {
				python = abs
				break
			}
		}
	}
	if python == "" {
		p, err := exec.LookPath("python3")
		if err != nil {
			t.Skip("no python interpreter for AI service")
		}
		python = p
	}

	port := 50661
	cmd := exec.Command(python, "-m", "ai.service.server", strconv.Itoa(port))
	cmd.Dir = ".." // repo root: package ai lives there
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Start(); err != nil {
		t.Fatalf("start AI service: %v", err)
	}
	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("AI service output:\n%s", out.String())
		}
		stop()
	})

	addr = fmt.Sprintf("127.0.0.1:%d", port)
	deadline := time.Now().Add(8 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", addr, 250*time.Millisecond)
		if err == nil {
			conn.Close()
			return addr, stop
		}
		if time.Now().After(deadline) {
			stop()
			t.Fatalf("AI service never came up on %s: %v\n%s", addr, err, out.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
}
