package ai

import (
	"context"
	"log/slog"
	"testing"
	"time"

	pb "github.com/relaymesh/relaymesh/api/proto"
	"github.com/relaymesh/relaymesh/internal/routing"
	"github.com/relaymesh/relaymesh/internal/telemetry"
)

// diamondRouter builds A—B—C and A—D—C: two loop-free paths to node-C.
func diamondRouter(t *testing.T) *routing.Router {
	t.Helper()
	r := routing.New("node-A", slog.Default())
	r.UpdateLink("node-A", "node-B", 1, 100, 0)
	r.UpdateLink("node-B", "node-C", 10, 100, 0)
	r.UpdateLink("node-A", "node-D", 5, 100, 0)
	r.UpdateLink("node-D", "node-C", 10, 100, 0)
	return r
}

func pathRec(path []string, confidence float64) *pb.RouteRecommendation {
	return &pb.RouteRecommendation{
		Source: "node-A", Destination: "node-C",
		Path: path, Confidence: confidence, Score: 10.0,
	}
}

func newTestAdvisor(t *testing.T, mock *mockAIServer, r *routing.Router) (*AIRouter, *telemetry.MetricsCollector) {
	t.Helper()
	client := NewClient(mockAddr(t, mock), slog.Default())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	t.Cleanup(func() { client.Close() })

	ar := NewAIRouter("node-A", r, client, slog.Default())
	ar.SetAIAvailable(true)
	collector := telemetry.NewCollector("node-A")
	collector.UpdatePeerMetrics("node-B", 1.0, 0.0, 100.0)
	collector.UpdatePeerMetrics("node-D", 5.0, 0.0, 100.0)
	ar.SetCollector("node-A", collector)
	return ar, collector
}

func mockAddr(t *testing.T, mock *mockAIServer) string {
	t.Helper()
	addr, stop := startMockAI(t, mock)
	t.Cleanup(stop)
	return addr
}

// While telemetry is unchanged, accepted advice must be served from
// cache: one gRPC call covers many sends.
func TestAICacheServesBetweenQueries(t *testing.T) {
	mock := &mockAIServer{recs: []*pb.RouteRecommendation{
		pathRec([]string{"node-A", "node-B", "node-C"}, 0.9),
		pathRec([]string{"node-A", "node-D", "node-C"}, 0.9),
	}}
	ar, _ := newTestAdvisor(t, mock, diamondRouter(t))

	ctx := context.Background()
	nh, ok := ar.AdviseRoute(ctx, "node-C")
	if !ok || nh != "node-B" {
		t.Fatalf("expected first advice node-B, got %s %v", nh, ok)
	}
	for i := 0; i < 5; i++ {
		nh, ok = ar.AdviseRoute(ctx, "node-C")
		if !ok || nh != "node-B" {
			t.Fatalf("cached advice %d drifted to %s %v", i, nh, ok)
		}
	}
	if mock.callCount() != 1 {
		t.Fatalf("expected cached advice to serve with 1 gRPC call, got %d", mock.callCount())
	}
}

// When measured link conditions change, the cached advice must be
// dropped and the AI re-consulted — degradation must flip the route.
func TestAIRequeriesWhenLinkConditionsChange(t *testing.T) {
	mock := &mockAIServer{recs: []*pb.RouteRecommendation{
		pathRec([]string{"node-A", "node-B", "node-C"}, 0.9),
		pathRec([]string{"node-A", "node-D", "node-C"}, 0.9),
	}}
	ar, collector := newTestAdvisor(t, mock, diamondRouter(t))

	ctx := context.Background()
	nh, ok := ar.AdviseRoute(ctx, "node-C")
	if !ok || nh != "node-B" {
		t.Fatalf("expected healthy advice node-B, got %s %v", nh, ok)
	}

	// Telemetry reports degradation on A—B (probes -> collector).
	collector.UpdatePeerMetrics("node-B", 1.0, 0.4, 100.0)
	time.Sleep(150 * time.Millisecond) // past changedRequeryInterval

	nh, ok = ar.AdviseRoute(ctx, "node-C")
	if !ok {
		t.Fatalf("expected advice after degradation, got fallback-miss")
	}
	if nh != "node-D" {
		t.Fatalf("expected switch to node-D after 0.4 loss on B, got %s", nh)
	}
	if mock.callCount() != 2 {
		t.Fatalf("expected re-query on condition change, got %d calls", mock.callCount())
	}

	// Deterministic routing must still be loss-blind: unchanged.
	det, exists := ar.router.GetNextHop("node-C")
	if !exists || det != "node-B" {
		t.Fatalf("expected deterministic next hop node-B, got %s %v", det, exists)
	}
}

// The request must carry Go-enumerated candidate paths and the
// source's live per-peer metrics — the inputs Python scores.
func TestAISendsCandidatePathsAndPeerMetrics(t *testing.T) {
	mock := &mockAIServer{rec: pathRec([]string{"node-A", "node-B", "node-C"}, 0.9)}
	ar, _ := newTestAdvisor(t, mock, diamondRouter(t))

	if _, ok := ar.AdviseRoute(context.Background(), "node-C"); !ok {
		t.Fatalf("expected advice")
	}

	req := mock.request()
	if req == nil {
		t.Fatalf("no request captured")
	}
	if len(req.CandidatePaths) != 2 {
		t.Fatalf("expected 2 candidate paths, got %d", len(req.CandidatePaths))
	}
	want := [][]string{
		{"node-A", "node-B", "node-C"},
		{"node-A", "node-D", "node-C"},
	}
	for i, cp := range req.CandidatePaths {
		if len(cp.Path) != len(want[i]) {
			t.Fatalf("candidate %d = %v, want %v", i, cp.Path, want[i])
		}
		for j := range cp.Path {
			if cp.Path[j] != want[i][j] {
				t.Fatalf("candidate %d = %v, want %v", i, cp.Path, want[i])
			}
		}
		if cp.BaseCost <= 0 {
			t.Fatalf("candidate %d missing base cost: %v", i, cp)
		}
	}
	if req.State == nil || len(req.State.Peers) != 2 {
		t.Fatalf("expected 2 peer metric entries, got %+v", req.State)
	}
	peerIDs := map[string]bool{}
	for _, p := range req.State.Peers {
		peerIDs[p.PeerId] = true
	}
	if !peerIDs["node-B"] || !peerIDs["node-D"] {
		t.Fatalf("peer metrics must include direct links, got %v", peerIDs)
	}
}
