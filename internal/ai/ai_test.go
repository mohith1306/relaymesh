package ai

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/relaymesh/relaymesh/api/proto"
	"github.com/relaymesh/relaymesh/internal/node"
	"github.com/relaymesh/relaymesh/internal/routing"
	"github.com/relaymesh/relaymesh/internal/telemetry"
	"google.golang.org/grpc"
)

type mockAIServer struct {
	pb.UnimplementedAIServiceServer
	mu    sync.Mutex
	calls int
	rec   *pb.RouteRecommendation
	err   error
}

func (m *mockAIServer) GetRecommendation(_ context.Context, _ *pb.InferenceRequest) (*pb.InferenceResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.err != nil {
		return nil, m.err
	}
	return &pb.InferenceResponse{Recommendations: []*pb.RouteRecommendation{m.rec}}, nil
}

func (m *mockAIServer) ReportFeedback(_ context.Context, _ *pb.TrainingFeedback) (*pb.Ack, error) {
	return &pb.Ack{Success: true}, nil
}

func (m *mockAIServer) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func startMockAI(t *testing.T, mock *mockAIServer) (addr string, stop func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterAIServiceServer(srv, mock)
	go srv.Serve(lis)
	return lis.Addr().String(), srv.Stop
}

func testRouter(t *testing.T) *routing.Router {
	t.Helper()
	r := routing.New("node-A", slog.Default())
	r.UpdateLink("node-A", "node-B", 10, 100, 0)
	r.UpdateLink("node-B", "node-C", 20, 100, 0)
	return r
}

func testCollectors() map[node.NodeID]*telemetry.MetricsCollector {
	return map[node.NodeID]*telemetry.MetricsCollector{
		"node-A": telemetry.NewCollector("node-A"),
	}
}

// Valid AI recommendation must be used.
func TestAIAdviseValidRoute(t *testing.T) {
	mock := &mockAIServer{rec: &pb.RouteRecommendation{
		Source: "node-A", Destination: "node-C",
		Path:       []string{"node-A", "node-B", "node-C"},
		Confidence: 0.8, Score: 25.0,
	}}
	addr, stop := startMockAI(t, mock)
	defer stop()

	client := NewClient(addr, slog.Default())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client.Close()

	ar := NewAIRouter("node-A", testRouter(t), client, slog.Default())
	ar.SetAIAvailable(true)
	ar.SetCollector("node-A", testCollectors()["node-A"])
	ar.SetPeers([]node.NodeID{"node-A", "node-B", "node-C"})

	nh, ok := ar.AdviseRoute(ctx, "node-C")
	if !ok {
		t.Fatalf("expected AI advice, got fallback-miss")
	}
	if nh != "node-B" {
		t.Fatalf("expected next hop node-B, got %s", nh)
	}
	if mock.callCount() != 1 {
		t.Fatalf("expected 1 AI call, got %d", mock.callCount())
	}
}

// Invalid AI path must fall back to deterministic routing.
func TestAIAdviseInvalidFallsBack(t *testing.T) {
	mock := &mockAIServer{rec: &pb.RouteRecommendation{
		Source: "node-A", Destination: "node-C",
		Path:       []string{"node-A", "node-X", "node-C"},
		Confidence: 0.9, Score: 1.0,
	}}
	addr, stop := startMockAI(t, mock)
	defer stop()

	client := NewClient(addr, slog.Default())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client.Close()

	ar := NewAIRouter("node-A", testRouter(t), client, slog.Default())
	ar.SetAIAvailable(true)
	ar.SetCollector("node-A", testCollectors()["node-A"])
	ar.SetPeers([]node.NodeID{"node-A", "node-B", "node-C"})

	nh, ok := ar.AdviseRoute(ctx, "node-C")
	if !ok {
		t.Fatalf("expected deterministic fallback, got miss")
	}
	if nh != "node-B" {
		t.Fatalf("expected fallback next hop node-B, got %s", nh)
	}
}

// AI service errors must fall back to deterministic routing.
func TestAIAdviseErrorFallsBack(t *testing.T) {
	mock := &mockAIServer{err: context.DeadlineExceeded}
	addr, stop := startMockAI(t, mock)
	defer stop()

	client := NewClient(addr, slog.Default())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client.Close()

	ar := NewAIRouter("node-A", testRouter(t), client, slog.Default())
	ar.SetAIAvailable(true)
	ar.SetCollector("node-A", testCollectors()["node-A"])
	ar.SetPeers([]node.NodeID{"node-A", "node-B", "node-C"})

	nh, ok := ar.AdviseRoute(ctx, "node-C")
	if !ok || nh != "node-B" {
		t.Fatalf("expected deterministic fallback to node-B, got %s %v", nh, ok)
	}
}

// AI disabled must never call the service.
func TestAIAdviseDisabledNoRPC(t *testing.T) {
	mock := &mockAIServer{rec: &pb.RouteRecommendation{
		Source: "node-A", Destination: "node-C",
		Path:       []string{"node-A", "node-B", "node-C"},
		Confidence: 0.9, Score: 1.0,
	}}
	addr, stop := startMockAI(t, mock)
	defer stop()

	client := NewClient(addr, slog.Default())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	defer client.Close()

	ar := NewAIRouter("node-A", testRouter(t), client, slog.Default())
	ar.SetCollector("node-A", testCollectors()["node-A"])
	ar.SetPeers([]node.NodeID{"node-A", "node-B", "node-C"})

	nh, ok := ar.AdviseRoute(ctx, "node-C")
	if !ok || nh != "node-B" {
		t.Fatalf("expected deterministic route to node-B, got %s %v", nh, ok)
	}
	if mock.callCount() != 0 {
		t.Fatalf("expected no AI calls when disabled, got %d", mock.callCount())
	}
}
