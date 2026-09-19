package test

import (
	"context"
	"testing"
	"time"

	"github.com/relaymesh/relaymesh/internal/discovery"
	"github.com/relaymesh/relaymesh/internal/node"
	"github.com/relaymesh/relaymesh/internal/routing"
	"github.com/relaymesh/relaymesh/internal/transport"
	"log/slog"
)

func TestNodeCreation(t *testing.T) {
	logger := slog.Default()
	config := &node.NodeConfig{
		ID:                "test-node",
		Address:           "0.0.0.0",
		Port:              9001,
		MaxPeers:          10,
		HeartbeatInterval: 1 * time.Second,
		PeerTimeout:       5 * time.Second,
	}

	n := node.New(config, logger)
	if n.ID() != "test-node" {
		t.Errorf("Expected node ID test-node, got %s", n.ID())
	}
}

func TestPeerDiscovery(t *testing.T) {
	logger := slog.Default()

	config1 := discovery.DiscoveryConfig{
		NodeID:       "node-1",
		Address:      "0.0.0.0",
		Port:         9201,
		Interval:     100 * time.Millisecond,
		PeerTimeout:  1 * time.Second,
		KnownPorts:   []uint16{9201, 9202},
	}

	config2 := discovery.DiscoveryConfig{
		NodeID:       "node-2",
		Address:      "0.0.0.0",
		Port:         9202,
		Interval:     100 * time.Millisecond,
		PeerTimeout:  1 * time.Second,
		KnownPorts:   []uint16{9201, 9202},
	}

	disc1 := discovery.New(config1, logger)
	disc2 := discovery.New(config2, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := disc1.Start(ctx); err != nil {
		t.Fatalf("Failed to start disc1: %v", err)
	}
	if err := disc2.Start(ctx); err != nil {
		t.Fatalf("Failed to start disc2: %v", err)
	}

	time.Sleep(1 * time.Second)

	if disc1.PeerCount() == 0 {
		t.Error("Node 1 should have discovered peers")
	}
	if disc2.PeerCount() == 0 {
		t.Error("Node 2 should have discovered peers")
	}

	disc1.Stop()
	disc2.Stop()
}

func TestDijkstraRouting(t *testing.T) {
	graph := routing.NewGraph()

	graph.AddLink(&routing.Link{
		From:       "A",
		To:         "B",
		Latency:    10,
		Bandwidth:  100,
		PacketLoss: 0,
	})

	graph.AddLink(&routing.Link{
		From:       "B",
		To:         "C",
		Latency:    20,
		Bandwidth:  100,
		PacketLoss: 0,
	})

	graph.AddLink(&routing.Link{
		From:       "A",
		To:         "C",
		Latency:    50,
		Bandwidth:  100,
		PacketLoss: 0,
	})

	dijkstra := routing.NewDijkstra(graph)
	result := dijkstra.FindPath("A", "C")

	if !result.Found {
		t.Error("Path should be found")
	}

	if len(result.Path) != 3 {
		t.Errorf("Expected path length 3, got %d", len(result.Path))
	}

	if result.Path[0] != "A" || result.Path[1] != "B" || result.Path[2] != "C" {
		t.Errorf("Expected path A->B->C, got %v", result.Path)
	}
}

func TestRouterIntegration(t *testing.T) {
	logger := slog.Default()
	router := routing.New("node-1", logger)

	router.UpdateLink("node-1", "node-2", 10, 100, 0)
	router.UpdateLink("node-2", "node-3", 20, 100, 0)
	router.UpdateLink("node-1", "node-3", 50, 100, 0)

	route, exists := router.GetRoute("node-3")
	if !exists {
		t.Error("Route to node-3 should exist")
	}

	if route.NextHop != "node-2" {
		t.Errorf("Expected next hop node-2, got %s", route.NextHop)
	}
}

func TestTransportUDP(t *testing.T) {
	transport1 := transport.NewUDP("node-1")
	transport2 := transport.NewUDP("node-2")

	err := transport1.Listen(":9011")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}

	err = transport2.Listen(":9012")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}

	defer transport1.Close()
	defer transport2.Close()

	err = transport1.Connect("node-2", "127.0.0.1:9012")
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	pkt := &transport.Packet{
		Source:      "node-1",
		Destination: "node-2",
		Payload:     []byte("hello"),
		Sequence:    1,
	}

	err = transport1.Send("node-2", pkt)
	if err != nil {
		t.Fatalf("Failed to send: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan bool, 1)
	go func() {
		received, _, err := transport2.Receive()
		if err != nil {
			t.Errorf("Failed to receive: %v", err)
			done <- false
			return
		}
		if string(received.Payload) != "hello" {
			t.Errorf("Expected payload 'hello', got '%s'", string(received.Payload))
			done <- false
			return
		}
		done <- true
	}()

	select {
	case <-ctx.Done():
		t.Error("Timeout waiting for packet")
	case success := <-done:
		if !success {
			t.Error("Failed to receive packet")
		}
	}
}
