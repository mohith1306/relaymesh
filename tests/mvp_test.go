package test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/relaymesh/relaymesh/internal/discovery"
	"github.com/relaymesh/relaymesh/internal/node"
	"github.com/relaymesh/relaymesh/internal/routing"
)

func TestMVPMeshNetwork(t *testing.T) {
	logger := slog.Default()

	nodes := make([]*node.Node, 3)
	discoveries := make([]*discovery.Discovery, 3)
	routers := make([]*routing.Router, 3)

	for i := 0; i < 3; i++ {
		nodeID := node.NodeID("node-" + string(rune('A'+i)))
		port := uint16(9001 + i)

		config := &node.NodeConfig{
			ID:                nodeID,
			Address:           "127.0.0.1",
			Port:              port,
			MaxPeers:          10,
			HeartbeatInterval: 100 * time.Millisecond,
			PeerTimeout:       1 * time.Second,
		}

		nodes[i] = node.New(config, logger)

		discConfig := discovery.DiscoveryConfig{
			NodeID:      nodeID,
			Address:     "127.0.0.1",
			Port:        port,
			Interval:    100 * time.Millisecond,
			PeerTimeout: 1 * time.Second,
			KnownPorts:  []uint16{9001, 9002, 9003},
		}
		discoveries[i] = discovery.New(discConfig, logger)
		routers[i] = routing.New(nodeID, logger)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 3; i++ {
		discoveries[i].Start(ctx)
	}

	time.Sleep(500 * time.Millisecond)

	// Add links bidirectionally on all routers
	for i := 0; i < 3; i++ {
		routers[i].UpdateLink("node-A", "node-B", 10, 100, 0)
		routers[i].UpdateLink("node-B", "node-A", 10, 100, 0)
		routers[i].UpdateLink("node-B", "node-C", 20, 100, 0)
		routers[i].UpdateLink("node-C", "node-B", 20, 100, 0)
	}

	time.Sleep(200 * time.Millisecond)

	route, exists := routers[0].GetRoute("node-C")
	if !exists {
		t.Fatal("Route from A to C should exist")
	}

	t.Logf("Route A -> C: %v (cost: %.2f)", route.Path, route.Cost)

	if route.NextHop != "node-B" {
		t.Errorf("Expected next hop node-B, got %s", route.NextHop)
	}

	for i := 0; i < 3; i++ {
		discoveries[i].Stop()
	}

	t.Log("MVP mesh network test completed successfully")
}
