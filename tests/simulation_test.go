package test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/relaymesh/relaymesh/internal/discovery"
	"github.com/relaymesh/relaymesh/internal/node"
	"github.com/relaymesh/relaymesh/internal/routing"
	"github.com/relaymesh/relaymesh/internal/telemetry"
)

type SimNode struct {
	Node      *node.Node
	Discovery *discovery.Discovery
	Router    *routing.Router
	Telemetry *telemetry.MetricsCollector
}

func TestSimulationMeshTopology(t *testing.T) {
	logger := slog.Default()

	// Topology:
	//   [Laptop] -> [A] -> [B] -> [C] -> [Gateway] -> [Internet]
	//                  \-> [D] -/

	nodes := make(map[string]*SimNode)
	nodeIDs := []string{"A", "B", "C", "D", "Gateway"}

	for i, id := range nodeIDs {
		port := uint16(9100 + i)
		nodeID := node.NodeID("node-" + id)

		cfg := &node.NodeConfig{
			ID:                nodeID,
			Address:           "127.0.0.1",
			Port:              port,
			MaxPeers:          10,
			HeartbeatInterval: 100 * time.Millisecond,
			PeerTimeout:       1 * time.Second,
		}

		discCfg := discovery.DiscoveryConfig{
			NodeID:      nodeID,
			Address:     "127.0.0.1",
			Port:        port,
			Interval:    100 * time.Millisecond,
			PeerTimeout: 1 * time.Second,
			KnownPorts:  []uint16{9100, 9101, 9102, 9103, 9104},
		}

		nodes[id] = &SimNode{
			Node:      node.New(cfg, logger),
			Discovery: discovery.New(discCfg, logger),
			Router:    routing.New(nodeID, logger),
			Telemetry: telemetry.NewCollector(nodeID),
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, sim := range nodes {
		sim.Discovery.Start(ctx)
	}
	time.Sleep(500 * time.Millisecond)

	// Build links
	addLink(nodes, "A", "B", 10, 100, 0.01)
	addLink(nodes, "B", "C", 20, 100, 0.02)
	addLink(nodes, "C", "Gateway", 5, 100, 0.001)
	addLink(nodes, "A", "D", 15, 100, 0.01)
	addLink(nodes, "D", "C", 15, 100, 0.01)

	// Exchange routing tables (simulate routing protocol)
	for i := 0; i < 3; i++ {
		for _, sim := range nodes {
			routes := sim.Router.GetRoutingUpdate()
			for _, other := range nodes {
				if other != sim {
					other.Router.HandleRoutingUpdate(sim.Node.ID(), routes)
				}
			}
		}
	}

	time.Sleep(200 * time.Millisecond)

	// Scenario 1: Normal routing
	fmt.Println("\n========================================")
	fmt.Println("SCENARIO 1: Normal Routing A -> Gateway")
	fmt.Println("========================================")

	for id, sim := range nodes {
		routes := sim.Router.GetTable().GetAllDestinations()
		fmt.Printf("Node %s knows about destinations: %v\n", id, routes)
	}

	route, exists := nodes["A"].Router.GetRoute("node-Gateway")
	if exists {
		fmt.Printf("Route A -> Gateway: %v\n", route.Path)
		fmt.Printf("Cost: %.2f, Hops: %d\n", route.Cost, route.HopCount)
		fmt.Printf("Next Hop: %s\n", route.NextHop)
	} else {
		fmt.Println("No direct route found, checking all routes from A:")
		for _, dest := range nodes["A"].Router.GetTable().GetAllDestinations() {
			r, ok := nodes["A"].Router.GetRoute(dest)
			if ok {
				fmt.Printf("  -> %s via %s (cost: %.2f)\n", dest, r.NextHop, r.Cost)
			}
		}
	}

	// Scenario 2: Failover - remove B
	fmt.Println("\n========================================")
	fmt.Println("SCENARIO 2: Node B Fails - Failover")
	fmt.Println("========================================")

	nodes["A"].Router.RemoveLink("node-A", "node-B")
	nodes["B"].Router.RemoveLink("node-B", "node-A")
	nodes["C"].Router.RemoveLink("node-C", "node-B")
	nodes["B"].Router.RemoveLink("node-B", "node-C")

	route2, exists2 := nodes["A"].Router.GetRoute("node-Gateway")
	if exists2 {
		fmt.Printf("New Route A -> Gateway: %v\n", route2.Path)
		fmt.Printf("Cost: %.2f, Hops: %d\n", route2.Cost, route2.HopCount)
	}

	// Scenario 3: Telemetry
	fmt.Println("\n========================================")
	fmt.Println("SCENARIO 3: Telemetry Collection")
	fmt.Println("========================================")

	nodes["A"].Telemetry.UpdateLatency(15.5)
	nodes["A"].Telemetry.UpdatePacketLoss(0.02)
	nodes["A"].Telemetry.UpdateBandwidth(95.0)
	nodes["A"].Telemetry.UpdateJitter(2.3)
	nodes["A"].Telemetry.UpdateCongestion(0.1)

	features := nodes["A"].Telemetry.ToFeatureVector()
	fmt.Printf("Node A feature vector: %v\n", features)

	snapshot := nodes["A"].Telemetry.Snapshot()
	fmt.Printf("Snapshot - Latency: %.2f ms, Loss: %.2f%%, BW: %.2f Mbps\n",
		snapshot.Metrics.Latency,
		snapshot.Metrics.PacketLoss*100,
		snapshot.Metrics.Bandwidth,
	)

	for _, sim := range nodes {
		sim.Discovery.Stop()
	}

	fmt.Println("\n========================================")
	fmt.Println("SIMULATION COMPLETE")
	fmt.Println("========================================")
}

func addLink(nodes map[string]*SimNode, from, to string, latency, bw, loss float64) {
	fromID := node.NodeID("node-" + from)
	toID := node.NodeID("node-" + to)
	nodes[from].Router.UpdateLink(fromID, toID, latency, bw, loss)
	nodes[to].Router.UpdateLink(toID, fromID, latency, bw, loss)
}
