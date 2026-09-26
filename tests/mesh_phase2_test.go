package test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/relaymesh/relaymesh/internal/mesh"
	"github.com/relaymesh/relaymesh/internal/node"
)

// TestMeshPhase2Diamond proves multi-path routing and failover:
//
//        B
//       / \
//      A   D
//       \ /
//        C
//
// A only hears B and C; D only hears B and C. A learns both B-D and
// C-D links from neighbor heartbeats, picks a 2-hop route to D, and
// after B dies it fails over to A -> C -> D with no static config.
func TestMeshPhase2Diamond(t *testing.T) {
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

	nodeA := newNode("node-A", 9351, 19531, []uint16{9352, 9353})
	nodeB := newNode("node-B", 9352, 19532, []uint16{9351, 9354})
	nodeC := newNode("node-C", 9353, 19533, []uint16{9351, 9354})
	nodeD := newNode("node-D", 9354, 19534, []uint16{9352, 9353})

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	nodes := []*mesh.MeshNode{nodeA, nodeB, nodeC, nodeD}
	for _, n := range nodes {
		if err := n.Start(ctx); err != nil {
			t.Fatalf("failed to start %s: %v", n.ID(), err)
		}
		defer n.Stop()
	}

	waitRoute := func(from *mesh.MeshNode, dest node.NodeID, timeout time.Duration) node.NodeID {
		deadline := time.Now().Add(timeout)
		for {
			if nh, ok := from.Router().GetNextHop(dest); ok {
				return nh
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s never learned a route to %s (peers=%d routes=%v)",
					from.ID(), dest, from.Stats().PeerCount,
					from.Router().GetTable().GetAllDestinations())
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	// A must find a 2-hop route to D through either B or C.
	nh := waitRoute(nodeA, "node-D", 10*time.Second)
	route, ok := nodeA.Router().GetRoute("node-D")
	if !ok {
		t.Fatalf("route to node-D vanished mid-test")
	}
	if route.HopCount != 2 {
		t.Fatalf("expected 2-hop route A -> D, got %d hops via %s (path=%v)", route.HopCount, nh, route.Path)
	}
	t.Logf("initial route A -> D: %v", route.Path)

	// The relay needs its own route before it can forward.
	if route.Path[1] == "node-B" {
		waitRouteTo(t, nodeB, "node-D", 10*time.Second)
	} else {
		waitRouteTo(t, nodeC, "node-D", 10*time.Second)
	}

	// Deliver end-to-end over the chosen path (retries cover the
	// window where the relay's forwarding table has not synced yet).
	d := sendUntilDelivered(t, nodeA, nodeD, "node-D", []byte("diamond-1"), 8*time.Second)
	if len(d.Path) != 3 {
		t.Fatalf("expected 3-node delivery path, got %v", d.Path)
	}
	t.Logf("delivered A -> D via path %v", d.Path)

	// Kill the middle node on the active path and verify failover
	// to the surviving branch.
	victim := route.Path[1]
	t.Logf("killing intermediate node %s", victim)
	if victim == "node-B" {
		_ = nodeB.Stop()
	} else {
		_ = nodeC.Stop()
	}

	deadline := time.Now().Add(12 * time.Second)
	var newNH node.NodeID
	for {
		if nh, ok := nodeA.Router().GetNextHop("node-D"); ok && nh != victim {
			newNH = nh
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("A never failed over from %s (route=%v)", victim,
				nodeA.Router().GetTable().GetAllDestinations())
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Logf("failover route A -> D via %s", newNH)

	// The surviving relay needs a synced route before forwarding.
	if newNH == "node-B" {
		waitRouteTo(t, nodeB, "node-D", 10*time.Second)
	} else {
		waitRouteTo(t, nodeC, "node-D", 10*time.Second)
	}

	d2 := sendUntilDelivered(t, nodeA, nodeD, "node-D", []byte("diamond-2"), 8*time.Second)
	for _, n := range d2.Path {
		if n == victim {
			t.Fatalf("failover path still traverses dead node %s: %v", victim, d2.Path)
		}
	}
	t.Logf("delivered A -> D after failover via path %v", d2.Path)
}
