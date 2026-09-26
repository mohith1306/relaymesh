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
	route, _ := nodeA.Router().GetRoute("node-D")
	if route.HopCount != 2 {
		t.Fatalf("expected 2-hop route A -> D, got %d hops via %s (path=%v)", route.HopCount, nh, route.Path)
	}
	t.Logf("initial route A -> D: %v", route.Path)

	// Deliver end-to-end over the chosen path.
	if err := nodeA.Send("node-D", []byte("diamond-1")); err != nil {
		t.Fatalf("A failed to send to D: %v", err)
	}
	select {
	case d := <-nodeD.Delivered():
		if string(d.Payload) != "diamond-1" {
			t.Fatalf("D received wrong payload: %q", string(d.Payload))
		}
		if len(d.Path) != 3 {
			t.Fatalf("expected 3-node delivery path, got %v", d.Path)
		}
		t.Logf("delivered A -> D via path %v", d.Path)
	case <-time.After(5 * time.Second):
		t.Fatalf("D never received packet from A (A=%+v B=%+v C=%+v D=%+v)",
			nodeA.Stats(), nodeB.Stats(), nodeC.Stats(), nodeD.Stats())
	}

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

	if err := nodeA.Send("node-D", []byte("diamond-2")); err != nil {
		t.Fatalf("A failed to send to D after failover: %v", err)
	}
	select {
	case d := <-nodeD.Delivered():
		if string(d.Payload) != "diamond-2" {
			t.Fatalf("D received wrong payload after failover: %q", string(d.Payload))
		}
		for _, n := range d.Path {
			if n == victim {
				t.Fatalf("failover path still traverses dead node %s: %v", victim, d.Path)
			}
		}
		t.Logf("delivered A -> D after failover via path %v", d.Path)
	case <-time.After(5 * time.Second):
		t.Fatalf("D never received packet after failover (A=%+v C=%+v D=%+v)",
			nodeA.Stats(), nodeC.Stats(), nodeD.Stats())
	}
}
