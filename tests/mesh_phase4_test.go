package test

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/relaymesh/relaymesh/internal/mesh"
	"github.com/relaymesh/relaymesh/internal/node"
)

func phase4Diamond(t *testing.T, baseDisc, baseData uint16) (ctx context.Context, cancel context.CancelFunc, nodes map[string]*mesh.MeshNode) {
	t.Helper()
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

	//        B
	//       / \
	//      A   D
	//       \ /
	//        C
	nodes = map[string]*mesh.MeshNode{
		"A": newNode("node-A", baseDisc, baseData, []uint16{baseDisc + 1, baseDisc + 2}),
		"B": newNode("node-B", baseDisc+1, baseData+1, []uint16{baseDisc, baseDisc + 3}),
		"C": newNode("node-C", baseDisc+2, baseData+2, []uint16{baseDisc, baseDisc + 3}),
		"D": newNode("node-D", baseDisc+3, baseData+3, []uint16{baseDisc + 1, baseDisc + 2}),
	}

	ctx, cancel = context.WithTimeout(context.Background(), 60*time.Second)
	for _, n := range nodes {
		if err := n.Start(ctx); err != nil {
			cancel()
			t.Fatalf("failed to start %s: %v", n.ID(), err)
		}
	}
	return ctx, cancel, nodes
}

func waitNextHop(t *testing.T, from *mesh.MeshNode, dest node.NodeID, timeout time.Duration) node.NodeID {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if nh, ok := from.Router().GetNextHop(dest); ok {
			return nh
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never learned a route to %s", from.ID(), dest)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestMeshPhase4SustainedFailover keeps traffic flowing A -> D while the
// active relay is killed mid-stream. Delivery must resume on the
// surviving branch with no manual intervention.
func TestMeshPhase4SustainedFailover(t *testing.T) {
	_, cancel, nodes := phase4Diamond(t, 9381, 19831)
	defer cancel()
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	nodeA, nodeD := nodes["A"], nodes["D"]

	nh := waitNextHop(t, nodeA, "node-D", 10*time.Second)
	route, ok := nodeA.Router().GetRoute("node-D")
	if !ok {
		t.Fatalf("route to node-D vanished mid-test")
	}
	if route.HopCount != 2 {
		t.Fatalf("expected 2-hop route, got %v", route.Path)
	}
	victim := string(route.Path[1])
	t.Logf("active path A -> D via %s", victim)
	_ = nh

	var delivered atomic.Uint64
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			select {
			case <-done:
				return
			case d := <-nodeD.Delivered():
				_ = d
				delivered.Add(1)
			}
		}
	}()

	stopSend := make(chan struct{})
	defer close(stopSend)
	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		i := 0
		for {
			select {
			case <-stopSend:
				return
			case <-ticker.C:
				i++
				_ = nodeA.Send("node-D", []byte(fmt.Sprintf("stream-%d", i)))
			}
		}
	}()

	// Let steady flow establish, then kill the active relay mid-stream.
	time.Sleep(1500 * time.Millisecond)
	before := delivered.Load()
	if before == 0 {
		t.Fatalf("no deliveries before kill (A=%+v D=%+v)", nodeA.Stats(), nodeD.Stats())
	}
	t.Logf("steady flow: %d delivered, killing %s", before, victim)
	if victim == "node-B" {
		nodes["B"].Stop()
	} else {
		nodes["C"].Stop()
	}
	killAt := time.Now()

	// Delivery must resume on the surviving branch.
	deadline := time.Now().Add(12 * time.Second)
	resumed := false
	for {
		if delivered.Load() > before {
			resumed = true
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !resumed {
		t.Fatalf("traffic never resumed after killing %s (delivered stuck at %d)",
			victim, before)
	}
	t.Logf("traffic resumed %v after kill (total delivered=%d)",
		time.Since(killAt).Round(100*time.Millisecond), delivered.Load())

	// The new route must avoid the dead node.
	if nh, ok := nodeA.Router().GetNextHop("node-D"); !ok || string(nh) == victim {
		t.Fatalf("route still uses dead node %s (nextHop=%s)", victim, nh)
	}
}

// TestMeshPhase4LinkDownUp breaks only the data-plane link to the active
// relay while every node stays alive, then restores it. Routing must
// move away and (once cleared) be able to use the link again.
func TestMeshPhase4LinkDownUp(t *testing.T) {
	_, cancel, nodes := phase4Diamond(t, 9391, 19931)
	defer cancel()
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	nodeA, nodeD := nodes["A"], nodes["D"]

	waitNextHop(t, nodeA, "node-D", 10*time.Second)
	route, ok := nodeA.Router().GetRoute("node-D")
	if !ok {
		t.Fatalf("route to node-D vanished mid-test")
	}
	active := route.Path[1]
	t.Logf("active path A -> D via %s", active)

	// Break the data link administratively; the node stays alive.
	nodeA.SetLinkDown(active, true)

	deadline := time.Now().Add(12 * time.Second)
	var newNH node.NodeID
	for {
		if nh, ok := nodeA.Router().GetNextHop("node-D"); ok && nh != active {
			newNH = nh
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("A never routed around down link to %s", active)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Logf("rerouted around %s via %s", active, newNH)

	// Traffic must flow on the surviving branch.
	d := sendUntilDelivered(t, nodeA, nodeD, "node-D", []byte("link-down-1"), 8*time.Second)
	for _, n := range d.Path {
		if n == active {
			t.Fatalf("path traverses down link %s: %v", active, d.Path)
		}
	}
	t.Logf("delivered around down link via %v", d.Path)

	// Restore the link; the route must become available again.
	nodeA.SetLinkDown(active, false)
	waitNextHop(t, nodeA, "node-D", 10*time.Second)
	d2 := sendUntilDelivered(t, nodeA, nodeD, "node-D", []byte("link-up-1"), 8*time.Second)
	t.Logf("delivered after link restore via %v", d2.Path)
}
