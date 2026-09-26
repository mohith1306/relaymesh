package test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/relaymesh/relaymesh/internal/mesh"
	"github.com/relaymesh/relaymesh/internal/node"
	"github.com/relaymesh/relaymesh/internal/transport"
)

// TestTransportPSK proves packet authentication at the transport layer:
// same-PSK peers deliver, wrong-PSK traffic is dropped before the
// forwarder ever sees it, and failures are counted.
func TestTransportPSK(t *testing.T) {
	t1 := transport.NewUDP("node-1")
	t2 := transport.NewUDP("node-2")
	t1.SetPSK([]byte("shared-secret"))
	t2.SetPSK([]byte("shared-secret"))

	if err := t1.Listen("127.0.0.1:20211"); err != nil {
		t.Fatalf("listen t1: %v", err)
	}
	defer t1.Close()
	if err := t2.Listen("127.0.0.1:20212"); err != nil {
		t.Fatalf("listen t2: %v", err)
	}
	defer t2.Close()

	if err := t1.Connect("node-2", "127.0.0.1:20212"); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := t2.Connect("node-1", "127.0.0.1:20211"); err != nil {
		t.Fatalf("connect: %v", err)
	}

	if err := t1.Send("node-2", &transport.Packet{
		Source: "node-1", Destination: "node-2",
		Payload: []byte("secret-hello"), ID: "p1", TTL: 64,
	}); err != nil {
		t.Fatalf("send failed: %v", err)
	}
	select {
	case <-time.After(2 * time.Second):
		t.Fatalf("same-PSK packet never delivered")
	case r := <-waitTransport(t2):
		if string(r.Payload) != "secret-hello" {
			t.Fatalf("wrong payload: %q", string(r.Payload))
		}
	}

	// Attacker with a different key: every packet must die at ingress.
	t3 := transport.NewUDP("node-3")
	t3.SetPSK([]byte("wrong-key"))
	if err := t3.Listen("127.0.0.1:20213"); err != nil {
		t.Fatalf("listen t3: %v", err)
	}
	defer t3.Close()
	if err := t3.Connect("node-2", "127.0.0.1:20212"); err != nil {
		t.Fatalf("connect t3: %v", err)
	}
	before := t2.AuthFailures()
	for i := 0; i < 3; i++ {
		_ = t3.Send("node-2", &transport.Packet{
			Source: "node-3", Destination: "node-2",
			Payload: []byte("evil"), ID: "evil", TTL: 64,
		})
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
	if got := t2.AuthFailures(); got <= before {
		t.Fatalf("expected auth failures to increase, before=%d after=%d", before, got)
	}
	t.Logf("wrong-PSK packets dropped at ingress (auth failures=%d)", t2.AuthFailures())
}

type transportResult struct {
	Payload []byte
}

func waitTransport(tr *transport.UDPTransport) <-chan transportResult {
	ch := make(chan transportResult, 1)
	go func() {
		pkt, _, _ := tr.Receive()
		if pkt != nil {
			ch <- transportResult{Payload: pkt.Payload}
		}
	}()
	return ch
}

// TestMeshPhase9Isolation proves a wrong-PSK node cannot participate in
// the mesh: neither direction delivers, while correct-PSK peers are
// unaffected on the same links.
func TestMeshPhase9Isolation(t *testing.T) {
	logger := slog.Default()
	const psk = "correct-horse-battery"

	newNode := func(id string, discPort, dataPort uint16, known []uint16, key string) *mesh.MeshNode {
		return mesh.New(mesh.Config{
			NodeID:              node.NodeID(id),
			Address:             "127.0.0.1",
			DiscoveryPort:       discPort,
			DataPort:            dataPort,
			KnownDiscoveryPorts: known,
			HeartbeatInterval:   100 * time.Millisecond,
			PeerTimeout:         2 * time.Second,
			PSK:                 []byte(key),
		}, logger)
	}

	nodeA := newNode("node-A", 9411, 20131, []uint16{9412}, psk)
	nodeB := newNode("node-B", 9412, 20132, []uint16{9411, 9413}, psk)
	nodeC := newNode("node-C", 9413, 20133, []uint16{9412}, "wrong-key")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, n := range []*mesh.MeshNode{nodeA, nodeB, nodeC} {
		if err := n.Start(ctx); err != nil {
			t.Fatalf("failed to start %s: %v", n.ID(), err)
		}
		defer n.Stop()
	}

	// Wait until B sees both peers (discovery is unauthenticated).
	deadline := time.Now().Add(10 * time.Second)
	for {
		if nodeB.Stats().PeerCount >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("B never discovered both peers")
		}
		time.Sleep(100 * time.Millisecond)
	}

	// C attempts to send into the mesh; B must never deliver it.
	for i := 0; i < 3; i++ {
		_ = nodeC.Send("node-B", []byte("evil"))
		time.Sleep(200 * time.Millisecond)
	}
	timeout := time.After(2 * time.Second)
	caughtFromC := false
drain:
	for {
		select {
		case d := <-nodeB.Delivered():
			if d.Source == "node-C" {
				caughtFromC = true
				break drain
			}
		case <-timeout:
			break drain
		}
	}
	if caughtFromC {
		t.Fatalf("wrong-PSK packet from C was delivered by B")
	}
	if nodeB.AuthFailures() == 0 {
		t.Fatalf("expected B to record auth failures from C")
	}
	t.Logf("C isolated: B dropped %d unauthenticated packets", nodeB.AuthFailures())

	// Correct-PSK peers are unaffected on the same node.
	waitRouteTo(t, nodeA, "node-B", 10*time.Second)
	if err := nodeA.Send("node-B", []byte("good")); err != nil {
		t.Fatalf("A send failed: %v", err)
	}
	select {
	case d := <-nodeB.Delivered():
		if d.Source != "node-A" || string(d.Payload) != "good" {
			t.Fatalf("wrong delivery: %s %q", d.Source, string(d.Payload))
		}
		t.Logf("A -> B delivered despite attacker's presence")
	case <-time.After(5 * time.Second):
		t.Fatalf("A -> B not delivered (A=%+v B=%+v)", nodeA.Stats(), nodeB.Stats())
	}
}
