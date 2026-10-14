package test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/relaymesh/relaymesh/internal/identity"
	"github.com/relaymesh/relaymesh/internal/mesh"
	"github.com/relaymesh/relaymesh/internal/node"
)

// TestMeshPhase10EncryptedMultiHop proves the full encrypted mesh:
// mutual authentication between neighbors, encrypted multi-hop
// delivery (each hop is its own secure link), and encrypted probing
// feeding real RTT telemetry — all with zero handshake failures.
func TestMeshPhase10EncryptedMultiHop(t *testing.T) {
	logger := slog.Default()
	const psk = "phase10-network-key"

	newNode := func(id string, discPort, dataPort uint16, known []uint16) *mesh.MeshNode {
		ident, err := identity.Generate()
		if err != nil {
			t.Fatalf("identity for %s: %v", id, err)
		}
		return mesh.New(mesh.Config{
			NodeID:              node.NodeID(id),
			Address:             "127.0.0.1",
			DiscoveryPort:       discPort,
			DataPort:            dataPort,
			KnownDiscoveryPorts: known,
			HeartbeatInterval:   100 * time.Millisecond,
			PeerTimeout:         2 * time.Second,
			PSK:                 []byte(psk),
			Identity:            ident,
			Encrypt:             true,
		}, logger)
	}

	// Topology: A — B — C (A and C are two secure hops apart).
	nodeA := newNode("node-A", 9421, 20141, []uint16{9422})
	nodeB := newNode("node-B", 9422, 20142, []uint16{9421, 9423})
	nodeC := newNode("node-C", 9423, 20143, []uint16{9422})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, n := range []*mesh.MeshNode{nodeA, nodeB, nodeC} {
		if err := n.Start(ctx); err != nil {
			t.Fatalf("failed to start %s: %v", n.ID(), err)
		}
		defer n.Stop()
	}

	for _, n := range []*mesh.MeshNode{nodeA, nodeC} {
		if !n.SecureEnabled() {
			t.Fatalf("%s not in secure mode", n.ID())
		}
	}

	waitRouteTo(t, nodeA, "node-C", 10*time.Second)
	waitRouteTo(t, nodeB, "node-C", 10*time.Second)

	d := sendUntilDelivered(t, nodeA, nodeC, "node-C", []byte("enc-hop-1"), 10*time.Second)
	if len(d.Path) != 3 || d.Path[0] != "node-A" || d.Path[1] != "node-B" || d.Path[2] != "node-C" {
		t.Fatalf("expected A->B->C path, got %v", d.Path)
	}
	t.Logf("encrypted 2-hop delivery via %v", d.Path)

	// Every direct link must have a session; nothing should have
	// failed authentication.
	waitSecure := func(n *mesh.MeshNode, wantSessions int, who string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			s := n.SecureStats()
			if s.Sessions >= wantSessions && s.HandshakeFailures == 0 {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s secure stats wrong: %+v (want >=%d sessions)", who, s, wantSessions)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	waitSecure(nodeA, 1, "A")
	waitSecure(nodeB, 2, "B")
	waitSecure(nodeC, 1, "C")

	// Probes flow through the tunnel: real RTT measured on a link
	// whose packets never touch the wire in cleartext.
	deadline := time.Now().Add(5 * time.Second)
	for {
		ok := false
		for _, li := range nodeA.LinkStates() {
			if li.Peer == "node-B" && li.HasRTT && li.Sent > 0 && li.Recv > 0 {
				ok = true
			}
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("A never measured RTT over the encrypted link: %+v", nodeA.LinkStates())
		}
		time.Sleep(200 * time.Millisecond)
	}
	for _, n := range []*mesh.MeshNode{nodeA, nodeB, nodeC} {
		if af := n.AuthFailures(); af != 0 {
			t.Fatalf("%s recorded auth failures in a healthy secure mesh: %d", n.ID(), af)
		}
	}
	t.Logf("A-B RTT over encrypted link confirmed; all nodes auth-failure free")
}

// TestMeshPhase10PSKIsolation proves a node with the wrong network
// PSK cannot complete the handshake and never delivers traffic, even
// though plaintext discovery still lets it see the mesh.
func TestMeshPhase10PSKIsolation(t *testing.T) {
	logger := slog.Default()

	newNode := func(id string, discPort, dataPort uint16, known []uint16, key string) *mesh.MeshNode {
		ident, err := identity.Generate()
		if err != nil {
			t.Fatalf("identity for %s: %v", id, err)
		}
		return mesh.New(mesh.Config{
			NodeID:              node.NodeID(id),
			Address:             "127.0.0.1",
			DiscoveryPort:       discPort,
			DataPort:            dataPort,
			KnownDiscoveryPorts: known,
			HeartbeatInterval:   100 * time.Millisecond,
			PeerTimeout:         2 * time.Second,
			PSK:                 []byte(key),
			Identity:            ident,
			Encrypt:             true,
		}, logger)
	}

	nodeA := newNode("node-A", 9431, 20151, []uint16{9432}, "right-key")
	nodeB := newNode("node-B", 9432, 20152, []uint16{9431}, "wrong-key")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, n := range []*mesh.MeshNode{nodeA, nodeB} {
		if err := n.Start(ctx); err != nil {
			t.Fatalf("failed to start %s: %v", n.ID(), err)
		}
		defer n.Stop()
	}

	// Discovery heartbeats are plaintext: both sides see each other
	// and install routes — the data plane is what must refuse.
	deadline := time.Now().Add(10 * time.Second)
	for nodeA.Stats().PeerCount < 1 || nodeB.Stats().PeerCount < 1 {
		if time.Now().After(deadline) {
			t.Fatalf("discovery failed: A=%d B=%d", nodeA.Stats().PeerCount, nodeB.Stats().PeerCount)
		}
		time.Sleep(100 * time.Millisecond)
	}
	waitRouteTo(t, nodeA, "node-B", 10*time.Second)

	// A attempts delivery; the handshake must fail on both sides.
	for i := 0; i < 3; i++ {
		_ = nodeA.Send("node-B", []byte("evil"))
		time.Sleep(300 * time.Millisecond)
	}

	timeout := time.After(3 * time.Second)
drain:
	for {
		select {
		case d := <-nodeB.Delivered():
			t.Fatalf("wrong-PSK encrypted packet delivered: %s %q", d.Source, d.Payload)
		case <-timeout:
			break drain
		}
	}

	aStats, bStats := nodeA.SecureStats(), nodeB.SecureStats()
	if aStats.Sessions != 0 || bStats.Sessions != 0 {
		t.Fatalf("sessions established despite PSK mismatch: A=%+v B=%+v", aStats, bStats)
	}
	if bStats.HandshakeFailures == 0 {
		t.Fatalf("B never rejected the handshake: %+v", bStats)
	}
	t.Logf("wrong-PSK node isolated: B rejected %d handshakes, 0 sessions",
		bStats.HandshakeFailures)
}
