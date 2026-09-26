package test

import (
	"testing"
	"time"

	"github.com/relaymesh/relaymesh/internal/forwarding"
	"github.com/relaymesh/relaymesh/internal/mesh"
	"github.com/relaymesh/relaymesh/internal/node"
)

// sendUntilDelivered sends payload from -> to, retrying until the
// destination delivers it or timeout expires. Retries are required
// because the mesh provides at-most-once forwarding: the first packet
// may arrive at a relay whose forwarding table has not synced yet.
// Returns the delivered packet.
func sendUntilDelivered(t *testing.T, from *mesh.MeshNode, to *mesh.MeshNode,
	dest node.NodeID, payload []byte, timeout time.Duration) forwarding.DeliveredPacket {
	t.Helper()
	deadline := time.Now().Add(timeout)
	retry := time.NewTicker(300 * time.Millisecond)
	defer retry.Stop()

	// The first attempt may fail (no route yet); retries cover it.
	_ = from.Send(dest, payload)
	for {
		select {
		case d := <-to.Delivered():
			if string(d.Payload) == string(payload) {
				return d
			}
		case <-retry.C:
			if time.Now().After(deadline) {
				t.Fatalf("no delivery of %q from %s to %s within %v (from=%+v)",
					string(payload), from.ID(), dest, timeout, from.Stats())
			}
			_ = from.Send(dest, payload)
		}
	}
}

// waitRouteTo polls until node has a next hop for dest.
func waitRouteTo(t *testing.T, n *mesh.MeshNode, dest node.NodeID, timeout time.Duration) node.NodeID {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if nh, ok := n.Router().GetNextHop(dest); ok {
			return nh
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never learned a route to %s", n.ID(), dest)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
