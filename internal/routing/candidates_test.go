package routing

import (
	"log/slog"
	"testing"

	"github.com/relaymesh/relaymesh/internal/node"
)

// testDiamond builds A—B—D and A—C—D (two loop-free paths A→D).
func testDiamond(t *testing.T) *Router {
	t.Helper()
	r := New("node-A", slog.Default())
	r.UpdateLink("node-A", "node-B", 2, 100, 0)
	r.UpdateLink("node-B", "node-D", 10, 100, 0)
	r.UpdateLink("node-A", "node-C", 5, 100, 0)
	r.UpdateLink("node-C", "node-D", 10, 100, 0)
	return r
}

// Candidates must enumerate both loop-free paths, cheapest first.
func TestCandidatesTwoPathDiamond(t *testing.T) {
	r := testDiamond(t)

	cands := r.Candidates("node-D", 4)
	if len(cands) != 2 {
		t.Fatalf("expected 2 candidate paths, got %d: %v", len(cands), cands)
	}
	want := [][]node.NodeID{
		{"node-A", "node-B", "node-D"},
		{"node-A", "node-C", "node-D"},
	}
	for i, c := range cands {
		if len(c.Path) != len(want[i]) {
			t.Fatalf("candidate %d path %v, want %v", i, c.Path, want[i])
		}
		for j := range c.Path {
			if c.Path[j] != want[i][j] {
				t.Fatalf("candidate %d path %v, want %v", i, c.Path, want[i])
			}
		}
	}
	if !(cands[0].Cost < cands[1].Cost) {
		t.Fatalf("expected cheapest first, costs %v then %v", cands[0].Cost, cands[1].Cost)
	}
}

// Max must cap the returned set.
func TestCandidatesRespectsMax(t *testing.T) {
	r := testDiamond(t)
	if cands := r.Candidates("node-D", 1); len(cands) != 1 {
		t.Fatalf("expected 1 candidate with max=1, got %d", len(cands))
	}
}

// Unreachable and self destinations must yield no candidates.
func TestCandidatesUnreachableAndSelf(t *testing.T) {
	r := testDiamond(t)
	if cands := r.Candidates("node-Z", 4); len(cands) != 0 {
		t.Fatalf("expected no candidates for unreachable dest, got %v", cands)
	}
	if cands := r.Candidates("node-A", 4); len(cands) != 0 {
		t.Fatalf("expected no candidates for self, got %v", cands)
	}
}

// Every candidate hop must be a real graph link (safety validator check).
func TestCandidatesAllHopsLinked(t *testing.T) {
	r := testDiamond(t)
	for _, c := range r.Candidates("node-D", 4) {
		for i := 0; i < len(c.Path)-1; i++ {
			if _, ok := r.GetGraph().GetLink(c.Path[i], c.Path[i+1]); !ok {
				t.Fatalf("candidate %v uses missing link %s-%s", c.Path, c.Path[i], c.Path[i+1])
			}
		}
	}
}
