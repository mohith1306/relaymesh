package routing

import (
	"sort"

	"github.com/relaymesh/relaymesh/internal/node"
)

// CandidatePath is a loop-free path from the router's own node to a
// destination with its deterministic (loss-blind) cost. Candidates are
// the options Go hands the AI layer: Go keeps topology knowledge, the
// AI only scores paths it is given.
type CandidatePath struct {
	Path []node.NodeID
	Cost float64
}

const (
	// maxCandidateHops bounds enumeration so the AI is never shown
	// chains longer than the safety validator's max hop count.
	maxCandidateHops = 6
	// candidateWalkBudget bounds the DFS on dense graphs.
	candidateWalkBudget = 4000
)

// Candidates enumerates loop-free paths from this router's node to
// destination over the current graph, cheapest first, capped at max
// (4 when max <= 0). Returns nil when destination is unreachable.
func (r *Router) Candidates(destination node.NodeID, max int) []CandidatePath {
	if destination == r.nodeID {
		return nil
	}
	if max <= 0 {
		max = 4
	}

	g := r.graph
	var out []CandidatePath
	visited := map[node.NodeID]bool{r.nodeID: true}
	budget := candidateWalkBudget

	seen := make(map[string]bool)
	var explore func(cur node.NodeID, path []node.NodeID, acc float64)
	explore = func(cur node.NodeID, path []node.NodeID, acc float64) {
		if budget <= 0 {
			return
		}
		budget--
		if cur == destination {
			key := pathKey(path)
			if !seen[key] {
				seen[key] = true
				p := make([]node.NodeID, len(path))
				copy(p, path)
				out = append(out, CandidatePath{Path: p, Cost: acc})
			}
			return
		}
		if len(path)-1 >= maxCandidateHops {
			return
		}
		for _, nb := range g.GetNeighbors(cur) {
			if visited[nb] {
				continue
			}
			link, ok := g.GetLink(cur, nb)
			if !ok {
				continue
			}
			visited[nb] = true
			explore(nb, append(path, nb), acc+g.CalculateCost(link))
			delete(visited, nb)
		}
	}
	explore(r.nodeID, []node.NodeID{r.nodeID}, 0)

	sort.Slice(out, func(i, j int) bool {
		if out[i].Cost != out[j].Cost {
			return out[i].Cost < out[j].Cost
		}
		return pathKey(out[i].Path) < pathKey(out[j].Path)
	})
	if len(out) > max {
		out = out[:max]
	}
	return out
}

func pathKey(path []node.NodeID) string {
	if len(path) == 0 {
		return ""
	}
	key := string(path[0])
	for _, n := range path[1:] {
		key += "\x00" + string(n)
	}
	return key
}
