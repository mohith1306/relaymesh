package routing

import (
	"log/slog"
	"sync"
	"time"

	"github.com/relaymesh/relaymesh/internal/node"
)

type Router struct {
	nodeID    node.NodeID
	table     *Table
	graph     *Graph
	dijkstra  *Dijkstra
	logger    *slog.Logger
	mu        sync.RWMutex
}

func New(nodeID node.NodeID, logger *slog.Logger) *Router {
	graph := NewGraph()
	return &Router{
		nodeID:   nodeID,
		table:    NewTable(),
		graph:    graph,
		dijkstra: NewDijkstra(graph),
		logger:   logger,
	}
}

func (r *Router) UpdateLink(from, to node.NodeID, latency, bandwidth, packetLoss float64) {
	r.graph.AddLink(&Link{
		From:       from,
		To:         to,
		Latency:    latency,
		Bandwidth:  bandwidth,
		PacketLoss: packetLoss,
	})

	r.recalculate()
}

func (r *Router) RemoveLink(from, to node.NodeID) {
	r.graph.RemoveLink(from, to)
	r.table.RemoveAllFrom(to)
}

// PruneStaleLinks removes graph links not refreshed within maxAge and
// rebuilds the routing table so learned multi-hop routes disappear
// when the underlying path breaks. Returns the pruned link count.
func (r *Router) PruneStaleLinks(maxAge time.Duration) int {
	removed := r.graph.RemoveStaleLinks(maxAge)
	if len(removed) == 0 {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.table.Clear()
	r.table.Cleanup()
	results := r.dijkstra.FindAllPaths(r.nodeID)
	for dest, result := range results {
		if !result.Found || len(result.Path) < 2 {
			continue
		}
		route := NewRoute(dest, result.Path[1], result.Path, result.Cost, result.HopCount, 5*time.Minute)
		r.table.AddRoute(route)
	}
	r.logger.Debug("pruned stale links",
		"removed", len(removed),
		"node_id", r.nodeID,
	)
	return len(removed)
}

func (r *Router) GetRoute(destination node.NodeID) (*Route, bool) {
	if destination == r.nodeID {
		return &Route{
			Destination: r.nodeID,
			NextHop:     r.nodeID,
			Path:        []node.NodeID{r.nodeID},
			Cost:        0,
			HopCount:    0,
			Expiry:      time.Now().Add(time.Hour),
			CreatedAt:   time.Now(),
		}, true
	}

	return r.table.GetBestRoute(destination)
}

func (r *Router) GetNextHop(destination node.NodeID) (node.NodeID, bool) {
	if destination == r.nodeID {
		return r.nodeID, true
	}
	return r.table.GetNextHop(destination)
}

func (r *Router) recalculate() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.table.Cleanup()

	results := r.dijkstra.FindAllPaths(r.nodeID)

	for dest, result := range results {
		if !result.Found {
			continue
		}

		if len(result.Path) < 2 {
			continue
		}

		nextHop := result.Path[1]
		route := NewRoute(dest, nextHop, result.Path, result.Cost, result.HopCount, 5*time.Minute)
		r.table.AddRoute(route)
	}

	r.logger.Debug("routing table recalculated",
		"destinations", len(results),
		"node_id", r.nodeID,
	)
}

func (r *Router) GetTable() *Table {
	return r.table
}

func (r *Router) GetGraph() *Graph {
	return r.graph
}

func (r *Router) HandleRoutingUpdate(source node.NodeID, routes []*Route) {
	r.logger.Debug("received routing update",
		"source", source,
		"routes", len(routes),
	)

	for _, route := range routes {
		if route.Destination == r.nodeID {
			continue
		}

		if len(route.Path) > 0 && route.Path[0] == r.nodeID {
			continue
		}

		newPath := make([]node.NodeID, 0, len(route.Path)+1)
		newPath = append(newPath, r.nodeID)
		newPath = append(newPath, route.Path...)

		link, exists := r.graph.GetLink(r.nodeID, source)
		if !exists {
			continue
		}

		newCost := r.graph.CalculateCost(link) + route.Cost
		newRoute := NewRoute(route.Destination, source, newPath, newCost, route.HopCount+1, 5*time.Minute)
		r.table.AddRoute(newRoute)
	}
}

func (r *Router) GetRoutingUpdate() []*Route {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var routes []*Route
	for _, dest := range r.table.GetAllDestinations() {
		if route, ok := r.table.GetBestRoute(dest); ok {
			routes = append(routes, route)
		}
	}
	return routes
}
