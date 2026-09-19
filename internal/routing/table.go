package routing

import (
	"sync"

	"github.com/relaymesh/relaymesh/internal/node"
)

type Table struct {
	routes map[node.NodeID]map[node.NodeID]*Route
	mu     sync.RWMutex
}

func NewTable() *Table {
	return &Table{
		routes: make(map[node.NodeID]map[node.NodeID]*Route),
	}
}

func (t *Table) AddRoute(route *Route) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.routes[route.Destination]; !ok {
		t.routes[route.Destination] = make(map[node.NodeID]*Route)
	}
	t.routes[route.Destination][route.NextHop] = route
}

func (t *Table) GetRoute(dest, nextHop node.NodeID) (*Route, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if hops, ok := t.routes[dest]; ok {
		if route, ok := hops[nextHop]; ok {
			if !route.IsExpired() {
				return route, true
			}
		}
	}
	return nil, false
}

func (t *Table) GetBestRoute(dest node.NodeID) (*Route, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	hops, ok := t.routes[dest]
	if !ok {
		return nil, false
	}

	var best *Route
	for _, route := range hops {
		if route.IsExpired() {
			continue
		}
		if best == nil || route.Cost < best.Cost {
			best = route
		}
	}

	return best, best != nil
}

func (t *Table) RemoveRoute(dest, nextHop node.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if hops, ok := t.routes[dest]; ok {
		delete(hops, nextHop)
		if len(hops) == 0 {
			delete(t.routes, dest)
		}
	}
}

func (t *Table) RemoveAllFrom(nextHop node.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for dest, hops := range t.routes {
		delete(hops, nextHop)
		if len(hops) == 0 {
			delete(t.routes, dest)
		}
	}
}

func (t *Table) GetRoutesTo(dest node.NodeID) []*Route {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var routes []*Route
	if hops, ok := t.routes[dest]; ok {
		for _, route := range hops {
			if !route.IsExpired() {
				routes = append(routes, route)
			}
		}
	}
	return routes
}

func (t *Table) Cleanup() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	removed := 0
	for dest, hops := range t.routes {
		for nextHop, route := range hops {
			if route.IsExpired() {
				delete(hops, nextHop)
				removed++
			}
		}
		if len(hops) == 0 {
			delete(t.routes, dest)
		}
	}
	return removed
}

func (t *Table) GetAllDestinations() []node.NodeID {
	t.mu.RLock()
	defer t.mu.RUnlock()

	dests := make([]node.NodeID, 0, len(t.routes))
	for dest := range t.routes {
		dests = append(dests, dest)
	}
	return dests
}

func (t *Table) GetNextHop(dest node.NodeID) (node.NodeID, bool) {
	route, ok := t.GetBestRoute(dest)
	if !ok {
		return "", false
	}
	return route.NextHop, true
}
