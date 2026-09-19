package routing

import (
	"math"
	"sync"

	"github.com/relaymesh/relaymesh/internal/node"
)

const Infinity = math.MaxFloat64

type Link struct {
	From      node.NodeID
	To        node.NodeID
	Latency   float64
	Bandwidth float64
	PacketLoss float64
	Weight    float64
}

type Graph struct {
	links  map[node.NodeID]map[node.NodeID]*Link
	mu     sync.RWMutex
}

func NewGraph() *Graph {
	return &Graph{
		links: make(map[node.NodeID]map[node.NodeID]*Link),
	}
}

func (g *Graph) AddLink(link *Link) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.links[link.From]; !ok {
		g.links[link.From] = make(map[node.NodeID]*Link)
	}
	if _, ok := g.links[link.To]; !ok {
		g.links[link.To] = make(map[node.NodeID]*Link)
	}

	g.links[link.From][link.To] = link
	g.links[link.To][link.From] = &Link{
		From:       link.To,
		To:         link.From,
		Latency:    link.Latency,
		Bandwidth:  link.Bandwidth,
		PacketLoss: link.PacketLoss,
		Weight:     link.Weight,
	}
}

func (g *Graph) RemoveLink(from, to node.NodeID) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if hops, ok := g.links[from]; ok {
		delete(hops, to)
	}
	if hops, ok := g.links[to]; ok {
		delete(hops, from)
	}
}

func (g *Graph) GetNeighbors(id node.NodeID) []node.NodeID {
	g.mu.RLock()
	defer g.mu.RUnlock()

	var neighbors []node.NodeID
	if hops, ok := g.links[id]; ok {
		for neighbor := range hops {
			neighbors = append(neighbors, neighbor)
		}
	}
	return neighbors
}

func (g *Graph) GetLink(from, to node.NodeID) (*Link, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if hops, ok := g.links[from]; ok {
		if link, ok := hops[to]; ok {
			return link, true
		}
	}
	return nil, false
}

func (g *Graph) CalculateCost(link *Link) float64 {
	if link.Weight > 0 {
		return link.Weight
	}
	return link.Latency + (link.PacketLoss * 100) + (1.0 / link.Bandwidth)
}

type Dijkstra struct {
	graph *Graph
}

func NewDijkstra(graph *Graph) *Dijkstra {
	return &Dijkstra{graph: graph}
}

type PathResult struct {
	Path     []node.NodeID
	Cost     float64
	HopCount uint32
	Found    bool
}

func (d *Dijkstra) FindPath(source, destination node.NodeID) PathResult {
	d.graph.mu.RLock()
	defer d.graph.mu.RUnlock()

	dist := make(map[node.NodeID]float64)
	prev := make(map[node.NodeID]node.NodeID)
	visited := make(map[node.NodeID]bool)

	for node := range d.graph.links {
		dist[node] = Infinity
	}
	dist[source] = 0

	for {
		var current node.NodeID
		minDist := Infinity

		for node, d := range dist {
			if !visited[node] && d < minDist {
				minDist = d
				current = node
			}
		}

		if current == "" || current == destination {
			break
		}

		visited[current] = true

		if hops, ok := d.graph.links[current]; ok {
			for neighbor, link := range hops {
				if visited[neighbor] {
					continue
				}

				newDist := dist[current] + d.graph.CalculateCost(link)
				if newDist < dist[neighbor] {
					dist[neighbor] = newDist
					prev[neighbor] = current
				}
			}
		}
	}

	if _, ok := dist[destination]; !ok || dist[destination] == Infinity {
		return PathResult{Found: false}
	}

	path := make([]node.NodeID, 0)
	for at := destination; at != ""; at = prev[at] {
		path = append([]node.NodeID{at}, path...)
		if at == source {
			break
		}
	}

	return PathResult{
		Path:     path,
		Cost:     dist[destination],
		HopCount: uint32(len(path) - 1),
		Found:    true,
	}
}

func (d *Dijkstra) FindAllPaths(source node.NodeID) map[node.NodeID]PathResult {
	d.graph.mu.RLock()
	defer d.graph.mu.RUnlock()

	results := make(map[node.NodeID]PathResult)

	dist := make(map[node.NodeID]float64)
	prev := make(map[node.NodeID]node.NodeID)
	visited := make(map[node.NodeID]bool)

	for node := range d.graph.links {
		dist[node] = Infinity
	}
	dist[source] = 0

	for {
		var current node.NodeID
		minDist := Infinity

		for node, d := range dist {
			if !visited[node] && d < minDist {
				minDist = d
				current = node
			}
		}

		if current == "" {
			break
		}

		visited[current] = true

		if hops, ok := d.graph.links[current]; ok {
			for neighbor, link := range hops {
				if visited[neighbor] {
					continue
				}

				newDist := dist[current] + d.graph.CalculateCost(link)
				if newDist < dist[neighbor] {
					dist[neighbor] = newDist
					prev[neighbor] = current
				}
			}
		}
	}

	for dest := range d.graph.links {
		if dest == source {
			continue
		}

		if dist[dest] == Infinity {
			results[dest] = PathResult{Found: false}
			continue
		}

		path := make([]node.NodeID, 0)
		for at := dest; at != ""; at = prev[at] {
			path = append([]node.NodeID{at}, path...)
			if at == source {
				break
			}
		}

		results[dest] = PathResult{
			Path:     path,
			Cost:     dist[dest],
			HopCount: uint32(len(path) - 1),
			Found:    true,
		}
	}

	return results
}
