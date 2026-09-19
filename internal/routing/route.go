package routing

import (
	"time"

	"github.com/relaymesh/relaymesh/internal/node"
)

type Route struct {
	Destination node.NodeID
	NextHop     node.NodeID
	Path        []node.NodeID
	Cost        float64
	HopCount    uint32
	Expiry      time.Time
	CreatedAt   time.Time
}

func NewRoute(dest, nextHop node.NodeID, path []node.NodeID, cost float64, hopCount uint32, ttl time.Duration) *Route {
	return &Route{
		Destination: dest,
		NextHop:     nextHop,
		Path:        path,
		Cost:        cost,
		HopCount:    hopCount,
		Expiry:      time.Now().Add(ttl),
		CreatedAt:   time.Now(),
	}
}

func (r *Route) IsExpired() bool {
	return time.Now().After(r.Expiry)
}

func (r *Route) IsValid() bool {
	return !r.IsExpired() && r.Cost < Infinity
}

func (r *Route) Clone() *Route {
	pathCopy := make([]node.NodeID, len(r.Path))
	copy(pathCopy, r.Path)

	return &Route{
		Destination: r.Destination,
		NextHop:     r.NextHop,
		Path:        pathCopy,
		Cost:        r.Cost,
		HopCount:    r.HopCount,
		Expiry:      r.Expiry,
		CreatedAt:   r.CreatedAt,
	}
}
