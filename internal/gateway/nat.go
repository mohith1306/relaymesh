package gateway

import (
	"sync"
	"time"

	"github.com/relaymesh/relaymesh/internal/node"
)

// Flow tracks one mesh-to-internet egress flow so return traffic can be
// matched back to the requesting mesh node. Flows expire; only tracked
// flows may receive return traffic.
type Flow struct {
	Source    node.NodeID
	RequestID string
	Target    string
	CreatedAt time.Time
	ExpiresAt time.Time
}

type NATTable struct {
	flows map[string]*Flow
	mu    sync.RWMutex
}

func NewNATTable() *NATTable {
	return &NATTable{flows: make(map[string]*Flow)}
}

func flowKey(source node.NodeID, requestID string) string {
	return string(source) + "|" + requestID
}

// Track records an egress flow. Expired entries are refreshed.
func (nt *NATTable) Track(source node.NodeID, requestID, target string, ttl time.Duration) {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	nt.flows[flowKey(source, requestID)] = &Flow{
		Source:    source,
		RequestID: requestID,
		Target:    target,
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(ttl),
	}
}

// Lookup returns the flow if it exists and has not expired.
func (nt *NATTable) Lookup(source node.NodeID, requestID string) (*Flow, bool) {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	f, ok := nt.flows[flowKey(source, requestID)]
	if !ok {
		return nil, false
	}
	if time.Now().After(f.ExpiresAt) {
		delete(nt.flows, flowKey(source, requestID))
		return nil, false
	}
	return f, true
}

// Cleanup removes expired flows and returns the removal count.
func (nt *NATTable) Cleanup() int {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	removed := 0
	now := time.Now()
	for key, f := range nt.flows {
		if now.After(f.ExpiresAt) {
			delete(nt.flows, key)
			removed++
		}
	}
	return removed
}

// Count returns the number of live flows.
func (nt *NATTable) Count() int {
	nt.mu.RLock()
	defer nt.mu.RUnlock()
	n := 0
	now := time.Now()
	for _, f := range nt.flows {
		if now.Before(f.ExpiresAt) {
			n++
		}
	}
	return n
}
