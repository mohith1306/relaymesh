package telemetry

import (
	"sync"
	"time"

	"github.com/relaymesh/relaymesh/internal/node"
)

type NetworkMetrics struct {
	Latency      float64
	PacketLoss   float64
	Bandwidth    float64
	Jitter       float64
	Congestion   float64
	HopCount     uint32
	Stability    float64
	QueueSize    uint64
	CPUUsage     float64
	MemoryUsage  float64
}

type PeerMetrics struct {
	PeerID     node.NodeID
	Latency    float64
	PacketLoss float64
	Bandwidth  float64
	LastSeen   time.Time
}

type MetricsSnapshot struct {
	NodeID    node.NodeID
	Metrics   NetworkMetrics
	Peers     []PeerMetrics
	Timestamp time.Time
}

type MetricsCollector struct {
	nodeID     node.NodeID
	metrics    NetworkMetrics
	peerMetrics map[node.NodeID]*PeerMetrics
	snapshots  []MetricsSnapshot
	mu         sync.RWMutex
	maxSnapshots int
}

func NewCollector(nodeID node.NodeID) *MetricsCollector {
	return &MetricsCollector{
		nodeID:       nodeID,
		peerMetrics:  make(map[node.NodeID]*PeerMetrics),
		snapshots:    make([]MetricsSnapshot, 0),
		maxSnapshots: 1000,
	}
}

func (mc *MetricsCollector) UpdateLatency(latency float64) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.metrics.Latency = latency
}

func (mc *MetricsCollector) UpdatePacketLoss(loss float64) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.metrics.PacketLoss = loss
}

func (mc *MetricsCollector) UpdateBandwidth(bw float64) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.metrics.Bandwidth = bw
}

func (mc *MetricsCollector) UpdateJitter(jitter float64) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.metrics.Jitter = jitter
}

func (mc *MetricsCollector) UpdateCongestion(congestion float64) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.metrics.Congestion = congestion
}

func (mc *MetricsCollector) UpdateHopCount(hops uint32) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.metrics.HopCount = hops
}

func (mc *MetricsCollector) UpdateStability(stability float64) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.metrics.Stability = stability
}

func (mc *MetricsCollector) UpdateQueueSize(size uint64) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.metrics.QueueSize = size
}

func (mc *MetricsCollector) UpdateCPUUsage(cpu float64) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.metrics.CPUUsage = cpu
}

func (mc *MetricsCollector) UpdateMemoryUsage(mem float64) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.metrics.MemoryUsage = mem
}

func (mc *MetricsCollector) UpdatePeerMetrics(peerID node.NodeID, latency, loss, bw float64) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	peer, exists := mc.peerMetrics[peerID]
	if !exists {
		peer = &PeerMetrics{PeerID: peerID}
		mc.peerMetrics[peerID] = peer
	}

	peer.Latency = latency
	peer.PacketLoss = loss
	peer.Bandwidth = bw
	peer.LastSeen = time.Now()
}

func (mc *MetricsCollector) GetMetrics() NetworkMetrics {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.metrics
}

func (mc *MetricsCollector) GetPeerMetrics(peerID node.NodeID) (*PeerMetrics, bool) {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	peer, ok := mc.peerMetrics[peerID]
	return peer, ok
}

func (mc *MetricsCollector) GetAllPeerMetrics() []PeerMetrics {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	peers := make([]PeerMetrics, 0, len(mc.peerMetrics))
	for _, peer := range mc.peerMetrics {
		peers = append(peers, *peer)
	}
	return peers
}

func (mc *MetricsCollector) Snapshot() MetricsSnapshot {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	peers := make([]PeerMetrics, 0, len(mc.peerMetrics))
	for _, peer := range mc.peerMetrics {
		peers = append(peers, *peer)
	}

	snapshot := MetricsSnapshot{
		NodeID:    mc.nodeID,
		Metrics:   mc.metrics,
		Peers:     peers,
		Timestamp: time.Now(),
	}

	mc.mu.RUnlock()
	mc.mu.Lock()
	mc.snapshots = append(mc.snapshots, snapshot)
	if len(mc.snapshots) > mc.maxSnapshots {
		mc.snapshots = mc.snapshots[1:]
	}
	mc.mu.Unlock()
	mc.mu.RLock()

	return snapshot
}

func (mc *MetricsCollector) GetSnapshots(count int) []MetricsSnapshot {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	if count > len(mc.snapshots) {
		count = len(mc.snapshots)
	}

	start := len(mc.snapshots) - count
	result := make([]MetricsSnapshot, count)
	copy(result, mc.snapshots[start:])
	return result
}

func (mc *MetricsCollector) ToFeatureVector() []float32 {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	return []float32{
		float32(mc.metrics.Latency),
		float32(mc.metrics.PacketLoss),
		float32(mc.metrics.Bandwidth),
		float32(mc.metrics.Jitter),
		float32(mc.metrics.Congestion),
		float32(mc.metrics.HopCount),
		float32(mc.metrics.Stability),
		float32(mc.metrics.QueueSize),
		float32(mc.metrics.CPUUsage),
		float32(mc.metrics.MemoryUsage),
	}
}
