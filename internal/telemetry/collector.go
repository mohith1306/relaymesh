package telemetry

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/relaymesh/relaymesh/internal/node"
)

type CollectorService struct {
	nodeID     node.NodeID
	collectors map[node.NodeID]*MetricsCollector
	logger     *slog.Logger
	mu         sync.RWMutex
}

func NewCollectorService(nodeID node.NodeID, logger *slog.Logger) *CollectorService {
	return &CollectorService{
		nodeID:     nodeID,
		collectors: make(map[node.NodeID]*MetricsCollector),
		logger:     logger,
	}
}

func (cs *CollectorService) Start(ctx context.Context) {
	cs.logger.Info("starting telemetry collector", "node_id", cs.nodeID)

	go cs.collectionLoop(ctx)
	go cs.reportLoop(ctx)
}

func (cs *CollectorService) GetCollector(peerID node.NodeID) *MetricsCollector {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	collector, exists := cs.collectors[peerID]
	if !exists {
		collector = NewCollector(peerID)
		cs.collectors[peerID] = collector
	}
	return collector
}

func (cs *CollectorService) GetNodeCollector() *MetricsCollector {
	return cs.GetCollector(cs.nodeID)
}

func (cs *CollectorService) GetPeerCollector(peerID node.NodeID) *MetricsCollector {
	return cs.GetCollector(peerID)
}

func (cs *CollectorService) collectionLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cs.collectMetrics()
		}
	}
}

func (cs *CollectorService) collectMetrics() {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	for peerID, collector := range cs.collectors {
		if peerID == cs.nodeID {
			continue
		}

		_ = collector
	}
}

func (cs *CollectorService) reportLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cs.generateReport()
		}
	}
}

func (cs *CollectorService) generateReport() {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	for peerID, collector := range cs.collectors {
		snapshot := collector.Snapshot()
		cs.logger.Debug("telemetry report",
			"peer_id", peerID,
			"latency", snapshot.Metrics.Latency,
			"packet_loss", snapshot.Metrics.PacketLoss,
			"bandwidth", snapshot.Metrics.Bandwidth,
		)
	}
}

func (cs *CollectorService) GetNetworkState() map[node.NodeID]NetworkMetrics {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	state := make(map[node.NodeID]NetworkMetrics)
	for peerID, collector := range cs.collectors {
		state[peerID] = collector.GetMetrics()
	}
	return state
}

func (cs *CollectorService) GetFeatureVectors() map[node.NodeID][]float32 {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	vectors := make(map[node.NodeID][]float32)
	for peerID, collector := range cs.collectors {
		vectors[peerID] = collector.ToFeatureVector()
	}
	return vectors
}

func (cs *CollectorService) Stop() {
	cs.logger.Info("stopping telemetry collector", "node_id", cs.nodeID)
}
