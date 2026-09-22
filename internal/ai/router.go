package ai

import (
	"log/slog"
	"sync"
	"time"

	"github.com/relaymesh/relaymesh/internal/node"
	"github.com/relaymesh/relaymesh/internal/routing"
	"github.com/relaymesh/relaymesh/internal/telemetry"
)

type AIRouter struct {
	router          *routing.Router
	client          *Client
	safety          *SafetyValidator
	collectors      map[node.NodeID]*telemetry.MetricsCollector
	peers           []node.NodeID
	logger          *slog.Logger
	aiAvailable     bool
	lastRecommendation time.Time
	mu              sync.RWMutex
}

func NewAIRouter(
	router *routing.Router,
	client *Client,
	logger *slog.Logger,
) *AIRouter {
	return &AIRouter{
		router:      router,
		client:      client,
		safety:      NewSafetyValidator(router, logger),
		collectors:  make(map[node.NodeID]*telemetry.MetricsCollector),
		logger:      logger,
		aiAvailable: false,
	}
}

func (ar *AIRouter) SetPeers(peers []node.NodeID) {
	ar.mu.Lock()
	defer ar.mu.Unlock()
	ar.peers = peers
}

func (ar *AIRouter) SetCollector(nodeID node.NodeID, collector *telemetry.MetricsCollector) {
	ar.mu.Lock()
	defer ar.mu.Unlock()
	ar.collectors[nodeID] = collector
}

func (ar *AIRouter) GetRoute(source, destination node.NodeID) (*routing.Route, bool) {
	ar.mu.RLock()
	defer ar.mu.RUnlock()

	if !ar.safety.ShouldUseAI(source, destination, ar.aiAvailable, ar.lastRecommendation) {
		ar.logger.Debug("using deterministic routing",
			"source", source,
			"destination", destination,
		)
		return ar.router.GetRoute(destination)
	}

	rec, err := ar.client.GetRecommendation(
		nil,
		source,
		destination,
		ar.collectors,
		ar.peers,
	)

	if err != nil {
		ar.logger.Warn("AI recommendation failed, falling back to deterministic",
			"error", err,
		)
		return ar.router.GetRoute(destination)
	}

	path := make([]node.NodeID, len(rec.Path))
	for i, p := range rec.Path {
		path[i] = node.NodeID(p)
	}

	result := ar.safety.ValidateRecommendation(source, destination, path, rec.Confidence)

	if !result.Valid {
		ar.logger.Warn("AI recommendation invalid, using fallback",
			"reason", result.Reason,
		)
		if result.Fallback != nil {
			return result.Fallback, true
		}
		return ar.router.GetRoute(destination)
	}

	ar.lastRecommendation = time.Now()

	route := &routing.Route{
		Destination: destination,
		NextHop:     path[1],
		Path:        path,
		Cost:        rec.Score,
		HopCount:    uint32(len(path) - 1),
		Expiry:      time.Now().Add(5 * time.Minute),
		CreatedAt:   time.Now(),
	}

	ar.logger.Info("using AI route",
		"source", source,
		"destination", destination,
		"path", path,
		"confidence", rec.Confidence,
	)

	return route, true
}

func (ar *AIRouter) OnRouteUsed(source, destination node.NodeID, success bool, latency float64) {
	reward := 0.0
	if success {
		reward = 10.0 - latency*0.1
	} else {
		reward = -10.0
	}

	routeID := string(source) + "-" + string(destination)
	ar.client.ReportFeedback(nil, routeID, reward, success)
}

func (ar *AIRouter) SetAIAvailable(available bool) {
	ar.mu.Lock()
	defer ar.mu.Unlock()
	ar.aiAvailable = available
	ar.logger.Info("AI availability changed", "available", available)
}

func (ar *AIRouter) IsAIAvailable() bool {
	ar.mu.RLock()
	defer ar.mu.RUnlock()
	return ar.aiAvailable
}
