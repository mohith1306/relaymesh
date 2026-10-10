package ai

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/relaymesh/relaymesh/internal/node"
	"github.com/relaymesh/relaymesh/internal/routing"
	"github.com/relaymesh/relaymesh/internal/telemetry"
)

const (
	// adviceTTL bounds how long an accepted AI route is served without
	// re-evaluation even if metrics never change (topology may have).
	adviceTTL = 30 * time.Second
	// minRequeryInterval rate-limits gRPC calls when conditions are
	// unchanged (cache expiry, failed queries).
	minRequeryInterval = 500 * time.Millisecond
	// changedRequeryInterval bounds how fast the AI can be re-asked
	// after measured link conditions moved — this is the adaptation
	// latency: degradation in, re-evaluated route out.
	changedRequeryInterval = 100 * time.Millisecond
	// lossChangeThreshold ignores jitter below this loss delta.
	lossChangeThreshold = 0.01
	// candidateCount is how many loop-free paths Go hands Python.
	candidateCount = 4
)

// advice is one accepted AI recommendation and the link conditions it
// was chosen under.
type advice struct {
	route      *routing.Route
	path       []node.NodeID
	confidence float64
	expires    time.Time
	losses     map[node.NodeID]float64
}

type AIRouter struct {
	source      node.NodeID
	router      *routing.Router
	client      *Client
	safety      *SafetyValidator
	collectors  map[node.NodeID]*telemetry.MetricsCollector
	peers       []node.NodeID
	logger      *slog.Logger
	aiAvailable bool

	mu        sync.Mutex
	cache     map[node.NodeID]*advice
	lastQuery map[node.NodeID]time.Time
	queries   uint64
	served    uint64
}

func NewAIRouter(
	source node.NodeID,
	router *routing.Router,
	client *Client,
	logger *slog.Logger,
) *AIRouter {
	return &AIRouter{
		source:      source,
		router:      router,
		client:      client,
		safety:      NewSafetyValidator(router, logger),
		collectors:  make(map[node.NodeID]*telemetry.MetricsCollector),
		logger:      logger,
		aiAvailable: false,
		cache:       make(map[node.NodeID]*advice),
		lastQuery:   make(map[node.NodeID]time.Time),
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

// Stats reports gRPC attempts and AI routes actually served.
func (ar *AIRouter) Stats() (queries, served uint64) {
	ar.mu.Lock()
	defer ar.mu.Unlock()
	return ar.queries, ar.served
}

// snapshotLocked captures per-peer packet loss from the source's
// telemetry collector — byte-for-byte the input Python scores — so an
// advice can be invalidated the moment that input changes.
func (ar *AIRouter) snapshotLocked() map[node.NodeID]float64 {
	src, ok := ar.collectors[ar.source]
	if !ok || src == nil {
		return nil
	}
	peers := src.GetAllPeerMetrics()
	if len(peers) == 0 {
		return nil
	}
	losses := make(map[node.NodeID]float64, len(peers))
	for _, pm := range peers {
		losses[pm.PeerID] = pm.PacketLoss
	}
	return losses
}

// conditionsChanged reports whether scored link loss moved since the
// advice was accepted. With no baseline there is nothing to compare.
func conditionsChanged(base, cur map[node.NodeID]float64) bool {
	if base == nil || cur == nil {
		return false
	}
	if len(base) != len(cur) {
		return true
	}
	for peer, loss := range base {
		curLoss, ok := cur[peer]
		if !ok {
			return true
		}
		if math.Abs(curLoss-loss) > lossChangeThreshold {
			return true
		}
	}
	return false
}

func (ar *AIRouter) queryDue(dest node.NodeID, conditionsChanged bool) bool {
	last, ok := ar.lastQuery[dest]
	if !ok {
		return true
	}
	elapsed := time.Since(last)
	if conditionsChanged {
		return elapsed >= changedRequeryInterval
	}
	return elapsed >= minRequeryInterval
}

// GetRoute serves the cached advice while measured conditions match,
// re-queries the AI service when conditions change (or the advice
// expires), and always falls back to deterministic routing when the AI
// is unavailable, fails, or is rejected by the safety validator.
func (ar *AIRouter) GetRoute(ctx context.Context, source, destination node.NodeID) (*routing.Route, bool) {
	ar.mu.Lock()
	defer ar.mu.Unlock()

	if !ar.aiAvailable {
		return ar.router.GetRoute(destination)
	}

	// 1) Fresh cached advice with unchanged link conditions.
	changed := false
	if adv, ok := ar.cache[destination]; ok && time.Now().Before(adv.expires) {
		if conditionsChanged(adv.losses, ar.snapshotLocked()) {
			// Degradation (or recovery) since acceptance: the moment
			// to re-evaluate — drop the advice and re-query below.
			changed = true
			delete(ar.cache, destination)
		} else {
			res := ar.safety.ValidateRecommendation(source, destination, adv.path, adv.confidence)
			if res.Valid {
				ar.served++
				return adv.route, true
			}
			// Topology moved under us: drop and re-query below.
			delete(ar.cache, destination)
		}
	}

	// 2) Conditions changed / cache empty: ask AI if rate limit allows.
	if ar.queryDue(destination, changed) {
		ar.lastQuery[destination] = time.Now()
		ar.queries++
		if route, ok := ar.queryLocked(ctx, source, destination); ok {
			return route, true
		}
	}

	// 3) Deterministic fallback — AI can never break forwarding.
	return ar.router.GetRoute(destination)
}

// queryLocked performs the gRPC round trip and caches a valid answer.
// Caller holds ar.mu.
func (ar *AIRouter) queryLocked(ctx context.Context, source, destination node.NodeID) (*routing.Route, bool) {
	candidates := ar.router.Candidates(destination, candidateCount)

	rec, err := ar.client.GetRecommendation(
		ctx,
		source,
		destination,
		ar.collectors,
		candidates,
	)
	if err != nil {
		ar.logger.Warn("AI recommendation failed, falling back to deterministic",
			"error", err,
		)
		return nil, false
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
		return nil, false
	}

	if len(path) < 2 {
		ar.logger.Warn("AI recommendation path too short, using fallback", "path", path)
		return nil, false
	}

	route := &routing.Route{
		Destination: destination,
		NextHop:     path[1],
		Path:        path,
		Cost:        rec.Score,
		HopCount:    uint32(len(path) - 1),
		Expiry:      time.Now().Add(adviceTTL),
		CreatedAt:   time.Now(),
	}
	ar.cache[destination] = &advice{
		route:      route,
		path:       path,
		confidence: rec.Confidence,
		expires:    time.Now().Add(adviceTTL),
		losses:     ar.snapshotLocked(),
	}
	ar.served++

	ar.logger.Info("using AI route",
		"source", source,
		"destination", destination,
		"path", path,
		"confidence", rec.Confidence,
		"candidates", len(candidates),
	)

	return route, true
}

func (ar *AIRouter) OnRouteUsed(ctx context.Context, source, destination node.NodeID, success bool, latency float64) {
	if ctx == nil {
		return
	}
	reward := 0.0
	if success {
		reward = 10.0 - latency*0.1
	} else {
		reward = -10.0
	}

	routeID := string(source) + "-" + string(destination)
	if err := ar.client.ReportFeedback(ctx, routeID, reward, success); err != nil {
		ar.logger.Debug("feedback report failed", "error", err)
	}
}

func (ar *AIRouter) SetAIAvailable(available bool) {
	ar.mu.Lock()
	defer ar.mu.Unlock()
	ar.aiAvailable = available
	ar.logger.Info("AI availability changed", "available", available)
}

func (ar *AIRouter) IsAIAvailable() bool {
	ar.mu.Lock()
	defer ar.mu.Unlock()
	return ar.aiAvailable
}

// AdviseRoute implements mesh.RouteAdvisor: it returns the AI-chosen
// next hop when the safety validator accepts the recommendation, and
// reports false so the caller falls back to deterministic routing
// otherwise. AI can never break forwarding — worst case is fallback.
func (ar *AIRouter) AdviseRoute(ctx context.Context, dest node.NodeID) (node.NodeID, bool) {
	route, ok := ar.GetRoute(ctx, ar.source, dest)
	if !ok {
		return "", false
	}
	if route.NextHop == "" {
		return "", false
	}
	return route.NextHop, true
}
