package ai

import (
	"log/slog"
	"time"

	"github.com/relaymesh/relaymesh/internal/node"
	"github.com/relaymesh/relaymesh/internal/routing"
)

type SafetyValidator struct {
	router  *routing.Router
	logger  *slog.Logger
	maxHops uint32
	maxLatency float64
	maxPacketLoss float64
}

func NewSafetyValidator(router *routing.Router, logger *slog.Logger) *SafetyValidator {
	return &SafetyValidator{
		router:       router,
		logger:       logger,
		maxHops:      10,
		maxLatency:   100.0,
		maxPacketLoss: 0.1,
	}
}

type ValidationResult struct {
	Valid    bool
	Reason   string
	Fallback *routing.Route
}

func (sv *SafetyValidator) ValidateRecommendation(
	source, destination node.NodeID,
	path []node.NodeID,
	confidence float64,
) ValidationResult {

	if len(path) == 0 {
		return ValidationResult{
			Valid:  false,
			Reason: "empty path",
		}
	}

	if path[0] != source {
		return ValidationResult{
			Valid:  false,
			Reason: "path does not start at source",
		}
	}

	if path[len(path)-1] != destination {
		return ValidationResult{
			Valid:  false,
			Reason: "path does not end at destination",
		}
	}

	if uint32(len(path)-1) > sv.maxHops {
		sv.logger.Warn("AI recommendation exceeds max hops",
			"hops", len(path)-1,
			"max", sv.maxHops,
		)
		fallback, exists := sv.router.GetRoute(destination)
		if exists {
			return ValidationResult{
				Valid:    false,
				Reason:   "exceeds max hops",
				Fallback: fallback,
			}
		}
		return ValidationResult{
			Valid:  false,
			Reason: "exceeds max hops and no fallback",
		}
	}

	for i := 0; i < len(path)-1; i++ {
		link, exists := sv.router.GetGraph().GetLink(path[i], path[i+1])
		if !exists {
			sv.logger.Warn("AI recommendation uses non-existent link",
				"from", path[i],
				"to", path[i+1],
			)
			fallback, _ := sv.router.GetRoute(destination)
			return ValidationResult{
				Valid:    false,
				Reason:   "non-existent link",
				Fallback: fallback,
			}
		}

		if link.Latency > sv.maxLatency {
			sv.logger.Warn("AI recommendation has high latency link",
				"latency", link.Latency,
				"max", sv.maxLatency,
			)
		}

		if link.PacketLoss > sv.maxPacketLoss {
			sv.logger.Warn("AI recommendation has high packet loss link",
				"loss", link.PacketLoss,
				"max", sv.maxPacketLoss,
			)
		}
	}

	if confidence < 0.3 {
		sv.logger.Warn("AI recommendation has low confidence",
			"confidence", confidence,
		)
		fallback, exists := sv.router.GetRoute(destination)
		if exists {
			return ValidationResult{
				Valid:    false,
				Reason:   "low confidence",
				Fallback: fallback,
			}
		}
	}

	return ValidationResult{
		Valid:  true,
		Reason: "valid",
	}
}

func (sv *SafetyValidator) ShouldUseAI(
	source, destination node.NodeID,
	aiAvailable bool,
	lastAIRecommendation time.Time,
) bool {
	if !aiAvailable {
		return false
	}

	deterministicRoute, exists := sv.router.GetRoute(destination)
	if !exists {
		return true
	}

	if deterministicRoute.HopCount > 3 {
		return true
	}

	if time.Since(lastAIRecommendation) > 30*time.Second {
		return true
	}

	return false
}
