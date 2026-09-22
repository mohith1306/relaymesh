"""Reward functions for RL training."""
import numpy as np
from typing import Dict, List


class RewardCalculator:
    """Calculates rewards for routing decisions."""

    def __init__(self, weights: Dict[str, float] = None):
        self.weights = weights or {
            'throughput': 1.0,
            'latency': -0.5,
            'packet_loss': -2.0,
            'congestion': -0.3,
            'hop_penalty': -0.1,
            'route_change': -0.2,
            'delivery_bonus': 10.0,
            'failure_penalty': -10.0,
        }
        self.previous_path: List[str] = []

    def calculate(self, metrics: Dict, path: List[str],
                  reached_destination: bool) -> float:
        reward = 0.0

        if reached_destination:
            reward += self.weights['delivery_bonus']
        elif not reached_destination and len(path) > 1:
            reward += self.weights['failure_penalty']

        reward += metrics.get('delivery_rate', 0) * self.weights['throughput']
        reward += metrics.get('avg_latency', 0) * self.weights['latency']
        reward += metrics.get('avg_packet_loss', 0) * self.weights['packet_loss']
        reward += metrics.get('avg_congestion', 0) * self.weights['congestion']

        hop_penalty = len(path) * self.weights['hop_penalty']
        reward += hop_penalty

        if self.previous_path and path != self.previous_path:
            reward += self.weights['route_change']

        self.previous_path = path.copy()
        return reward

    def calculate_segment_reward(self, source: str, next_hop: str,
                                 link_latency: float, link_loss: float,
                                 link_congestion: float) -> float:
        reward = 0.0
        reward += link_latency * self.weights['latency']
        reward += link_loss * self.weights['packet_loss']
        reward += link_congestion * self.weights['congestion']
        return reward


class AdaptiveRewardCalculator(RewardCalculator):
    """Dynamically adjusts reward weights based on performance."""

    def __init__(self):
        super().__init__()
        self.performance_history: List[float] = []
        self.adjustment_rate = 0.01

    def adjust_weights(self, recent_performance: float):
        self.performance_history.append(recent_performance)

        if len(self.performance_history) < 10:
            return

        avg_performance = np.mean(self.performance_history[-10:])

        if avg_performance < 0.5:
            self.weights['delivery_bonus'] *= (1 + self.adjustment_rate)
            self.weights['failure_penalty'] *= (1 + self.adjustment_rate)
        elif avg_performance > 0.8:
            self.weights['latency'] *= (1 - self.adjustment_rate)
            self.weights['throughput'] *= (1 + self.adjustment_rate)
