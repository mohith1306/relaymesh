"""Network simulation for RL training."""
import numpy as np
from typing import Dict, List, Tuple, Optional
from .topology import NetworkTopology, Node, Link, create_mesh_topology
import random


class NetworkSimulator:
    """Simulates network conditions for RL training."""

    def __init__(self, topology: NetworkTopology):
        self.topology = topology
        self.time_step = 0
        self.packet_count = 0
        self.delivered_count = 0
        self.dropped_count = 0
        self.history: List[Dict] = []

    def reset(self):
        self.time_step = 0
        self.packet_count = 0
        self.delivered_count = 0
        self.dropped_count = 0
        self.history = []
        for link in self.topology.links.values():
            link.congestion = 0.0
            link.is_active = True
        return self._get_state()

    def step(self, action: int) -> Tuple[np.ndarray, float, bool, Dict]:
        self.time_step += 1

        self._update_network_conditions()

        reward = self._calculate_reward(action)
        state = self._get_state()
        done = self.time_step >= 100

        info = {
            'time_step': self.time_step,
            'packet_count': self.packet_count,
            'delivered_count': self.delivered_count,
            'dropped_count': self.dropped_count,
            'delivery_rate': self.delivered_count / max(1, self.packet_count),
        }

        self.history.append(info)

        return state, reward, done, info

    def _update_network_conditions(self):
        for link in self.topology.links.values():
            link.congestion += random.uniform(-0.05, 0.05)
            link.congestion = max(0, min(1, link.congestion))

            link.latency += random.uniform(-1, 1)
            link.latency = max(1, link.latency)

            if random.random() < 0.001:
                link.is_active = not link.is_active

    def _calculate_reward(self, action: int) -> float:
        self.packet_count += 1

        source = "node-A"
        destination = f"node-{chr(65 + len(self.topology.nodes) - 1)}"

        paths = self.topology.get_all_paths(source, destination, max_hops=5)

        if not paths:
            self.dropped_count += 1
            return -10.0

        best_path = min(paths, key=lambda p: self.topology.calculate_path_cost(p))
        cost = self.topology.calculate_path_cost(best_path)

        if cost < float('inf'):
            self.delivered_count += 1
            reward = 10.0 - cost * 0.1
        else:
            self.dropped_count += 1
            reward = -10.0

        return reward

    def _get_state(self) -> np.ndarray:
        source = "node-A"
        destination = f"node-{chr(65 + len(self.topology.nodes) - 1)}"
        return self.topology.get_state_vector(source, destination)

    def inject_failure(self, source: str, target: str):
        self.topology.remove_link(source, target)

    def restore_link(self, source: str, target: str, latency: float = 10.0,
                     bandwidth: float = 100.0, packet_loss: float = 0.01):
        link = Link(source=source, target=target, latency=latency,
                    bandwidth=bandwidth, packet_loss=packet_loss)
        self.topology.add_link(link)

    def get_metrics(self) -> Dict:
        return {
            'time_step': self.time_step,
            'packet_count': self.packet_count,
            'delivered_count': self.delivered_count,
            'dropped_count': self.dropped_count,
            'delivery_rate': self.delivered_count / max(1, self.packet_count),
            'avg_latency': np.mean([l.latency for l in self.topology.links.values()]),
            'avg_packet_loss': np.mean([l.packet_loss for l in self.topology.links.values()]),
            'avg_congestion': np.mean([l.congestion for l in self.topology.links.values()]),
        }
