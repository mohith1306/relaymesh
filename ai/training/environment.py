"""RL Environment for route optimization."""
import numpy as np
from typing import Dict, Tuple, Optional
from ..simulation.topology import NetworkTopology, create_mesh_topology
from ..simulation.network import NetworkSimulator


class RelayMeshEnv:
    """OpenAI Gym-style environment for RelayMesh routing."""

    def __init__(self, num_nodes: int = 5, max_hops: int = 10):
        self.num_nodes = num_nodes
        self.max_hops = max_hops
        self.node_ids = [f"node-{chr(65 + i)}" for i in range(num_nodes)]

        self.observation_space_dim = 10
        self.action_space_dim = num_nodes

        self.simulator: Optional[NetworkSimulator] = None
        self.source = "node-A"
        self.destination = f"node-{chr(65 + num_nodes - 1)}"
        self.current_path: list = []
        self.step_count = 0

    def reset(self) -> np.ndarray:
        topology = create_mesh_topology(self.num_nodes, connectivity=0.7)
        self.simulator = NetworkSimulator(topology)
        self.current_path = [self.source]
        self.step_count = 0
        return self.simulator.reset()

    def step(self, action: int) -> Tuple[np.ndarray, float, bool, Dict]:
        self.step_count += 1
        next_node = self.node_ids[action]

        self.current_path.append(next_node)

        state, reward, done, info = self.simulator.step(action)

        if next_node == self.destination:
            reward += 20.0
            done = True
            info['reached_destination'] = True
        elif self.step_count >= self.max_hops:
            reward -= 15.0
            done = True
            info['max_hops_exceeded'] = True
        elif next_node not in self.simulator.topology.get_neighbors(self.current_path[-2]):
            reward -= 10.0
            done = True
            info['invalid_move'] = True

        info['current_path'] = self.current_path.copy()
        return state, reward, done, info

    def get_valid_actions(self) -> list:
        if not self.simulator or not self.current_path:
            return list(range(self.num_nodes))

        current = self.current_path[-1]
        neighbors = self.simulator.topology.get_neighbors(current)
        return [self.node_ids.index(n) for n in neighbors if n in self.node_ids]

    def render(self):
        if self.simulator:
            metrics = self.simulator.get_metrics()
            print(f"Step: {self.step_count}")
            print(f"Path: {' -> '.join(self.current_path)}")
            print(f"Delivery Rate: {metrics['delivery_rate']:.2%}")
            print(f"Avg Latency: {metrics['avg_latency']:.2f}ms")
            print(f"Avg Loss: {metrics['avg_packet_loss']:.2%}")
