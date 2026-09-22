"""Traffic pattern generator for simulation."""
import random
from typing import List, Tuple
from dataclasses import dataclass


@dataclass
class TrafficFlow:
    source: str
    destination: str
    rate: float
    packet_size: int
    priority: int = 0


class TrafficGenerator:
    """Generates realistic traffic patterns for RL training."""

    def __init__(self, nodes: List[str]):
        self.nodes = nodes
        self.flows: List[TrafficFlow] = []

    def generate_random_flows(self, num_flows: int = 5) -> List[TrafficFlow]:
        self.flows = []
        for _ in range(num_flows):
            source = random.choice(self.nodes)
            destination = random.choice([n for n in self.nodes if n != source])
            flow = TrafficFlow(
                source=source,
                destination=destination,
                rate=random.uniform(1, 100),
                packet_size=random.choice([64, 128, 256, 512, 1024]),
                priority=random.randint(0, 3),
            )
            self.flows.append(flow)
        return self.flows

    def generate_mesh_traffic(self) -> List[TrafficFlow]:
        self.flows = []
        gateway = [n for n in self.nodes if "gateway" in n.lower()]
        if not gateway:
            gateway = [self.nodes[-1]]

        for node in self.nodes:
            if node not in gateway:
                flow = TrafficFlow(
                    source=node,
                    destination=gateway[0],
                    rate=random.uniform(10, 50),
                    packet_size=256,
                    priority=1,
                )
                self.flows.append(flow)
        return self.flows

    def get_traffic_matrix(self) -> List[List[float]]:
        n = len(self.nodes)
        matrix = [[0.0] * n for _ in range(n)]
        node_idx = {node: i for i, node in enumerate(self.nodes)}

        for flow in self.flows:
            i = node_idx[flow.source]
            j = node_idx[flow.destination]
            matrix[i][j] = flow.rate

        return matrix
