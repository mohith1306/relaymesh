"""Network topology simulation for RL training."""
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, field
import random


@dataclass
class Node:
    id: str
    address: str = "127.0.0.1"
    port: int = 0
    is_gateway: bool = False
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    queue_size: int = 0


@dataclass
class Link:
    source: str
    target: str
    latency: float = 10.0
    bandwidth: float = 100.0
    packet_loss: float = 0.01
    congestion: float = 0.0
    is_active: bool = True


@dataclass
class NetworkTopology:
    nodes: Dict[str, Node] = field(default_factory=dict)
    links: Dict[Tuple[str, str], Link] = field(default_factory=dict)

    def add_node(self, node: Node):
        self.nodes[node.id] = node

    def add_link(self, link: Link):
        self.links[(link.source, link.target)] = link
        self.links[(link.target, link.source)] = Link(
            source=link.target,
            target=link.source,
            latency=link.latency,
            bandwidth=link.bandwidth,
            packet_loss=link.packet_loss,
            congestion=link.congestion,
            is_active=link.is_active,
        )

    def remove_link(self, source: str, target: str):
        if (source, target) in self.links:
            del self.links[(source, target)]
        if (target, source) in self.links:
            del self.links[(target, source)]

    def get_neighbors(self, node_id: str) -> List[str]:
        neighbors = []
        for (src, tgt), link in self.links.items():
            if src == node_id and link.is_active:
                neighbors.append(tgt)
        return neighbors

    def get_link(self, source: str, target: str) -> Optional[Link]:
        return self.links.get((source, target))

    def get_all_paths(self, source: str, destination: str, max_hops: int = 10) -> List[List[str]]:
        paths = []
        self._dfs(source, destination, [source], paths, max_hops)
        return paths

    def _dfs(self, current: str, target: str, path: List[str],
             paths: List[List[str]], max_hops: int):
        if len(path) > max_hops:
            return
        if current == target and len(path) > 1:
            paths.append(path[:])
            return
        for neighbor in self.get_neighbors(current):
            if neighbor not in path:
                path.append(neighbor)
                self._dfs(neighbor, target, path, paths, max_hops)
                path.pop()

    def calculate_path_cost(self, path: List[str]) -> float:
        if len(path) < 2:
            return 0.0
        total_cost = 0.0
        for i in range(len(path) - 1):
            link = self.get_link(path[i], path[i + 1])
            if link and link.is_active:
                cost = link.latency + (link.packet_loss * 100) + (link.congestion * 50)
                total_cost += cost
            else:
                return float('inf')
        return total_cost

    def to_adjacency_matrix(self) -> np.ndarray:
        node_ids = sorted(self.nodes.keys())
        n = len(node_ids)
        idx = {nid: i for i, nid in enumerate(node_ids)}
        matrix = np.full((n, n), float('inf'))
        np.fill_diagonal(matrix, 0)
        for (src, tgt), link in self.links.items():
            if link.is_active:
                i, j = idx[src], idx[tgt]
                matrix[i][j] = link.latency + (link.packet_loss * 100)
        return matrix

    def get_state_vector(self, source: str, destination: str) -> np.ndarray:
        features = []
        neighbors = self.get_neighbors(source)
        features.append(len(neighbors))
        features.append(len(self.nodes))
        features.append(len([l for l in self.links.values() if l.is_active]))

        if neighbors:
            avg_latency = np.mean([self.get_link(source, n).latency for n in neighbors if self.get_link(source, n)])
            avg_loss = np.mean([self.get_link(source, n).packet_loss for n in neighbors if self.get_link(source, n)])
            avg_bw = np.mean([self.get_link(source, n).bandwidth for n in neighbors if self.get_link(source, n)])
        else:
            avg_latency, avg_loss, avg_bw = 0, 0, 0

        features.extend([avg_latency, avg_loss, avg_bw])

        paths = self.get_all_paths(source, destination, max_hops=5)
        features.append(len(paths))
        if paths:
            best_cost = min(self.calculate_path_cost(p) for p in paths)
            features.append(best_cost)
        else:
            features.append(float('inf'))

        while len(features) < 10:
            features.append(0.0)

        return np.array(features[:10], dtype=np.float32)


def create_mesh_topology(num_nodes: int = 5, connectivity: float = 0.6) -> NetworkTopology:
    topology = NetworkTopology()
    for i in range(num_nodes):
        is_gw = (i == num_nodes - 1)
        node = Node(
            id=f"node-{chr(65 + i)}",
            address="127.0.0.1",
            port=9000 + i,
            is_gateway=is_gw,
        )
        topology.add_node(node)

    for i in range(num_nodes):
        for j in range(i + 1, num_nodes):
            if random.random() < connectivity or j == i + 1:
                link = Link(
                    source=f"node-{chr(65 + i)}",
                    target=f"node-{chr(65 + j)}",
                    latency=random.uniform(5, 30),
                    bandwidth=random.uniform(50, 100),
                    packet_loss=random.uniform(0, 0.05),
                )
                topology.add_link(link)

    return topology


def create_linear_topology(num_nodes: int = 5) -> NetworkTopology:
    topology = NetworkTopology()
    for i in range(num_nodes):
        is_gw = (i == num_nodes - 1)
        node = Node(
            id=f"node-{chr(65 + i)}",
            address="127.0.0.1",
            port=9000 + i,
            is_gateway=is_gw,
        )
        topology.add_node(node)

    for i in range(num_nodes - 1):
        link = Link(
            source=f"node-{chr(65 + i)}",
            target=f"node-{chr(65 + i + 1)}",
            latency=10 + i * 5,
            bandwidth=100 - i * 10,
            packet_loss=0.01 * (i + 1),
        )
        topology.add_link(link)

    return topology
