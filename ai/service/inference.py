"""RL inference engine for route optimization."""
import numpy as np
from typing import List, Dict, Optional

try:
    import torch
    _TORCH_AVAILABLE = True
except ImportError:
    torch = None
    _TORCH_AVAILABLE = False

from .feature_processor import FeatureProcessor


class RouteInference:
    """RL-based route inference engine."""

    # Loss dominates path scoring: a 0.4 loss link must outweigh a
    # few ms of base-cost difference so adaptation actually switches.
    LOSS_WEIGHT = 100.0

    def __init__(self):
        self.feature_processor = FeatureProcessor()
        self.model = None
        self.is_loaded = False
        self.model_kind = None  # 'torch' | 'qtable'
        self.q_table = {}
        self.action_dim = 0
        
    def load_model(self, model_path: str) -> bool:
        """Load a trained model: torch checkpoint (.pt) or Q-table JSON."""
        if model_path.endswith('.json'):
            return self._load_qtable(model_path)
        if not _TORCH_AVAILABLE:
            print("torch not installed; staying on heuristic recommendations")
            self.is_loaded = False
            return False
        try:
            self.model = torch.load(model_path)
            self.model.eval()
            self.model_kind = 'torch'
            self.is_loaded = True
            return True
        except Exception as e:
            print(f"Failed to load model: {e}")
            self.is_loaded = False
            return False

    def _load_qtable(self, model_path: str) -> bool:
        """Load a tabular Q-learning policy saved by train.py."""
        import json
        try:
            with open(model_path) as f:
                data = json.load(f)
            self.q_table = {k: np.array(v, dtype=np.float32)
                            for k, v in data['q_table'].items()}
            if self.q_table:
                self.action_dim = len(next(iter(self.q_table.values())))
            self.model_kind = 'qtable'
            self.is_loaded = True
            return True
        except Exception as e:
            print(f"Failed to load Q-table: {e}")
            self.is_loaded = False
            return False

    @staticmethod
    def _state_key(features: np.ndarray) -> str:
        # Must match QLearningAgent.get_state_key in training/train.py.
        n_neighbors = int(round(float(features[0]))) if len(features) > 0 else 0
        n_active = int(round(float(features[2]))) if len(features) > 2 else 0
        n_paths = int(round(float(features[6]))) if len(features) > 6 else 0
        cost = float(features[7]) if len(features) > 7 else 0.0
        cost_bucket = int(cost // 10) if cost != float('inf') else 999
        return f"{n_neighbors},{n_active},{n_paths},{cost_bucket}"
    
    def get_recommendation(self, state: Dict, candidates: List[str],
                           candidate_paths: Optional[List[Dict]] = None) -> Dict:
        """Get route recommendation for given candidates.

        When Go supplies candidate_paths (loop-free topology paths it
        enumerated), score those — topology knowledge stays in Go and
        the model only ranks the options it is handed. Legacy callers
        without candidate paths use the destination-candidate flow.
        """
        if candidate_paths:
            return self._path_recommendation(state, candidate_paths)
        if not self.is_loaded:
            return self._heuristic_recommendation(state, candidates)
        if self.model_kind == 'qtable':
            return self._qtable_recommendation(state, candidates)
        
        features = self.feature_processor.process_network_state(state)
        
        with torch.no_grad():
            q_values = self.model(torch.FloatTensor(features))
        
        best_idx = torch.argmax(q_values).item()
        
        return {
            'destination': candidates[best_idx] if candidates else None,
            'path': [state.get('node_id', ''), candidates[best_idx]] if candidates else [],
            'confidence': float(q_values[best_idx]),
            'score': float(q_values[best_idx]),
        }

    def _qtable_recommendation(self, state: Dict, candidates: List[str]) -> Dict:
        """Greedy action from a loaded Q-table. Candidates map to actions
        by position; falls back to heuristic on any shape mismatch."""
        if not candidates or len(candidates) != self.action_dim:
            return self._heuristic_recommendation(state, candidates)
        features = self.feature_processor.process_network_state(state)
        q_values = self.q_table.get(self._state_key(features))
        if q_values is None:
            return self._heuristic_recommendation(state, candidates)
        best_idx = int(np.argmax(q_values))
        source = state.get('node_id', '')
        return {
            'destination': candidates[best_idx],
            'path': [source, candidates[best_idx]],
            'confidence': float(q_values[best_idx]),
            'score': float(q_values[best_idx]),
        }
    
    def _path_recommendation(self, state: Dict, candidate_paths: List[Dict]) -> Dict:
        """Score Go-enumerated candidate paths with live per-link health.

        score = -(base_cost + first_hop_rtt + loss * LOSS_WEIGHT)

        base_cost comes from Go's loss-blind Dijkstra, the RTT/loss
        from the source's own probe telemetry — so a degrading link is
        invisible to deterministic routing but flips this score. A
        matching Q-table state raises confidence (RL-confirmed).
        """
        peers = {p.get('peer_id'): p for p in state.get('peers', [])}
        source = state.get('node_id', '')

        options = []
        for cand in candidate_paths or []:
            path = list(cand.get('path') or [])
            if len(path) < 2 or path[0] != source:
                continue
            base = float(cand.get('base_cost', 0.0) or 0.0)
            hop_metrics = peers.get(path[1], {}) or {}
            rtt = float(hop_metrics.get('latency', 0.0) or 0.0)
            loss = float(hop_metrics.get('packet_loss', 0.0) or 0.0)
            score = -(base + rtt + loss * self.LOSS_WEIGHT)
            options.append({
                'path': path,
                'destination': path[-1],
                'score': score,
            })

        if not options:
            return {'destination': None, 'path': [], 'confidence': 0.0, 'score': 0.0}

        options.sort(key=lambda o: o['score'], reverse=True)
        best = options[0]

        if len(options) == 1:
            confidence = 0.6
        else:
            gap = best['score'] - options[1]['score']
            confidence = 0.6 + min(0.35, gap / 10.0)

        if self.is_loaded and self.model_kind == 'qtable':
            features = self.feature_processor.process_network_state(state)
            if self._state_key(features) in self.q_table:
                confidence = max(confidence, 0.8)

        return {
            'destination': best['destination'],
            'path': best['path'],
            'confidence': float(confidence),
            'score': float(best['score']),
        }

    def _heuristic_recommendation(self, state: Dict, candidates: List[str]) -> Dict:
        """Fallback heuristic recommendation."""
        if not candidates:
            return {'destination': None, 'path': [], 'confidence': 0.0, 'score': 0.0}
        
        metrics = state.get('metrics', {})
        latency = metrics.get('latency', 1.0)
        packet_loss = metrics.get('packet_loss', 0.0)
        
        best_score = float('-inf')
        best_dest = candidates[0]
        
        for dest in candidates:
            score = -latency - (packet_loss * 100)
            if score > best_score:
                best_score = score
                best_dest = dest
        
        source = state.get('node_id', '')
        return {
            'destination': best_dest,
            'path': [source, best_dest],
            'confidence': 0.5,
            'score': best_score,
        }
    
    def update_from_feedback(self, route_id: str, reward: float):
        """Update model from feedback."""
        pass
