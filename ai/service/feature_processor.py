"""Feature processor for RL inference."""
import numpy as np
from typing import List, Dict


class FeatureProcessor:
    """Process network state features for RL model input."""
    
    def __init__(self, feature_dim: int = 10):
        self.feature_dim = feature_dim
        self.scaler = None
        
    def process_network_state(self, state: Dict) -> np.ndarray:
        """Convert network state to feature vector."""
        features = []
        
        if 'metrics' in state:
            metrics = state['metrics']
            features.extend([
                metrics.get('latency', 0.0),
                metrics.get('packet_loss', 0.0),
                metrics.get('bandwidth', 0.0),
                metrics.get('jitter', 0.0),
                metrics.get('congestion', 0.0),
                metrics.get('hop_count', 0),
                metrics.get('stability', 1.0),
                metrics.get('queue_size', 0),
                metrics.get('cpu_usage', 0.0),
                metrics.get('memory_usage', 0.0),
            ])
        
        while len(features) < self.feature_dim:
            features.append(0.0)
        
        return np.array(features[:self.feature_dim], dtype=np.float32)
    
    def process_peer_features(self, peers: List[Dict]) -> np.ndarray:
        """Process peer metrics into feature matrix."""
        peer_features = []
        
        for peer in peers[:10]:  # Max 10 peers
            features = [
                peer.get('latency', 0.0),
                peer.get('packet_loss', 0.0),
                peer.get('bandwidth', 0.0),
            ]
            peer_features.append(features)
        
        while len(peer_features) < 10:
            peer_features.append([0.0, 0.0, 0.0])
        
        return np.array(peer_features, dtype=np.float32)
    
    def normalize(self, features: np.ndarray) -> np.ndarray:
        """Normalize features to [0, 1] range."""
        if self.scaler is None:
            return features
        
        return self.scaler.transform(features.reshape(1, -1)).flatten()
    
    def extract_route_features(self, route: Dict, state: Dict) -> np.ndarray:
        """Extract features for a specific route."""
        route_features = [
            route.get('cost', 0.0),
            route.get('hop_count', 0),
            len(route.get('path', [])),
        ]
        
        state_features = self.process_network_state(state)
        
        return np.concatenate([route_features, state_features])
