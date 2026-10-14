"""RL Training script for RelayMesh routing."""
import numpy as np
import json
import os
from typing import List, Dict
from .environment import RelayMeshEnv
from .reward import RewardCalculator, AdaptiveRewardCalculator


class QLearningAgent:
    """Simple Q-learning agent for route optimization."""

    def __init__(self, state_dim: int, action_dim: int,
                 learning_rate: float = 0.1,
                 discount_factor: float = 0.99,
                 epsilon: float = 1.0,
                 epsilon_decay: float = 0.995,
                 epsilon_min: float = 0.01):
        self.state_dim = state_dim
        self.action_dim = action_dim
        self.lr = learning_rate
        self.gamma = discount_factor
        self.epsilon = epsilon
        self.epsilon_decay = epsilon_decay
        self.epsilon_min = epsilon_min

        self.q_table: Dict[str, np.ndarray] = {}
        self.training_history: List[Dict] = []

    def get_state_key(self, state: np.ndarray) -> str:
        # Coarse key over STABLE features only: volatile latency/loss
        # would make every state unique and prevent any generalization.
        # Layout: [neighbors, nodes, active, latency, loss, bw, paths, cost, ..]
        n_neighbors = int(round(float(state[0]))) if len(state) > 0 else 0
        n_active = int(round(float(state[2]))) if len(state) > 2 else 0
        n_paths = int(round(float(state[6]))) if len(state) > 6 else 0
        cost = float(state[7]) if len(state) > 7 else 0.0
        cost_bucket = int(cost // 10) if cost != float('inf') else 999
        return f"{n_neighbors},{n_active},{n_paths},{cost_bucket}"

    def get_q_values(self, state_key: str) -> np.ndarray:
        if state_key not in self.q_table:
            self.q_table[state_key] = np.zeros(self.action_dim)
        return self.q_table[state_key]

    def choose_action(self, state: np.ndarray, valid_actions: List[int] = None) -> int:
        if np.random.random() < self.epsilon:
            if valid_actions:
                return np.random.choice(valid_actions)
            return np.random.randint(self.action_dim)

        state_key = self.get_state_key(state)
        q_values = self.get_q_values(state_key)

        if valid_actions:
            valid_q = [(a, q_values[a]) for a in valid_actions]
            return max(valid_q, key=lambda x: x[1])[0]

        return np.argmax(q_values)

    def learn(self, state: np.ndarray, action: int, reward: float,
              next_state: np.ndarray, done: bool):
        state_key = self.get_state_key(state)
        next_state_key = self.get_state_key(next_state)

        q_values = self.get_q_values(state_key)
        next_q_values = self.get_q_values(next_state_key)

        if done:
            target = reward
        else:
            target = reward + self.gamma * np.max(next_q_values)

        q_values[action] += self.lr * (target - q_values[action])

        self.epsilon = max(self.epsilon_min, self.epsilon * self.epsilon_decay)

    def save(self, path: str):
        data = {
            'q_table': {k: v.tolist() for k, v in self.q_table.items()},
            'epsilon': self.epsilon,
            'training_history': self.training_history,
        }
        with open(path, 'w') as f:
            json.dump(data, f)

    def load(self, path: str):
        with open(path, 'r') as f:
            data = json.load(f)
        self.q_table = {k: np.array(v) for k, v in data['q_table'].items()}
        self.epsilon = data.get('epsilon', self.epsilon_min)
        self.training_history = data.get('training_history', [])


class DQNAgent:
    """Deep Q-Network agent (simplified)."""

    def __init__(self, state_dim: int, action_dim: int,
                 hidden_dim: int = 64,
                 learning_rate: float = 0.001,
                 discount_factor: float = 0.99,
                 epsilon: float = 1.0,
                 epsilon_decay: float = 0.995,
                 epsilon_min: float = 0.01):
        self.state_dim = state_dim
        self.action_dim = action_dim
        self.gamma = discount_factor
        self.epsilon = epsilon
        self.epsilon_decay = epsilon_decay
        self.epsilon_min = epsilon_min

        try:
            import torch
            import torch.nn as nn
            self.use_torch = True

            class QNetwork(nn.Module):
                def __init__(self):
                    super().__init__()
                    self.fc1 = nn.Linear(state_dim, hidden_dim)
                    self.fc2 = nn.Linear(hidden_dim, hidden_dim)
                    self.fc3 = nn.Linear(hidden_dim, action_dim)

                def forward(self, x):
                    x = torch.relu(self.fc1(x))
                    x = torch.relu(self.fc2(x))
                    return self.fc3(x)

            self.q_network = QNetwork()
            self.target_network = QNetwork()
            self.target_network.load_state_dict(self.q_network.state_dict())
            self.optimizer = torch.optim.Adam(self.q_network.parameters(), lr=learning_rate)
            self.loss_fn = nn.MSELoss()
            self.memory: List = []
            self.batch_size = 32

        except ImportError:
            self.use_torch = False
            self.q_table: Dict[str, np.ndarray] = {}

        self.training_history: List[Dict] = []

    def get_state_key(self, state: np.ndarray) -> str:
        n_neighbors = int(round(float(state[0]))) if len(state) > 0 else 0
        n_active = int(round(float(state[2]))) if len(state) > 2 else 0
        n_paths = int(round(float(state[6]))) if len(state) > 6 else 0
        cost = float(state[7]) if len(state) > 7 else 0.0
        cost_bucket = int(cost // 10) if cost != float('inf') else 999
        return f"{n_neighbors},{n_active},{n_paths},{cost_bucket}"

    def choose_action(self, state: np.ndarray, valid_actions: List[int] = None) -> int:
        if np.random.random() < self.epsilon:
            if valid_actions:
                return np.random.choice(valid_actions)
            return np.random.randint(self.action_dim)

        if self.use_torch:
            import torch
            with torch.no_grad():
                state_tensor = torch.FloatTensor(state).unsqueeze(0)
                q_values = self.q_network(state_tensor).numpy()[0]
        else:
            state_key = self.get_state_key(state)
            if state_key not in self.q_table:
                self.q_table[state_key] = np.zeros(self.action_dim)
            q_values = self.q_table[state_key]

        if valid_actions:
            valid_q = [(a, q_values[a]) for a in valid_actions]
            return max(valid_q, key=lambda x: x[1])[0]

        return np.argmax(q_values)

    def learn(self, state: np.ndarray, action: int, reward: float,
              next_state: np.ndarray, done: bool):
        if self.use_torch:
            import torch
            self.memory.append((state, action, reward, next_state, done))

            if len(self.memory) < self.batch_size:
                return

            batch = np.random.choice(len(self.memory), self.batch_size, replace=False)
            states = torch.FloatTensor([self.memory[i][0] for i in batch])
            actions = torch.LongTensor([self.memory[i][1] for i in batch])
            rewards = torch.FloatTensor([self.memory[i][2] for i in batch])
            next_states = torch.FloatTensor([self.memory[i][3] for i in batch])
            dones = torch.FloatTensor([self.memory[i][4] for i in batch])

            current_q = self.q_network(states).gather(1, actions.unsqueeze(1))
            next_q = self.target_network(next_states).max(1)[0].detach()
            target_q = rewards + self.gamma * next_q * (1 - dones)

            loss = self.loss_fn(current_q.squeeze(), target_q)

            self.optimizer.zero_grad()
            loss.backward()
            self.optimizer.step()
        else:
            state_key = self.get_state_key(state)
            next_state_key = self.get_state_key(next_state)

            if state_key not in self.q_table:
                self.q_table[state_key] = np.zeros(self.action_dim)
            if next_state_key not in self.q_table:
                self.q_table[next_state_key] = np.zeros(self.action_dim)

            q_values = self.q_table[state_key]
            next_q_values = self.q_table[next_state_key]

            if done:
                target = reward
            else:
                target = reward + self.gamma * np.max(next_q_values)

            q_values[action] += 0.1 * (target - q_values[action])

        self.epsilon = max(self.epsilon_min, self.epsilon * self.epsilon_decay)

    def update_target_network(self):
        if self.use_torch:
            self.target_network.load_state_dict(self.q_network.state_dict())

    def save(self, path: str):
        if self.use_torch:
            import torch
            torch.save({
                'q_network': self.q_network.state_dict(),
                'epsilon': self.epsilon,
                'training_history': self.training_history,
            }, path)
        else:
            data = {
                'q_table': {k: v.tolist() for k, v in self.q_table.items()},
                'epsilon': self.epsilon,
                'training_history': self.training_history,
            }
            with open(path, 'w') as f:
                json.dump(data, f)

    def load(self, path: str):
        if self.use_torch:
            import torch
            checkpoint = torch.load(path)
            self.q_network.load_state_dict(checkpoint['q_network'])
            self.epsilon = checkpoint.get('epsilon', self.epsilon_min)
            self.training_history = checkpoint.get('training_history', [])
        else:
            with open(path, 'r') as f:
                data = json.load(f)
            self.q_table = {k: np.array(v) for k, v in data['q_table'].items()}
            self.epsilon = data.get('epsilon', self.epsilon_min)
            self.training_history = data.get('training_history', [])


def train(num_episodes: int = 1000, num_nodes: int = 5,
          agent_type: str = "dqn", save_path: str = "models/relaymesh_policy.pt"):
    """Train RL agent for route optimization."""
    env = RelayMeshEnv(num_nodes=num_nodes)
    reward_calc = AdaptiveRewardCalculator()

    state_dim = env.observation_space_dim
    action_dim = env.action_space_dim

    if agent_type == "dqn":
        agent = DQNAgent(state_dim, action_dim)
    else:
        agent = QLearningAgent(state_dim, action_dim)

    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    episode_rewards = []
    best_reward = float('-inf')

    print(f"Training {agent_type.upper()} agent for {num_episodes} episodes...")
    print(f"Network: {num_nodes} nodes")
    print(f"State dim: {state_dim}, Action dim: {action_dim}")
    print("-" * 50)

    for episode in range(num_episodes):
        state = env.reset()
        total_reward = 0
        done = False
        path = []

        while not done:
            valid_actions = env.get_valid_actions()
            action = agent.choose_action(state, valid_actions)
            next_state, reward, done, info = env.step(action)

            agent.learn(state, action, reward, next_state, done)

            state = next_state
            total_reward += reward
            path = info.get('current_path', [])

        episode_rewards.append(total_reward)
        avg_reward = np.mean(episode_rewards[-100:])

        if avg_reward > best_reward:
            best_reward = avg_reward
            agent.save(save_path)

        if (episode + 1) % 100 == 0:
            print(f"Episode {episode + 1}/{num_episodes} | "
                  f"Avg Reward: {avg_reward:.2f} | "
                  f"Epsilon: {agent.epsilon:.3f} | "
                  f"Best: {best_reward:.2f}")

        agent.training_history.append({
            'episode': episode,
            'reward': total_reward,
            'avg_reward': avg_reward,
            'epsilon': agent.epsilon,
            'path': path,
        })

        if agent_type == "dqn" and hasattr(agent, 'update_target_network'):
            if (episode + 1) % 10 == 0:
                agent.update_target_network()

    agent.save(save_path)
    print(f"\nTraining complete. Model saved to {save_path}")
    print(f"Best average reward: {best_reward:.2f}")

    return agent, episode_rewards


if __name__ == "__main__":
    train(num_episodes=1000, agent_type="dqn")
