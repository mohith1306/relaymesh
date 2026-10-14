"""Evaluate learned RL routing against deterministic shortest-path.

Methodology: per trial, one random topology is generated and deep-copied
so both policies face identical starting conditions (same RNG seeds for
network drift). The deterministic baseline precomputes the cheapest path
upfront and follows it; the RL policy picks hop-by-hop greedily and can
route around failures that appear mid-episode.
"""
import copy
import random
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import numpy as np

from ai.training.train import QLearningAgent, train
from ai.training.environment import RelayMeshEnv


def run_baseline(env: RelayMeshEnv):
    """Follow the upfront cheapest path; return (delivered, hops, cost)."""
    topo = env.simulator.topology
    paths = topo.get_all_paths(env.source, env.destination, max_hops=10)
    if not paths:
        return False, 0, float('inf')
    best = min(paths, key=lambda p: topo.calculate_path_cost(p))
    cost = topo.calculate_path_cost(best)
    state = env.simulator.reset()
    delivered, hops = False, 0
    for node_id in best[1:]:
        action = env.node_ids.index(node_id)
        state, _, done, info = env.step(action)
        hops += 1
        if info.get('reached_destination'):
            delivered = True
            break
        if info.get('invalid_move') or info.get('max_hops_exceeded'):
            break
        if done:
            break
    return delivered, hops, cost


def run_rl(env: RelayMeshEnv, agent: QLearningAgent):
    """Greedy Q-policy hop-by-hop; return (delivered, hops)."""
    state = env._observe()
    delivered, hops = False, 0
    for _ in range(env.max_hops):
        valid = env.get_valid_actions()
        action = agent.choose_action(state, valid)
        state, _, done, info = env.step(action)
        hops += 1
        if info.get('reached_destination'):
            delivered = True
            break
        if info.get('invalid_move') or info.get('max_hops_exceeded'):
            break
        if done:
            break
    return delivered, hops


def evaluate(agent: QLearningAgent, trials: int = 20, num_nodes: int = 5,
             seed: int = 7):
    """Compare RL policy vs deterministic shortest-path."""
    agent.epsilon = 0.0  # pure greedy
    results = {'rl': [], 'base': []}
    for t in range(trials):
        env = RelayMeshEnv(num_nodes=num_nodes)
        random.seed(seed + t)
        np.random.seed(seed + t)
        env.reset()
        topo_copy = copy.deepcopy(env.simulator.topology)

        rl_env = RelayMeshEnv(num_nodes=num_nodes)
        random.seed(seed + t)
        np.random.seed(seed + t)
        rl_env.reset()
        rl_env.simulator.topology = topo_copy
        rl_env.current_path = [rl_env.source]
        rl_env.step_count = 0
        results['rl'].append(run_rl(rl_env, agent))

        base_env = RelayMeshEnv(num_nodes=num_nodes)
        random.seed(seed + t)
        np.random.seed(seed + t)
        base_env.reset()
        base_env.simulator.topology = copy.deepcopy(topo_copy)
        base_env.current_path = [base_env.source]
        base_env.step_count = 0
        d, h, _ = run_baseline(base_env)
        results['base'].append((d, h))

    def summarize(runs):
        n = len(runs)
        delivered = sum(1 for d, _ in runs if d)
        hops = [h for d, h in runs if d] or [0]
        return delivered / n, sum(hops) / len(hops)

    rl_rate, rl_hops = summarize(results['rl'])
    base_rate, base_hops = summarize(results['base'])
    print(f"trials={trials} nodes={num_nodes}")
    print(f"RL policy:      delivery={rl_rate:.0%} avg_hops={rl_hops:.2f}")
    print(f"Deterministic:  delivery={base_rate:.0%} avg_hops={base_hops:.2f}")
    return {'rl_rate': rl_rate, 'rl_hops': rl_hops,
            'base_rate': base_rate, 'base_hops': base_hops}


if __name__ == '__main__':
    agent, _ = train(num_episodes=150, num_nodes=5,
                     agent_type='qlearning',
                     save_path='/tmp/relaymesh_eval.json')
    evaluate(agent, trials=20, num_nodes=5)
