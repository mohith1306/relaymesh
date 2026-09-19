package node

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

type Manager struct {
	nodes  map[NodeID]*Node
	mu     sync.RWMutex
	logger *slog.Logger
}

func NewManager(logger *slog.Logger) *Manager {
	return &Manager{
		nodes:  make(map[NodeID]*Node),
		logger: logger,
	}
}

func (m *Manager) CreateNode(config *NodeConfig) (*Node, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.nodes[config.ID]; exists {
		return nil, fmt.Errorf("node %s already exists", config.ID)
	}

	node := New(config, m.logger.With("component", "node", "node_id", config.ID))
	m.nodes[config.ID] = node

	m.logger.Info("node created", "node_id", config.ID)
	return node, nil
}

func (m *Manager) GetNode(id NodeID) (*Node, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	node, exists := m.nodes[id]
	return node, exists
}

func (m *Manager) RemoveNode(id NodeID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	node, exists := m.nodes[id]
	if !exists {
		return fmt.Errorf("node %s not found", id)
	}

	if err := node.Stop(); err != nil {
		m.logger.Warn("error stopping node", "node_id", id, "error", err)
	}

	delete(m.nodes, id)
	m.logger.Info("node removed", "node_id", id)
	return nil
}

func (m *Manager) StartAll(ctx context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for id, node := range m.nodes {
		if err := node.Start(ctx); err != nil {
			return fmt.Errorf("failed to start node %s: %w", id, err)
		}
	}

	m.logger.Info("all nodes started", "count", len(m.nodes))
	return nil
}

func (m *Manager) StopAll() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for id, node := range m.nodes {
		if err := node.Stop(); err != nil {
			m.logger.Warn("error stopping node", "node_id", id, "error", err)
		}
	}

	m.logger.Info("all nodes stopped")
}

func (m *Manager) ListNodes() []NodeID {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids := make([]NodeID, 0, len(m.nodes))
	for id := range m.nodes {
		ids = append(ids, id)
	}
	return ids
}
