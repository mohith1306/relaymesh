package node

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type NodeID string

type NodeConfig struct {
	ID            NodeID
	Address       string
	Port          uint16
	ListenAddress string
	MaxPeers      int
	HeartbeatInterval time.Duration
	PeerTimeout   time.Duration
}

type Node struct {
	id       NodeID
	config   *NodeConfig
	state    *StateMachine
	logger   *slog.Logger
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	startTime time.Time
}

func New(config *NodeConfig, logger *slog.Logger) *Node {
	return &Node{
		id:     config.ID,
		config: config,
		state:  NewStateMachine(),
		logger: logger,
	}
}

func (n *Node) ID() NodeID {
	return n.id
}

func (n *Node) State() State {
	return n.state.Current()
}

func (n *Node) Config() *NodeConfig {
	return n.config
}

func (n *Node) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	n.cancel = cancel
	n.startTime = time.Now()

	n.logger.Info("starting node",
		"node_id", n.id,
		"address", n.config.Address,
		"port", n.config.Port,
	)

	if err := n.state.Transition(StateDiscovering); err != nil {
		return fmt.Errorf("failed to transition to discovering state: %w", err)
	}

	n.wg.Add(1)
	go n.run(ctx)

	return nil
}

func (n *Node) Stop() error {
	n.logger.Info("stopping node", "node_id", n.id)

	if err := n.state.Transition(StateShutdown); err != nil {
		n.logger.Warn("failed to transition to shutdown state", "error", err)
	}

	if n.cancel != nil {
		n.cancel()
	}

	n.wg.Wait()
	n.logger.Info("node stopped", "node_id", n.id, "uptime", time.Since(n.startTime))
	return nil
}

func (n *Node) run(ctx context.Context) {
	defer n.wg.Done()

	ticker := time.NewTicker(n.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.heartbeat()
		}
	}
}

func (n *Node) heartbeat() {
	n.logger.Debug("heartbeat", "node_id", n.id, "state", n.state.Current())
}

func (n *Node) OnTransition(callback func(from, to State)) {
	n.state.OnTransition(callback)
}
