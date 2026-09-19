package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/relaymesh/relaymesh/internal/node"
)

type Config struct {
	Node    NodeSettings    `json:"node"`
	Log     LogSettings     `json:"log"`
	Network NetworkSettings `json:"network"`
}

type NodeSettings struct {
	ID                string        `json:"id"`
	Address           string        `json:"address"`
	Port              uint16        `json:"port"`
	MaxPeers          int           `json:"max_peers"`
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`
	PeerTimeout       time.Duration `json:"peer_timeout"`
}

type LogSettings struct {
	Level  string `json:"level"`
	Format string `json:"format"`
}

type NetworkSettings struct {
	ListenAddress string `json:"listen_address"`
}

func DefaultConfig() *Config {
	return &Config{
		Node: NodeSettings{
			ID:                "node-1",
			Address:           "0.0.0.0",
			Port:              9000,
			MaxPeers:          10,
			HeartbeatInterval: 5 * time.Second,
			PeerTimeout:       30 * time.Second,
		},
		Log: LogSettings{
			Level:  "info",
			Format: "text",
		},
		Network: NetworkSettings{
			ListenAddress: "0.0.0.0:9000",
		},
	}
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	config := DefaultConfig()
	if err := json.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return config, nil
}

func (c *Config) Save(path string) error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

func (c *Config) ToNodeConfig() *node.NodeConfig {
	return &node.NodeConfig{
		ID:                node.NodeID(c.Node.ID),
		Address:           c.Node.Address,
		Port:              c.Node.Port,
		ListenAddress:     c.Network.ListenAddress,
		MaxPeers:          c.Node.MaxPeers,
		HeartbeatInterval: c.Node.HeartbeatInterval,
		PeerTimeout:       c.Node.PeerTimeout,
	}
}
