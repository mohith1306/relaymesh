package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/relaymesh/relaymesh/internal/config"
	"github.com/relaymesh/relaymesh/internal/discovery"
	"github.com/relaymesh/relaymesh/internal/node"
	"github.com/relaymesh/relaymesh/internal/transport"
	gw "github.com/relaymesh/relaymesh/internal/gateway"
)

func main() {
	configPath := flag.String("config", "", "Path to config file")
	nodeID := flag.String("node-id", "gateway-1", "Node ID")
	port := flag.Int("port", 9000, "Listen port")
	internetAddr := flag.String("internet-addr", "0.0.0.0:8080", "Internet-facing address")
	logLevel := flag.String("log-level", "info", "Log level")
	flag.Parse()

	level := slog.LevelInfo
	switch *logLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	cfg := config.DefaultConfig()
	if *configPath != "" {
		var err error
		cfg, err = config.Load(*configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
			os.Exit(1)
		}
	}

	cfg.Node.ID = *nodeID
	cfg.Node.Port = uint16(*port)
	nodeConfig := cfg.ToNodeConfig()

	mgr := node.NewManager(logger)
	n, err := mgr.CreateNode(nodeConfig)
	if err != nil {
		logger.Error("failed to create node", "error", err)
		os.Exit(1)
	}

	n.OnTransition(func(from, to node.State) {
		logger.Info("state transition", "from", from, "to", to)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.Info("received shutdown signal")
		cancel()
	}()

	discConfig := discovery.DiscoveryConfig{
		NodeID:      nodeConfig.ID,
		Address:     nodeConfig.Address,
		Port:        nodeConfig.Port,
		Interval:    nodeConfig.HeartbeatInterval,
		PeerTimeout: nodeConfig.PeerTimeout,
		KnownPorts:  []uint16{9001, 9002, 9003, 9004, 9005},
	}

	disc := discovery.New(discConfig, logger.With("component", "discovery"))
	if err := disc.Start(ctx); err != nil {
		logger.Error("failed to start discovery", "error", err)
		os.Exit(1)
	}

	trans := transport.NewUDP(nodeConfig.ID)
	if err := trans.Listen(fmt.Sprintf(":%d", nodeConfig.Port)); err != nil {
		logger.Error("failed to start transport", "error", err)
		os.Exit(1)
	}

	gateway := gw.NewGateway(nodeConfig.ID, trans, logger.With("component", "gateway"))
	if err := gateway.Start(*internetAddr); err != nil {
		logger.Error("failed to start gateway", "error", err)
		os.Exit(1)
	}

	if err := n.Start(ctx); err != nil {
		logger.Error("failed to start node", "error", err)
		os.Exit(1)
	}

	logger.Info("gateway node running", "node_id", n.ID(), "internet_addr", *internetAddr)
	<-ctx.Done()

	gateway.Stop()
	trans.Close()
	disc.Stop()
	n.Stop()

	logger.Info("gateway shut down")
}
