package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/relaymesh/relaymesh/internal/ai"
	"github.com/relaymesh/relaymesh/internal/config"
	"github.com/relaymesh/relaymesh/internal/discovery"
	"github.com/relaymesh/relaymesh/internal/node"
	"github.com/relaymesh/relaymesh/internal/routing"
	"github.com/relaymesh/relaymesh/internal/telemetry"
	"github.com/relaymesh/relaymesh/web"
)

func main() {
	configPath := flag.String("config", "", "Path to config file")
	nodeID := flag.String("node-id", "", "Node ID (overrides config)")
	port := flag.Int("port", 0, "Port number (overrides config)")
	addr := flag.String("address", "", "Listen address (overrides config)")
	logLevel := flag.String("log-level", "", "Log level (debug, info, warn, error)")
	generateConfig := flag.Bool("generate-config", false, "Generate default config file")
	dashboardAddr := flag.String("dashboard", "", "Dashboard address (e.g., :8080)")
	aiAddr := flag.String("ai", "", "AI service address (e.g., localhost:50051)")
	flag.Parse()

	if *generateConfig {
		cfg := config.DefaultConfig()
		if err := cfg.Save("relaymesh.json"); err != nil {
			fmt.Fprintf(os.Stderr, "Error generating config: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Generated relaymesh.json")
		return
	}

	cfg := config.DefaultConfig()
	if *configPath != "" {
		var err error
		cfg, err = config.Load(*configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
			os.Exit(1)
		}
	}

	if *nodeID != "" {
		cfg.Node.ID = *nodeID
	}
	if *port > 0 {
		cfg.Node.Port = uint16(*port)
	}
	if *addr != "" {
		cfg.Node.Address = *addr
	}

	level := slog.LevelInfo
	switch *logLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))

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

	router := routing.New(nodeConfig.ID, logger.With("component", "routing"))
	collector := telemetry.NewCollector(nodeConfig.ID)

	var aiRouter *ai.AIRouter
	if *aiAddr != "" {
		aiClient := ai.NewClient(*aiAddr, logger.With("component", "ai"))
		if err := aiClient.Connect(ctx); err != nil {
			logger.Warn("failed to connect to AI service, running without AI", "error", err)
		} else {
			aiRouter = ai.NewAIRouter(router, aiClient, logger.With("component", "ai-router"))
			aiRouter.SetAIAvailable(true)
			aiRouter.SetCollector(nodeConfig.ID, collector)
			logger.Info("AI routing enabled", "ai_addr", *aiAddr)
		}
	}

	dashboard := web.NewDashboard(logger.With("component", "dashboard"))
	dashboard.RegisterNode(nodeConfig.ID, nodeConfig.Address, nodeConfig.Port, router, collector)

	if *dashboardAddr != "" {
		go func() {
			if err := dashboard.Start(*dashboardAddr); err != nil {
				logger.Error("dashboard error", "error", err)
			}
		}()
		logger.Info("dashboard started", "addr", *dashboardAddr)
	}

	_ = aiRouter

	if err := n.Start(ctx); err != nil {
		logger.Error("failed to start node", "error", err)
		os.Exit(1)
	}

	logger.Info("node running",
		"node_id", n.ID(),
		"state", n.State(),
		"port", nodeConfig.Port,
	)
	<-ctx.Done()

	disc.Stop()
	if err := n.Stop(); err != nil {
		logger.Error("error stopping node", "error", err)
	}

	logger.Info("relaymesh shut down")
}
