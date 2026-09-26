package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/relaymesh/relaymesh/internal/ai"
	"github.com/relaymesh/relaymesh/internal/config"
	"github.com/relaymesh/relaymesh/internal/discovery"
	"github.com/relaymesh/relaymesh/internal/mesh"
	"github.com/relaymesh/relaymesh/internal/node"
	"github.com/relaymesh/relaymesh/internal/telemetry"
	"github.com/relaymesh/relaymesh/web"
)

func main() {	configPath := flag.String("config", "", "Path to config file")
	nodeID := flag.String("node-id", "", "Node ID (overrides config)")
	port := flag.Int("port", 0, "Discovery port (overrides config)")
	dataPort := flag.Int("data-port", 0, "Mesh data port (default: discovery port + 10000)")
	addr := flag.String("address", "", "Listen address (overrides config)")
	logLevel := flag.String("log-level", "", "Log level (debug, info, warn, error)")
	generateConfig := flag.Bool("generate-config", false, "Generate default config file")
	dashboardAddr := flag.String("dashboard", "", "Dashboard address (e.g., :8080 or 0.0.0.0:8080)")
	webNodeAddr := flag.String("web-node", "", "Web node server address (e.g., :8082)")
	aiAddr := flag.String("ai", "", "AI service address (e.g., localhost:50051)")
	sendTo := flag.String("send-to", "", "Phase 1 test: destination node ID to send messages to")
	sendMsg := flag.String("send-msg", "hello-mesh", "Phase 1 test: message payload to send")
	sendInterval := flag.Duration("send-interval", 5*time.Second, "Phase 1 test: interval between test messages")
	knownPorts := flag.String("known-ports", "9001,9002,9003,9004,9005", "Comma-separated discovery ports to probe for peers")
	psk := flag.String("psk", "", "Preshared key for data-plane packet authentication (all peers must match)")
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

	localIP := discovery.GetLocalIP()
	logger.Info("local network", "ip", localIP)

	nodeConfig := cfg.ToNodeConfig()
	if nodeConfig.Address == "0.0.0.0" {
		nodeConfig.Address = localIP
	}

	dPort := uint16(*dataPort)
	if dPort == 0 {
		dPort = nodeConfig.Port + 10000
	}

	knownDiscoveryPorts, err := parsePortList(*knownPorts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid --known-ports: %v\n", err)
		os.Exit(1)
	}

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

	// Phase 1 v1: single wired mesh data plane.
	// Discovery -> Router -> Forwarder -> Transport, all connected.
	meshNode := mesh.New(mesh.Config{
		NodeID:              nodeConfig.ID,
		Address:             localIP,
		DiscoveryPort:       nodeConfig.Port,
		DataPort:            dPort,
		KnownDiscoveryPorts: knownDiscoveryPorts,
		HeartbeatInterval:   nodeConfig.HeartbeatInterval,
		PeerTimeout:         nodeConfig.PeerTimeout,
		PSK:                 []byte(*psk),
	}, logger.With("component", "mesh"))
	if err := meshNode.Start(ctx); err != nil {
		logger.Error("failed to start mesh node", "error", err)
		os.Exit(1)
	}

	router := meshNode.Router()
	collector := telemetry.NewCollector(nodeConfig.ID)

	var aiRouter *ai.AIRouter
	if *aiAddr != "" {
		aiClient := ai.NewClient(*aiAddr, logger.With("component", "ai"))
		if err := aiClient.Connect(ctx); err != nil {
			logger.Warn("failed to connect to AI service, running without AI", "error", err)
		} else {
			aiRouter = ai.NewAIRouter(nodeConfig.ID, router, aiClient, logger.With("component", "ai-router"))
			aiRouter.SetAIAvailable(true)
			aiRouter.SetCollector(nodeConfig.ID, collector)
			meshNode.SetAdvisor(aiRouter)
			logger.Info("AI routing enabled", "ai_addr", *aiAddr)
		}
	}

	dashboard := web.NewDashboard(logger.With("component", "dashboard"))
	dashboard.RegisterNode(nodeConfig.ID, localIP, nodeConfig.Port, router, collector)

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				peers := meshNode.Discovery().Snapshot()
				peerIDs := make([]string, 0, len(peers))
				for _, p := range peers {
					peerIDs = append(peerIDs, string(p.ID))
					if dashboard.GetNode(p.ID) == nil {
						dashboard.RegisterNode(p.ID, p.Address, p.Port, nil, nil)
					}
				}
				dashboard.UpdatePeers(nodeConfig.ID, peerIDs)
				dashboard.UpdateNodeState(nodeConfig.ID, n.State().String())
				meshNode.SyncTelemetry(collector)
				if aiRouter != nil {
					ids := make([]node.NodeID, 0, len(peers)+1)
					ids = append(ids, nodeConfig.ID)
					for _, p := range peers {
						ids = append(ids, p.ID)
					}
					aiRouter.SetPeers(ids)
				}
			}
		}
	}()

	// Log packets delivered to this node and report mesh stats.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case d := <-meshNode.Delivered():
				logger.Info("mesh packet delivered",
					"source", d.Source,
					"payload", string(d.Payload),
					"seq", d.Sequence,
					"packet_id", d.ID,
				)
			}
		}
	}()
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				st := meshNode.Stats()
				logger.Info("mesh stats",
					"peers", st.PeerCount,
					"routes", st.Routes,
					"sent", st.Sent,
					"forwarded", st.Forwarded,
					"delivered", st.Delivered,
					"dropped", st.Dropped,
				)
			}
		}
	}()

	// Optional Phase 1 probe: periodically send a test message to a peer.
	if *sendTo != "" {
		target := node.NodeID(*sendTo)
		go func() {
			ticker := time.NewTicker(*sendInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					msg := fmt.Sprintf("%s seq=%d t=%s", *sendMsg, time.Now().Unix(), nodeConfig.ID)
					if err := meshNode.Send(target, []byte(msg)); err != nil {
						logger.Warn("mesh send failed", "dest", target, "error", err)
					} else {
						logger.Info("mesh send ok", "dest", target)
					}
				}
			}
		}()
	}

	if *dashboardAddr != "" {
		dashAddr := *dashboardAddr
		if dashAddr[0] == ':' {
			dashAddr = "0.0.0.0" + dashAddr
		}
		go func() {
			if err := dashboard.Start(dashAddr); err != nil {
				logger.Error("dashboard error", "error", err)
			}
		}()
		logger.Info("dashboard started", "addr", fmt.Sprintf("http://%s:%s", localIP, *dashboardAddr))
	}

	if *webNodeAddr != "" {
		wsAddr := *webNodeAddr
		if wsAddr[0] == ':' {
			wsAddr = "0.0.0.0" + wsAddr
		}
		webNodeServer := web.NewWebNodeServer(dashboard, logger.With("component", "webnode"))
		go func() {
			if err := webNodeServer.Start(wsAddr); err != nil {
				logger.Error("web node server error", "error", err)
			}
		}()
		logger.Info("web node server started",
			"addr", fmt.Sprintf("ws://%s:%s", localIP, *webNodeAddr),
			"node_page", fmt.Sprintf("http://%s:%s/node", localIP, *webNodeAddr),
		)
	}

	_ = aiRouter

	if err := n.Start(ctx); err != nil {
		logger.Error("failed to start node", "error", err)
		os.Exit(1)
	}

	logger.Info("node running",
		"node_id", n.ID(),
		"state", n.State(),
		"ip", localIP,
		"discovery_port", nodeConfig.Port,
		"data_port", dPort,
	)
	<-ctx.Done()

	_ = meshNode.Stop()
	if err := n.Stop(); err != nil {
		logger.Error("error stopping node", "error", err)
	}

	logger.Info("relaymesh shut down")
}

func parsePortList(s string) ([]uint16, error) {
	var ports []uint16
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		n, err := strconv.Atoi(part)
		if err != nil || n < 1 || n > 65535 {
			return nil, fmt.Errorf("invalid port %q", part)
		}
		ports = append(ports, uint16(n))
	}
	if len(ports) == 0 {
		return nil, fmt.Errorf("no ports specified")
	}
	return ports, nil
}
