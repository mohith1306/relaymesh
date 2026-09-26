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

	"github.com/relaymesh/relaymesh/internal/config"
	"github.com/relaymesh/relaymesh/internal/discovery"
	"github.com/relaymesh/relaymesh/internal/mesh"
	"github.com/relaymesh/relaymesh/internal/node"
	gw "github.com/relaymesh/relaymesh/internal/gateway"
)

func main() {
	configPath := flag.String("config", "", "Path to config file")
	nodeID := flag.String("node-id", "gateway-1", "Node ID")
	port := flag.Int("port", 9000, "Discovery port")
	dataPort := flag.Int("data-port", 0, "Mesh data port (default: discovery port + 10000)")
	addr := flag.String("address", "", "Listen address (overrides config)")
	logLevel := flag.String("log-level", "info", "Log level")
	knownPorts := flag.String("known-ports", "9001,9002,9003,9004,9005", "Comma-separated discovery ports to probe for peers")
	psk := flag.String("psk", "", "Preshared key for data-plane packet authentication (all peers must match)")
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
	if *addr != "" {
		cfg.Node.Address = *addr
	}
	nodeConfig := cfg.ToNodeConfig()

	localIP := discovery.GetLocalIP()
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

	gateway := gw.New(mesh.Config{
		NodeID:              nodeConfig.ID,
		Address:             localIP,
		DiscoveryPort:       nodeConfig.Port,
		DataPort:            dPort,
		KnownDiscoveryPorts: knownDiscoveryPorts,
		HeartbeatInterval:   nodeConfig.HeartbeatInterval,
		PeerTimeout:         nodeConfig.PeerTimeout,
		PSK:                 []byte(*psk),
	}, logger.With("component", "gateway"))
	if err := gateway.Start(ctx); err != nil {
		logger.Error("failed to start gateway", "error", err)
		os.Exit(1)
	}

	// Log non-egress packets addressed to the gateway.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case d := <-gateway.Mesh().Delivered():
				logger.Info("gateway received mesh packet",
					"source", d.Source,
					"payload_size", len(d.Payload),
					"path", d.Path,
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
				st := gateway.Mesh().Stats()
				logger.Info("gateway mesh stats",
					"peers", st.PeerCount,
					"routes", st.Routes,
					"forwarded", st.Forwarded,
					"delivered", st.Delivered,
					"dropped", st.Dropped,
					"nat_flows", gateway.NAT().Count(),
				)
			}
		}
	}()

	if err := n.Start(ctx); err != nil {
		logger.Error("failed to start node", "error", err)
		os.Exit(1)
	}

	logger.Info("gateway node running",
		"node_id", n.ID(),
		"discovery_port", nodeConfig.Port,
		"data_port", dPort,
	)
	<-ctx.Done()

	_ = gateway.Stop()
	if err := n.Stop(); err != nil {
		logger.Error("error stopping node", "error", err)
	}

	logger.Info("gateway shut down")
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
