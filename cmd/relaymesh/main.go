package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/relaymesh/relaymesh/internal/ai"
	"github.com/relaymesh/relaymesh/internal/config"
	"github.com/relaymesh/relaymesh/internal/discovery"
	"github.com/relaymesh/relaymesh/internal/flow"
	"github.com/relaymesh/relaymesh/internal/identity"
	"github.com/relaymesh/relaymesh/internal/mesh"
	"github.com/relaymesh/relaymesh/internal/node"
	"github.com/relaymesh/relaymesh/internal/proxy"
	"github.com/relaymesh/relaymesh/internal/stun"
	"github.com/relaymesh/relaymesh/internal/telemetry"
	"github.com/relaymesh/relaymesh/web"
)

func main() {
	configPath := flag.String("config", "", "Path to config file")
	nodeID := flag.String("node-id", "", "Node ID (overrides config)")
	port := flag.Int("port", 0, "Discovery port (overrides config)")
	dataPort := flag.Int("data-port", 0, "Mesh data port (default: discovery port + 10000)")
	addr := flag.String("address", "", "Listen address (overrides config)")
	logLevel := flag.String("log-level", "", "Log level (debug, info, warn, error)")
	generateConfig := flag.Bool("generate-config", false, "Generate default config file")
	dashboardAddr := flag.String("dashboard", "", "Dashboard address (e.g., :8080 or 0.0.0.0:8080)")
	webNodeAddr := flag.String("web-node", "", "Web node server address (e.g., :8082)")
	aiAddr := flag.String("ai", "", "AI service address (e.g., localhost:50051)")
	aiTLS := flag.Bool("ai-tls", false, "Connect to AI service over TLS (e.g., Render :443)")
	sendTo := flag.String("send-to", "", "Phase 1 test: destination node ID to send messages to")
	sendMsg := flag.String("send-msg", "hello-mesh", "Phase 1 test: message payload to send")
	sendInterval := flag.Duration("send-interval", 5*time.Second, "Phase 1 test: interval between test messages")
	knownPorts := flag.String("known-ports", "9001,9002,9003,9004,9005", "Comma-separated discovery ports to probe for peers")
	psk := flag.String("psk", "", "Preshared key for data-plane packet authentication (all peers must match)")
	identityPath := flag.String("identity", "", "Path to Ed25519 identity file (created if missing)")
	encrypt := flag.Bool("encrypt", false, "Require authenticated encryption on the data plane (requires --identity)")
	proxyAddr := flag.String("proxy", "", "Local SOCKS5/HTTP proxy address (e.g., 127.0.0.1:1080) to ride the mesh via a gateway")
	gatewayID := flag.String("gateway", "", "Gateway node ID to route proxied traffic through (required with --proxy)")
	signalURL := flag.String("signal", "", "Signaling server URL (e.g., https://relaymesh-f93s.onrender.com) for cross-internet hole punching; falls back to WSS relay if punching fails")
	stunServer := flag.String("stun", "stun.l.google.com:19302", "STUN server for public-address discovery (used with --signal)")
	relayURL := flag.String("relay", "", "Relay-server WSS URL for data-plane fallback when hole punching fails (default: derived from --signal)")
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
	if *encrypt && *identityPath == "" {
		fmt.Fprintf(os.Stderr, "--encrypt requires --identity\n")
		os.Exit(1)
	}
	var ident *identity.Identity
	if *identityPath != "" {
		created := false
		var err error
		ident, created, err = identity.LoadOrCreate(*identityPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading identity: %v\n", err)
			os.Exit(1)
		}
		if created {
			logger.Info("generated new identity", "path", *identityPath,
				"fingerprint", identity.Fingerprint(ident.PublicKey()))
		} else {
			logger.Info("loaded identity", "path", *identityPath,
				"fingerprint", identity.Fingerprint(ident.PublicKey()))
		}
	}
	meshNode := mesh.New(mesh.Config{
		NodeID:              nodeConfig.ID,
		Address:             localIP,
		DiscoveryPort:       nodeConfig.Port,
		DataPort:            dPort,
		KnownDiscoveryPorts: knownDiscoveryPorts,
		HeartbeatInterval:   nodeConfig.HeartbeatInterval,
		PeerTimeout:         nodeConfig.PeerTimeout,
		PSK:                 []byte(*psk),
		Identity:            ident,
		Encrypt:             *encrypt,
	}, logger.With("component", "mesh"))
	if err := meshNode.Start(ctx); err != nil {
		logger.Error("failed to start mesh node", "error", err)
		os.Exit(1)
	}

	// Phase 14: cross-internet connectivity.
	// 1. STUN discovers our server-reflexive (public) UDP address.
	// 2. The signaling server (/relay-ws) exchanges those addresses
	//    between registered nodes.
	// 3. Nodes race UDP hole punches on the exchanged addresses.
	// 4. If punching fails (symmetric NAT/CGNAT), traffic falls back to
	//    the relay-data path over the same WSS connection.
	if *signalURL != "" {
		relayWSS := *relayURL
		if relayWSS == "" {
			relayWSS = deriveRelayWSS(*signalURL)
		}
		go runSignaling(ctx, logger, meshNode, nodeConfig.ID, *signalURL, relayWSS, *stunServer, dPort)
	}

	// Client-side SOCKS5/HTTP proxy: local apps dial it, traffic is
	// tunneled through the mesh to the gateway and out to the internet.
	var proxySrv *proxy.Server
	if *proxyAddr != "" {
		if *gatewayID == "" {
			logger.Error("--proxy requires --gateway <node-id>", "error", "missing gateway")
			os.Exit(1)
		}
		flowMgr := flow.NewManager(nodeConfig.ID, meshNode.Send, logger.With("component", "flow"))
		meshNode.AddPayloadHandler(flowMgr.HandlePacket)
		dialer := &flow.GatewayDialer{Mgr: flowMgr, Gw: node.NodeID(*gatewayID)}
		proxySrv = proxy.NewServer(*proxyAddr, dialer, logger.With("component", "proxy"))
		go func() {
			if err := proxySrv.ListenAndServe(ctx); err != nil {
				logger.Warn("proxy server stopped", "error", err)
			}
		}()
		logger.Info("proxy listening", "addr", *proxyAddr, "gateway", *gatewayID)
	}

	router := meshNode.Router()
	collector := telemetry.NewCollector(nodeConfig.ID)

	var aiRouter *ai.AIRouter
	if *aiAddr != "" {
		aiClient := ai.NewClient(*aiAddr, logger.With("component", "ai"))
		if *aiTLS {
			aiClient.EnableTLS()
		} // Generous deadline: cloud services (Render free tier) can take
		// up to ~60s to wake, and the connect attempt itself wakes them.
		logger.Info("connecting to AI service", "addr", *aiAddr, "tls", *aiTLS)
		connectCtx, connectCancel := context.WithTimeout(ctx, 90*time.Second)
		err := aiClient.Connect(connectCtx)
		connectCancel()
		if err != nil {
			logger.Warn("failed to connect to AI service, running without AI", "error", err)
			aiClient.Close()
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

	if proxySrv != nil {
		proxySrv.Close()
	}
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

// deriveRelayWSS turns a signaling base URL (e.g.
// "https://relaymesh-f93s.onrender.com") into the WSS URL of its
// /relay-ws endpoint.
func deriveRelayWSS(base string) string {
	base = strings.TrimSuffix(base, "/")
	switch {
	case strings.HasPrefix(base, "https://"):
		return "wss://" + strings.TrimPrefix(base, "https://") + "/relay-ws"
	case strings.HasPrefix(base, "http://"):
		return "ws://" + strings.TrimPrefix(base, "http://") + "/relay-ws"
	case strings.HasPrefix(base, "wss://") || strings.HasPrefix(base, "ws://"):
		return base + "/relay-ws"
	default:
		return "wss://" + base + "/relay-ws"
	}
}

// runSignaling drives Phase 14's cross-internet path:
//   - STUN discovers our server-reflexive (public) data-port.
//   - We register with the signaling server and exchange candidates
//     with other registered nodes.
//   - We keep refreshing our candidate so the server does not evict us.
//
// The mesh data plane itself keeps using UDPTransport; signaling only
// injects candidate addresses into discovery via discovery.AddCandidate
// (which MeshNode.sync polls). If punching fails, relay-data fallback
// is available via the relayWSS endpoint.
func runSignaling(ctx context.Context, logger *slog.Logger, meshNode *mesh.MeshNode, selfID node.NodeID, signalURL, relayWSS, stunServer string, dataPort uint16) {
	logger.Info("signaling starting", "signal", signalURL, "relay_wss", relayWSS, "stun", stunServer)

	addr, err := stun.PublicAddr(stunServer, "", 5*time.Second)
	if err != nil {
		logger.Warn("STUN discovery failed, proceeding without srflx candidate", "error", err)
	}

	// Candidate we advertise: our public (server-reflexive) data port,
	// which is what remote peers must UDP-hole-punch to.
	type candidate struct {
		IP       string `json:"ip"`
		Port     uint16 `json:"port"`
		DataPort uint16 `json:"data_port,omitempty"`
	}
	var cands []candidate
	if err == nil && addr.Port != 0 {
		cands = append(cands, candidate{
			IP:       addr.IP.String(),
			Port:     addr.Port,
			DataPort: dataPort,
		})
	}

	wsURL := relayWSS
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		logger.Warn("signaling WS dial failed, cross-internet disabled", "url", wsURL, "error", err)
		return
	}
	defer conn.Close()

	register := map[string]interface{}{
		"type":       "register",
		"node_id":    string(selfID),
		"candidates": cands,
	}
	if err := conn.WriteJSON(register); err != nil {
		logger.Warn("signaling register failed", "error", err)
		return
	}
	logger.Info("signaling registered", "url", wsURL, "candidates", len(cands))

	// Read loop: server pushes {"type":"signal","payload":{"peers":{...}}}
	// mapping other node IDs to their candidates.
	go func() {
		for {
			var msg struct {
				Type    string `json:"type"`
				Payload struct {
					Peers map[string][]candidate `json:"peers"`
				} `json:"payload"`
			}
			if err := conn.ReadJSON(&msg); err != nil {
				logger.Warn("signaling read error", "error", err)
				return
			}
			if msg.Type != "signal" {
				continue
			}
			for peerID, pcands := range msg.Payload.Peers {
				if len(pcands) == 0 {
					continue
				}
				// Inject the first candidate (most setups have one).
				c := pcands[0]
				dport := c.DataPort
				if dport == 0 {
					dport = c.Port
				}
				meshNode.Discovery().AddCandidate(node.NodeID(peerID), c.IP, c.Port, dport)
				logger.Info("added remote candidate via signaling",
					"peer", peerID, "ip", c.IP, "port", c.Port, "data_port", dport)

				// Phase 14: race a hole punch to the candidate's data
				// port. Because we punch from our listen socket (same
				// local port the peer will send data to), this opens our
				// NAT's outbound mapping so the peer's own simultaneous
				// punch (and reply) can land. If punching fails the
				// transport's readLoop simply never re-binds, and
				// cross-internet stays down — relay-data over WSS is the
				// fallback.
				punchAddr := net.JoinHostPort(c.IP, strconv.Itoa(int(dport)))
				if err := meshNode.Punch(punchAddr); err != nil {
					logger.Debug("punch failed", "addr", punchAddr, "error", err)
				} else {
					logger.Info("sent hole punch", "addr", punchAddr)
				}
			}
		}
	}()

	// Refresh loop keeps our candidate fresh on the server.
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refresh := map[string]interface{}{
				"type":       "signal",
				"node_id":    string(selfID),
				"candidates": cands,
			}
			if err := conn.WriteJSON(refresh); err != nil {
				logger.Warn("signaling refresh failed", "error", err)
				return
			}
		}
	}
}
