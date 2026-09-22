package web

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/relaymesh/relaymesh/internal/node"
	"github.com/relaymesh/relaymesh/internal/routing"
	"github.com/relaymesh/relaymesh/internal/telemetry"
)

type Dashboard struct {
	nodes      map[node.NodeID]*NodeInfo
	routers    map[node.NodeID]*routing.Router
	collectors map[node.NodeID]*telemetry.MetricsCollector
	logger     *slog.Logger
	mu         sync.RWMutex
	hostname   string
	localIP    string
}

type NodeInfo struct {
	ID         node.NodeID  `json:"id"`
	Address    string       `json:"address"`
	Port       uint16       `json:"port"`
	State      string       `json:"state"`
	Peers      []string     `json:"peers"`
	Uptime     string       `json:"uptime"`
	StartTime  time.Time    `json:"-"`
	DeviceName string       `json:"device_name"`
	IsLocal    bool         `json:"is_local"`
}

type NetworkState struct {
	Nodes     []NodeInfo          `json:"nodes"`
	Links     []LinkInfo          `json:"links"`
	Metrics   map[string]Metrics  `json:"metrics"`
	Timestamp time.Time           `json:"timestamp"`
}

type LinkInfo struct {
	Source    string  `json:"source"`
	Target    string  `json:"target"`
	Latency   float64 `json:"latency"`
	Bandwidth float64 `json:"bandwidth"`
	PacketLoss float64 `json:"packet_loss"`
	Active    bool    `json:"active"`
}

type Metrics struct {
	Latency    float64 `json:"latency"`
	PacketLoss float64 `json:"packet_loss"`
	Bandwidth  float64 `json:"bandwidth"`
	Congestion float64 `json:"congestion"`
	HopCount   uint32  `json:"hop_count"`
}

func NewDashboard(logger *slog.Logger) *Dashboard {
	hostname, _ := os.Hostname()
	localIP := getLocalIP()

	return &Dashboard{
		nodes:      make(map[node.NodeID]*NodeInfo),
		routers:    make(map[node.NodeID]*routing.Router),
		collectors: make(map[node.NodeID]*telemetry.MetricsCollector),
		logger:     logger,
		hostname:   hostname,
		localIP:    localIP,
	}
}

func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "0.0.0.0"
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return "0.0.0.0"
}

func (d *Dashboard) RegisterNode(id node.NodeID, addr string, port uint16, router *routing.Router, collector *telemetry.MetricsCollector) {
	d.mu.Lock()
	defer d.mu.Unlock()

	isLocal := addr == d.localIP || addr == "0.0.0.0" || addr == "127.0.0.1"
	deviceName := string(id)
	if isLocal {
		deviceName = fmt.Sprintf("%s (This Device)", d.hostname)
	}

	if _, exists := d.nodes[id]; !exists {
		d.nodes[id] = &NodeInfo{
			ID:         id,
			Address:    addr,
			Port:       port,
			State:      "DISCOVERING",
			Peers:      []string{},
			StartTime:  time.Now(),
			DeviceName: deviceName,
			IsLocal:    isLocal,
		}
	}
	if router != nil {
		d.routers[id] = router
	}
	if collector != nil {
		d.collectors[id] = collector
	}
}

func (d *Dashboard) RegisterWebNode(id node.NodeID, deviceName string, isWeb bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.nodes[id]; !exists {
		d.nodes[id] = &NodeInfo{
			ID:         id,
			Address:    "web",
			Port:       0,
			State:      "CONNECTED",
			Peers:      []string{},
			StartTime:  time.Now(),
			DeviceName: deviceName,
			IsLocal:    false,
		}
	}
}

func (d *Dashboard) RemoveNode(id node.NodeID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.nodes, id)
	delete(d.routers, id)
	delete(d.collectors, id)
}

func (d *Dashboard) GetNode(id node.NodeID) *NodeInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.nodes[id]
}

func (d *Dashboard) UpdateNodeState(id node.NodeID, state string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if info, ok := d.nodes[id]; ok {
		info.State = state
	}
}

func (d *Dashboard) UpdatePeers(id node.NodeID, peers []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if info, ok := d.nodes[id]; ok {
		info.Peers = peers
	}
}

func (d *Dashboard) Start(addr string) error {
	http.HandleFunc("/", d.handleIndex)
	http.HandleFunc("/api/state", d.handleState)
	http.HandleFunc("/api/nodes", d.handleNodes)
	http.HandleFunc("/api/metrics", d.handleMetrics)
	http.HandleFunc("/api/topology", d.handleTopology)

	d.logger.Info("dashboard started", "addr", addr)
	return http.ListenAndServe(addr, nil)
}

func (d *Dashboard) handleIndex(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "web/static/index.html")
}

func (d *Dashboard) handleState(w http.ResponseWriter, r *http.Request) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	state := NetworkState{
		Nodes:     make([]NodeInfo, 0),
		Links:     make([]LinkInfo, 0),
		Metrics:   make(map[string]Metrics),
		Timestamp: time.Now(),
	}

	for _, info := range d.nodes {
		state.Nodes = append(state.Nodes, *info)
	}

	for id, router := range d.routers {
		for _, dest := range router.GetTable().GetAllDestinations() {
			if route, ok := router.GetRoute(dest); ok {
				for i := 0; i < len(route.Path)-1; i++ {
					link, exists := router.GetGraph().GetLink(route.Path[i], route.Path[i+1])
					if exists {
						state.Links = append(state.Links, LinkInfo{
							Source:     string(route.Path[i]),
							Target:     string(route.Path[i+1]),
							Latency:    link.Latency,
							Bandwidth:  link.Bandwidth,
							PacketLoss: link.PacketLoss,
							Active:     link.IsActive,
						})
					}
				}
			}
		}

		if collector, ok := d.collectors[id]; ok {
			m := collector.GetMetrics()
			state.Metrics[string(id)] = Metrics{
				Latency:    m.Latency,
				PacketLoss: m.PacketLoss,
				Bandwidth:  m.Bandwidth,
				Congestion: m.Congestion,
				HopCount:   m.HopCount,
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(state)
}

func (d *Dashboard) handleNodes(w http.ResponseWriter, r *http.Request) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	nodes := make([]NodeInfo, 0)
	for _, info := range d.nodes {
		info.Uptime = time.Since(info.StartTime).Round(time.Second).String()
		nodes = append(nodes, *info)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(nodes)
}

func (d *Dashboard) handleMetrics(w http.ResponseWriter, r *http.Request) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	metrics := make(map[string]Metrics)
	for id, collector := range d.collectors {
		m := collector.GetMetrics()
		metrics[string(id)] = Metrics{
			Latency:    m.Latency,
			PacketLoss: m.PacketLoss,
			Bandwidth:  m.Bandwidth,
			Congestion: m.Congestion,
			HopCount:   m.HopCount,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

func (d *Dashboard) handleTopology(w http.ResponseWriter, r *http.Request) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	type TopologyResponse struct {
		Nodes []NodeInfo `json:"nodes"`
		Edges []LinkInfo `json:"edges"`
	}

	resp := TopologyResponse{
		Nodes: make([]NodeInfo, 0),
		Edges: make([]LinkInfo, 0),
	}

	for _, info := range d.nodes {
		resp.Nodes = append(resp.Nodes, *info)
	}

	seen := make(map[string]bool)
	for _, router := range d.routers {
		for _, dest := range router.GetTable().GetAllDestinations() {
			if route, ok := router.GetRoute(dest); ok {
				for i := 0; i < len(route.Path)-1; i++ {
					key := fmt.Sprintf("%s-%s", route.Path[i], route.Path[i+1])
					if !seen[key] {
						seen[key] = true
						link, exists := router.GetGraph().GetLink(route.Path[i], route.Path[i+1])
						if exists {
							resp.Edges = append(resp.Edges, LinkInfo{
								Source:     string(route.Path[i]),
								Target:     string(route.Path[i+1]),
								Latency:    link.Latency,
								Bandwidth:  link.Bandwidth,
								PacketLoss: link.PacketLoss,
								Active:     link.IsActive,
							})
						}
					}
				}
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
