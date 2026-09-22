package web

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/relaymesh/relaymesh/internal/node"
	"github.com/gorilla/websocket"
)

const nodePageHTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>RelayMesh Node</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; display: flex; flex-direction: column; }
        .header { background: #1e293b; padding: 1rem 2rem; border-bottom: 1px solid #334155; }
        .header h1 { font-size: 1.5rem; color: #38bdf8; }
        .header p { color: #94a3b8; font-size: 0.875rem; }
        .container { flex: 1; display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; padding: 1rem; max-width: 1400px; margin: 0 auto; width: 100%; }
        .card { background: #1e293b; border-radius: 0.5rem; border: 1px solid #334155; padding: 1rem; }
        .card h2 { font-size: 1rem; color: #38bdf8; margin-bottom: 0.75rem; border-bottom: 1px solid #334155; padding-bottom: 0.5rem; }
        .full-width { grid-column: 1 / -1; }
        .status-bar { display: flex; gap: 1rem; align-items: center; margin-top: 0.5rem; }
        .status-dot { width: 8px; height: 8px; border-radius: 50%; background: #ef4444; }
        .status-dot.connected { background: #22c55e; animation: pulse 2s infinite; }
        @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.5; } }
        .form-group { margin-bottom: 1rem; }
        .form-group label { display: block; color: #94a3b8; font-size: 0.875rem; margin-bottom: 0.25rem; }
        .form-group input { width: 100%; padding: 0.5rem; background: #0f172a; border: 1px solid #334155; border-radius: 0.375rem; color: #e2e8f0; font-size: 0.875rem; }
        .btn { padding: 0.5rem 1rem; border-radius: 0.375rem; font-weight: 500; cursor: pointer; border: none; }
        .btn-primary { background: #38bdf8; color: #0f172a; }
        .btn-primary:hover { background: #7dd3fc; }
        .btn-danger { background: #ef4444; color: white; }
        .btn-danger:hover { background: #f87171; }
        .btn:disabled { opacity: 0.5; cursor: not-allowed; }
        .peer-list { display: flex; flex-direction: column; gap: 0.5rem; }
        .peer-item { display: flex; justify-content: space-between; align-items: center; padding: 0.75rem; background: #0f172a; border-radius: 0.375rem; border: 1px solid #334155; }
        .peer-name { font-weight: 600; color: #38bdf8; }
        .peer-status { padding: 0.25rem 0.5rem; border-radius: 0.25rem; font-size: 0.75rem; background: #065f46; color: #34d399; }
        .log { background: #0f172a; border-radius: 0.375rem; padding: 0.5rem; max-height: 200px; overflow-y: auto; font-family: monospace; font-size: 0.75rem; border: 1px solid #334155; }
        .log-entry { padding: 0.25rem 0; border-bottom: 1px solid #1e293b; }
        .log-entry.info { color: #38bdf8; }
        .log-entry.success { color: #22c55e; }
        .log-entry.error { color: #ef4444; }
        .message-box { margin-top: 1rem; }
        .message-box textarea { width: 100%; height: 80px; padding: 0.5rem; background: #0f172a; border: 1px solid #334155; border-radius: 0.375rem; color: #e2e8f0; font-size: 0.875rem; resize: vertical; }
    </style>
</head>
<body>
    <div class="header">
        <h1>RelayMesh Node</h1>
        <div class="status-bar">
            <div class="status-dot" id="statusDot"></div>
            <span id="statusText">Disconnected</span>
            <span id="nodeId" style="color: #64748b; font-size: 0.75rem;"></span>
        </div>
    </div>

    <div class="container">
        <div class="card">
            <h2>Connect to Network</h2>
            <div class="form-group">
                <label>Your Device Name</label>
                <input type="text" id="deviceName" placeholder="e.g., John's Phone" />
            </div>
            <div class="form-group">
                <label>Relay Server</label>
                <input type="text" id="serverAddr" placeholder="ws://your-relay-server:8080/ws" />
            </div>
            <div style="font-size: 0.75rem; color: #64748b; margin-bottom: 1rem;">
                Use a public relay server to connect across different networks.
                <br>Local: ws://192.168.31.219:8082/ws
                <br>Public: Deploy relay-server to cloud
            </div>
            <div style="display: flex; gap: 0.5rem;">
                <button class="btn btn-primary" id="connectBtn" onclick="connect()">Connect</button>
                <button class="btn btn-danger" id="disconnectBtn" onclick="disconnect()" disabled>Disconnect</button>
            </div>
        </div>

        <div class="card">
            <h2>Network Peers</h2>
            <div id="peerList" class="peer-list">
                <div style="color: #64748b;">Not connected</div>
            </div>
        </div>

        <div class="card full-width">
            <h2>Activity Log</h2>
            <div id="log" class="log"></div>
        </div>
    </div>

    <script>
        let ws = null;
        let nodeId = null;
        let peers = [];

        function generateId() {
            return 'web-' + Math.random().toString(36).substr(2, 9);
        }

        function addLog(msg, type = 'info') {
            const log = document.getElementById('log');
            const entry = document.createElement('div');
            entry.className = 'log-entry ' + type;
            entry.textContent = '[' + new Date().toLocaleTimeString() + '] ' + msg;
            log.insertBefore(entry, log.firstChild);
        }

        function updateStatus(connected) {
            const dot = document.getElementById('statusDot');
            const text = document.getElementById('statusText');
            const connectBtn = document.getElementById('connectBtn');
            const disconnectBtn = document.getElementById('disconnectBtn');

            if (connected) {
                dot.classList.add('connected');
                text.textContent = 'Connected';
                connectBtn.disabled = true;
                disconnectBtn.disabled = false;
            } else {
                dot.classList.remove('connected');
                text.textContent = 'Disconnected';
                connectBtn.disabled = false;
                disconnectBtn.disabled = true;
            }
        }

        function connect() {
            const deviceName = document.getElementById('deviceName').value || 'Anonymous Device';
            const addr = document.getElementById('serverAddr').value;

            if (!addr) {
                addLog('Please enter server address', 'error');
                return;
            }

            nodeId = generateId();
            addLog('Connecting as "' + deviceName + '" (' + nodeId + ')...');

            ws = new WebSocket(addr);

            ws.onopen = function() {
                addLog('Connected to network!', 'success');
                updateStatus(true);
                document.getElementById('nodeId').textContent = 'ID: ' + nodeId;

                ws.send(JSON.stringify({
                    type: 'register',
                    payload: {
                        node_id: nodeId,
                        device_name: deviceName
                    }
                }));
            };

            ws.onmessage = function(event) {
                var msg = JSON.parse(event.data);
                handleMessage(msg);
            };

            ws.onclose = function() {
                addLog('Disconnected from network', 'error');
                updateStatus(false);
                peers = [];
                renderPeers();
            };

            ws.onerror = function(error) {
                addLog('Connection error', 'error');
            };
        }

        function disconnect() {
            if (ws) {
                ws.close();
                ws = null;
            }
        }

        function handleMessage(msg) {
            switch (msg.type) {
                case 'peer_list':
                    peers = msg.payload || [];
                    renderPeers();
                    addLog('Peers updated: ' + peers.length + ' devices online');
                    break;
                case 'heartbeat':
                    break;
                case 'relay':
                    addLog('Message from ' + msg.node_id + ': ' + msg.payload, 'success');
                    break;
            }
        }

        function renderPeers() {
            var container = document.getElementById('peerList');
            if (!peers.length) {
                container.innerHTML = '<div style="color: #64748b;">No other peers online</div>';
                return;
            }

            container.innerHTML = peers.map(function(p) {
                return '<div class="peer-item"><div><span class="peer-name">' + p.device_name + '</span><span style="color: #64748b; font-size: 0.75rem; margin-left: 0.5rem;">' + p.id + '</span></div><span class="peer-status">' + (p.is_web ? 'Web' : 'Node') + '</span></div>';
            }).join('');
        }

        window.onload = function() {
            var host = window.location.hostname;
            document.getElementById('serverAddr').value = 'ws://' + host + ':8082/ws';
        };
    </script>
</body>
</html>`

type WSMessage struct {
	Type      string          `json:"type"`
	NodeID    node.NodeID     `json:"node_id,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Timestamp time.Time       `json:"timestamp"`
}

type PeerInfo struct {
	ID         node.NodeID `json:"id"`
	DeviceName string      `json:"device_name"`
	IsWeb      bool        `json:"is_web"`
}

type WebNodeServer struct {
	clients    map[*websocket.Conn]*WebClient
	clientsMu  sync.RWMutex
	dashboard  *Dashboard
	logger     *slog.Logger
	broadcast  chan []byte
}

type WebClient struct {
	Conn       *websocket.Conn
	NodeID     node.NodeID
	DeviceName string
	LastSeen   time.Time
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func NewWebNodeServer(dashboard *Dashboard, logger *slog.Logger) *WebNodeServer {
	return &WebNodeServer{
		clients:   make(map[*websocket.Conn]*WebClient),
		dashboard: dashboard,
		logger:    logger,
		broadcast: make(chan []byte, 256),
	}
}

func (s *WebNodeServer) Start(addr string) error {
	http.HandleFunc("/ws", s.handleWebSocket)
	http.HandleFunc("/node", s.handleNodePage)

	go s.broadcastLoop()

	s.logger.Info("web node server started", "addr", addr)
	return http.ListenAndServe(addr, nil)
}

func (s *WebNodeServer) handleNodePage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(nodePageHTML))
}

func (s *WebNodeServer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Error("websocket upgrade failed", "error", err)
		return
	}

	client := &WebClient{
		Conn:     conn,
		LastSeen: time.Now(),
	}

	s.clientsMu.Lock()
	s.clients[conn] = client
	s.clientsMu.Unlock()

	s.logger.Info("web client connected", "remote", r.RemoteAddr)

	defer func() {
		s.clientsMu.Lock()
		delete(s.clients, conn)
		s.clientsMu.Unlock()
		conn.Close()

		if client.NodeID != "" {
			s.dashboard.RemoveNode(client.NodeID)
			s.logger.Info("web client disconnected", "node_id", client.NodeID)
		}
	}()

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			return
		}

		s.handleMessage(client, message)
	}
}

func (s *WebNodeServer) handleMessage(client *WebClient, rawMsg []byte) {
	var msg WSMessage
	if err := json.Unmarshal(rawMsg, &msg); err != nil {
		return
	}

	switch msg.Type {
	case "register":
		s.handleRegister(client, msg)
	case "heartbeat":
		s.handleHeartbeat(client, msg)
	case "peer_list":
		s.handlePeerList(client, msg)
	case "relay":
		s.handleRelay(client, msg)
	}
}

func (s *WebNodeServer) handleRegister(client *WebClient, msg WSMessage) {
	var payload struct {
		NodeID     node.NodeID `json:"node_id"`
		DeviceName string      `json:"device_name"`
	}
	json.Unmarshal(msg.Payload, &payload)

	client.NodeID = payload.NodeID
	client.DeviceName = payload.DeviceName
	client.LastSeen = time.Now()

	s.dashboard.RegisterWebNode(payload.NodeID, payload.DeviceName, true)

	s.logger.Info("web node registered",
		"node_id", payload.NodeID,
		"device_name", payload.DeviceName,
	)

	// Send peer list to new node
	s.sendPeerList(client)
}

func (s *WebNodeServer) handleHeartbeat(client *WebClient, msg WSMessage) {
	client.LastSeen = time.Now()

	if client.NodeID != "" {
		s.dashboard.UpdateNodeState(client.NodeID, "CONNECTED")
	}
}

func (s *WebNodeServer) handlePeerList(client *WebClient, msg WSMessage) {
	s.sendPeerList(client)
}

func (s *WebNodeServer) handleRelay(client *WebClient, msg WSMessage) {
	var payload struct {
		Target node.NodeID `json:"target"`
		Data   []byte      `json:"data"`
	}
	json.Unmarshal(msg.Payload, &payload)

	s.clientsMu.RLock()
	for _, c := range s.clients {
		if c.NodeID == payload.Target {
			resp := WSMessage{
				Type:      "relay",
				NodeID:    client.NodeID,
				Payload:   msg.Payload,
				Timestamp: time.Now(),
			}
			data, _ := json.Marshal(resp)
			c.Conn.WriteMessage(websocket.TextMessage, data)
			break
		}
	}
	s.clientsMu.RUnlock()
}

func (s *WebNodeServer) sendPeerList(client *WebClient) {
	s.clientsMu.RLock()
	peers := make([]PeerInfo, 0)
	for _, c := range s.clients {
		if c.NodeID != client.NodeID && c.NodeID != "" {
			peers = append(peers, PeerInfo{
				ID:         c.NodeID,
				DeviceName: c.DeviceName,
				IsWeb:      true,
			})
		}
	}
	s.clientsMu.RUnlock()

	payload, _ := json.Marshal(peers)
	msg := WSMessage{
		Type:      "peer_list",
		Payload:   payload,
		Timestamp: time.Now(),
	}
	data, _ := json.Marshal(msg)
	client.Conn.WriteMessage(websocket.TextMessage, data)
}

func (s *WebNodeServer) broadcastLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.clientsMu.RLock()
		for conn, client := range s.clients {
			if client.NodeID != "" {
				msg := WSMessage{
					Type:      "heartbeat",
					NodeID:    client.NodeID,
					Timestamp: time.Now(),
				}
				data, _ := json.Marshal(msg)
				if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
					conn.Close()
					delete(s.clients, conn)
				}
			}
		}
		s.clientsMu.RUnlock()

		// Send updated peer lists
		s.clientsMu.RLock()
		for _, client := range s.clients {
			if client.NodeID != "" {
				s.sendPeerList(client)
			}
		}
		s.clientsMu.RUnlock()
	}
}

func (s *WebNodeServer) ClientCount() int {
	s.clientsMu.RLock()
	defer s.clientsMu.RUnlock()
	return len(s.clients)
}
