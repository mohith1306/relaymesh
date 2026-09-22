package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const relayIndexHTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>RelayMesh - Join Network</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; }
        .header { background: #1e293b; padding: 1rem; border-bottom: 1px solid #334155; text-align: center; }
        .header h1 { color: #38bdf8; font-size: 1.25rem; }
        .main { padding: 1rem; max-width: 500px; margin: 0 auto; }
        .card { background: #1e293b; border-radius: 0.5rem; border: 1px solid #334155; padding: 1rem; margin-bottom: 1rem; }
        .card h2 { color: #38bdf8; font-size: 0.875rem; margin-bottom: 0.75rem; }
        .form-group { margin-bottom: 1rem; }
        .form-group label { display: block; color: #94a3b8; font-size: 0.75rem; margin-bottom: 0.25rem; }
        .form-group input { width: 100%; padding: 0.625rem; background: #0f172a; border: 1px solid #334155; border-radius: 0.375rem; color: #e2e8f0; font-size: 0.875rem; }
        .btn { width: 100%; padding: 0.625rem; border: none; border-radius: 0.375rem; font-size: 0.875rem; font-weight: 500; cursor: pointer; }
        .btn-primary { background: #38bdf8; color: #0f172a; }
        .btn-primary:hover { background: #7dd3fc; }
        .btn-danger { background: #ef4444; color: white; margin-top: 0.5rem; }
        .btn:disabled { opacity: 0.5; cursor: not-allowed; }
        .status { padding: 0.5rem; border-radius: 0.375rem; text-align: center; font-size: 0.75rem; margin-top: 0.5rem; }
        .status.connected { background: #065f46; color: #34d399; }
        .status.error { background: #7f1d1d; color: #fca5a5; }
        .status.info { background: #1e3a5f; color: #60a5fa; }
        .peer-list { max-height: 200px; overflow-y: auto; }
        .peer-item { display: flex; justify-content: space-between; align-items: center; padding: 0.5rem; background: #0f172a; border-radius: 0.25rem; margin-bottom: 0.25rem; font-size: 0.875rem; }
        .peer-name { font-weight: 500; }
        .peer-badge { font-size: 0.625rem; padding: 0.125rem 0.375rem; border-radius: 0.25rem; background: #22c55e; color: #0f172a; }
        .empty { color: #64748b; text-align: center; padding: 1rem; font-size: 0.875rem; }
        .your-device { background: #052e16 !important; border: 1px solid #22c55e; }
    </style>
</head>
<body>
    <div class="header">
        <h1>RelayMesh</h1>
    </div>

    <div class="main" id="loginView">
        <div class="card">
            <h2>Join the Mesh Network</h2>
            <div class="form-group">
                <label>Your Device Name</label>
                <input type="text" id="deviceName" placeholder="e.g., John's Phone" />
            </div>
            <button class="btn btn-primary" id="joinBtn" onclick="connect()">Join Network</button>
            <div id="statusMsg"></div>
        </div>
    </div>

    <div class="main" id="meshView" style="display: none;">
        <div class="card">
            <h2>Your Device</h2>
            <div class="peer-item your-device">
                <span class="peer-name" id="yourName">-</span>
                <span class="peer-badge">YOU</span>
            </div>
            <button class="btn btn-danger" onclick="disconnect()">Leave Network</button>
        </div>

        <div class="card">
            <h2>Online Devices (<span id="peerCount">0</span>)</h2>
            <div class="peer-list" id="peerList">
                <div class="empty">Waiting for others to join...</div>
            </div>
        </div>
    </div>

    <script>
        var ws = null;
        var nodeId = null;
        var deviceName = '';

        function generateId() {
            return 'web-' + Math.random().toString(36).substr(2, 9);
        }

        function connect() {
            deviceName = document.getElementById('deviceName').value.trim();
            if (!deviceName) {
                showStatus('Enter your device name', 'error');
                return;
            }

            nodeId = generateId();
            
            // Use wss:// for HTTPS pages, ws:// for HTTP
            var protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            var wsUrl = protocol + '//' + window.location.host + '/ws';

            showStatus('Connecting...', 'info');

            ws = new WebSocket(wsUrl);

            ws.onopen = function() {
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
                if (msg.type === 'peer_list') {
                    showMeshView(msg.payload || []);
                }
                if (msg.type === 'heartbeat') {
                    ws.send(JSON.stringify({ type: 'heartbeat' }));
                }
            };

            ws.onclose = function() {
                showLoginView();
                showStatus('Disconnected', 'error');
            };

            ws.onerror = function() {
                showStatus('Connection failed', 'error');
            };
        }

        function disconnect() {
            if (ws) ws.close();
        }

        function showStatus(msg, type) {
            var el = document.getElementById('statusMsg');
            el.innerHTML = '<div class="status ' + type + '">' + msg + '</div>';
        }

        function showMeshView(peers) {
            document.getElementById('loginView').style.display = 'none';
            document.getElementById('meshView').style.display = 'block';
            document.getElementById('yourName').textContent = deviceName;
            updatePeers(peers);
        }

        function showLoginView() {
            document.getElementById('loginView').style.display = 'block';
            document.getElementById('meshView').style.display = 'none';
        }

        function updatePeers(peers) {
            document.getElementById('peerCount').textContent = peers.length;
            var list = document.getElementById('peerList');

            if (peers.length === 0) {
                list.innerHTML = '<div class="empty">Waiting for others to join...</div>';
                return;
            }

            list.innerHTML = peers.map(function(p) {
                return '<div class="peer-item"><span class="peer-name">' + p.device_name + '</span><span class="peer-badge">ONLINE</span></div>';
            }).join('');
        }
    </script>
</body>
</html>`

type Client struct {
	Conn       *websocket.Conn
	ID         string
	DeviceName string
	IsWeb      bool
	LastSeen   time.Time
	mu         sync.Mutex
}

type Message struct {
	Type      string          `json:"type"`
	NodeID    string          `json:"node_id,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Timestamp time.Time       `json:"timestamp"`
}

type RelayServer struct {
	clients    map[*websocket.Conn]*Client
	clientsMu  sync.RWMutex
	broadcast  chan []byte
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func NewRelayServer() *RelayServer {
	return &RelayServer{
		clients:   make(map[*websocket.Conn]*Client),
		broadcast: make(chan []byte, 256),
	}
}

func (s *RelayServer) Start(addr string) {
	http.HandleFunc("/ws", s.handleWebSocket)
	http.HandleFunc("/", s.handleIndex)
	http.HandleFunc("/health", s.handleHealth)

	go s.heartbeatLoop()
	go s.cleanupLoop()

	log.Printf("Relay server starting on %s", addr)
	log.Printf("WebSocket endpoint: ws://%s/ws", addr)
	log.Printf("Web node page: http://%s/", addr)

	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal("Server failed:", err)
	}
}

func (s *RelayServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(relayIndexHTML))
}

func (s *RelayServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.clientsMu.RLock()
	count := len(s.clients)
	s.clientsMu.RUnlock()

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "ok",
		"clients": count,
		"time":    time.Now(),
	})
}

func (s *RelayServer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade failed: %v", err)
		return
	}

	client := &Client{
		Conn:     conn,
		LastSeen: time.Now(),
		IsWeb:    true,
	}

	s.clientsMu.Lock()
	s.clients[conn] = client
	s.clientsMu.Unlock()

	log.Printf("Client connected from %s (total: %d)", r.RemoteAddr, len(s.clients))

	defer func() {
		s.clientsMu.Lock()
		delete(s.clients, conn)
		s.clientsMu.Unlock()
		conn.Close()

		if client.ID != "" {
			s.broadcastPeerList()
			log.Printf("Client disconnected: %s %s (total: %d)", client.ID, client.DeviceName, len(s.clients))
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

func (s *RelayServer) handleMessage(client *Client, rawMsg []byte) {
	var msg Message
	if err := json.Unmarshal(rawMsg, &msg); err != nil {
		return
	}

	switch msg.Type {
	case "register":
		s.handleRegister(client, msg)
	case "heartbeat":
		client.LastSeen = time.Now()
	case "relay":
		s.handleRelay(client, msg)
	case "get_peers":
		s.sendPeerList(client)
	}
}

func (s *RelayServer) handleRegister(client *Client, msg Message) {
	var payload struct {
		NodeID     string `json:"node_id"`
		DeviceName string `json:"device_name"`
	}
	json.Unmarshal(msg.Payload, &payload)

	client.ID = payload.NodeID
	client.DeviceName = payload.DeviceName
	client.LastSeen = time.Now()

	log.Printf("Node registered: %s (%s)", payload.NodeID, payload.DeviceName)

	s.sendPeerList(client)
	s.broadcastPeerList()
}

func (s *RelayServer) handleRelay(client *Client, msg Message) {
	var payload struct {
		Target string `json:"target"`
		Data   string `json:"data"`
	}
	json.Unmarshal(msg.Payload, &payload)

	data, _ := json.Marshal(msg)

	if payload.Target == "all" {
		s.broadcastMessage(client, data)
	} else {
		s.clientsMu.RLock()
		for _, c := range s.clients {
			if c.ID == payload.Target {
				c.Conn.WriteMessage(websocket.TextMessage, data)
				break
			}
		}
		s.clientsMu.RUnlock()
	}
}

func (s *RelayServer) broadcastMessage(sender *Client, rawMsg []byte) {
	s.clientsMu.RLock()
	defer s.clientsMu.RUnlock()

	for _, client := range s.clients {
		if client.ID != sender.ID {
			client.Conn.WriteMessage(websocket.TextMessage, rawMsg)
		}
	}
}

func (s *RelayServer) sendPeerList(client *Client) {
	s.clientsMu.RLock()
	peers := make([]map[string]interface{}, 0)
	for _, c := range s.clients {
		if c.ID != client.ID && c.ID != "" {
			peers = append(peers, map[string]interface{}{
				"id":          c.ID,
				"device_name": c.DeviceName,
				"is_web":      c.IsWeb,
			})
		}
	}
	s.clientsMu.RUnlock()

	payload, _ := json.Marshal(peers)
	msg := Message{
		Type:      "peer_list",
		Payload:   payload,
		Timestamp: time.Now(),
	}
	data, _ := json.Marshal(msg)
	client.Conn.WriteMessage(websocket.TextMessage, data)
}

func (s *RelayServer) broadcastPeerList() {
	s.clientsMu.RLock()
	clients := make([]*Client, 0)
	for _, c := range s.clients {
		if c.ID != "" {
			clients = append(clients, c)
		}
	}
	s.clientsMu.RUnlock()

	for _, client := range clients {
		s.sendPeerList(client)
	}
}

func (s *RelayServer) heartbeatLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.clientsMu.RLock()
		for conn := range s.clients {
			msg := Message{
				Type:      "heartbeat",
				Timestamp: time.Now(),
			}
			data, _ := json.Marshal(msg)
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				conn.Close()
				delete(s.clients, conn)
			}
		}
		s.clientsMu.RUnlock()
	}
}

func (s *RelayServer) cleanupLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.clientsMu.Lock()
		for conn, client := range s.clients {
			if time.Since(client.LastSeen) > 60*time.Second {
				log.Printf("Cleaning up stale client: %s", client.ID)
				conn.Close()
				delete(s.clients, conn)
			}
		}
		s.clientsMu.Unlock()
	}
}

func main() {
	port := "8080"
	if p := fmt.Sprintf("%s", port); p != "" {
		port = p
	}

	relay := NewRelayServer()
	relay.Start(":" + port)
}
