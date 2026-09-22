package main

import (
	"encoding/json"
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
    <title>RelayMesh - Mesh Network</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; }
        .header { background: #1e293b; padding: 0.75rem 1rem; border-bottom: 1px solid #334155; display: flex; justify-content: space-between; align-items: center; }
        .header h1 { color: #38bdf8; font-size: 1.125rem; }
        .header .status { display: flex; align-items: center; gap: 0.5rem; font-size: 0.75rem; }
        .header .dot { width: 8px; height: 8px; border-radius: 50%; background: #ef4444; }
        .header .dot.on { background: #22c55e; animation: pulse 2s infinite; }
        @keyframes pulse { 0%,100% { opacity:1; } 50% { opacity:0.5; } }
        .tabs { display: flex; background: #1e293b; border-bottom: 1px solid #334155; }
        .tab { flex: 1; padding: 0.625rem; text-align: center; cursor: pointer; font-size: 0.75rem; color: #64748b; border-bottom: 2px solid transparent; }
        .tab.active { color: #38bdf8; border-bottom-color: #38bdf8; }
        .content { padding: 0.75rem; max-width: 600px; margin: 0 auto; }
        .card { background: #1e293b; border-radius: 0.5rem; border: 1px solid #334155; padding: 0.75rem; margin-bottom: 0.75rem; }
        .card h2 { color: #38bdf8; font-size: 0.8125rem; margin-bottom: 0.5rem; }
        .form-group { margin-bottom: 0.75rem; }
        .form-group label { display: block; color: #94a3b8; font-size: 0.75rem; margin-bottom: 0.25rem; }
        .form-group input, .form-group textarea, .form-group select { width: 100%; padding: 0.5rem; background: #0f172a; border: 1px solid #334155; border-radius: 0.375rem; color: #e2e8f0; font-size: 0.875rem; }
        .form-group textarea { height: 60px; resize: none; }
        .btn { padding: 0.5rem 1rem; border: none; border-radius: 0.375rem; font-size: 0.8125rem; font-weight: 500; cursor: pointer; }
        .btn-primary { background: #38bdf8; color: #0f172a; width: 100%; }
        .btn-primary:hover { background: #7dd3fc; }
        .btn-danger { background: #ef4444; color: white; width: 100%; margin-top: 0.5rem; }
        .btn-send { background: #22c55e; color: #0f172a; }
        .btn:disabled { opacity: 0.5; cursor: not-allowed; }
        .status-msg { padding: 0.375rem; border-radius: 0.25rem; text-align: center; font-size: 0.75rem; margin-top: 0.5rem; }
        .status-msg.ok { background: #065f46; color: #34d399; }
        .status-msg.err { background: #7f1d1d; color: #fca5a5; }
        .status-msg.info { background: #1e3a5f; color: #60a5fa; }
        .peer-list { max-height: 150px; overflow-y: auto; }
        .peer-item { display: flex; justify-content: space-between; align-items: center; padding: 0.5rem; background: #0f172a; border-radius: 0.25rem; margin-bottom: 0.25rem; font-size: 0.8125rem; cursor: pointer; border: 1px solid transparent; }
        .peer-item:hover { border-color: #38bdf8; }
        .peer-item.you { background: #052e16; border: 1px solid #22c55e; }
        .peer-item.selected { border-color: #f59e0b; background: #1e3a5f; }
        .badge { font-size: 0.625rem; padding: 0.125rem 0.375rem; border-radius: 0.25rem; background: #22c55e; color: #0f172a; font-weight: 600; }
        .badge.web { background: #38bdf8; }
        .badge.relay { background: #f59e0b; color: #0f172a; }
        .empty { color: #64748b; text-align: center; padding: 1rem; font-size: 0.8125rem; }
        canvas { width: 100%; height: 200px; background: #0f172a; border-radius: 0.375rem; border: 1px solid #334155; }
        .chat { max-height: 300px; overflow-y: auto; margin-bottom: 0.5rem; }
        .chat-msg { padding: 0.5rem; margin-bottom: 0.25rem; border-radius: 0.25rem; font-size: 0.8125rem; }
        .chat-msg.me { background: #1e3a5f; margin-left: 2rem; }
        .chat-msg.other { background: #0f172a; margin-right: 2rem; }
        .chat-msg .sender { color: #38bdf8; font-size: 0.6875rem; margin-bottom: 0.125rem; }
        .chat-msg .time { color: #64748b; font-size: 0.625rem; }
        .chat-msg .route { color: #f59e0b; font-size: 0.625rem; margin-top: 0.125rem; }
        .chat-input { display: flex; gap: 0.5rem; }
        .chat-input input { flex: 1; }
        .hidden { display: none; }
        .stats { display: grid; grid-template-columns: 1fr 1fr; gap: 0.5rem; }
        .stat { background: #0f172a; padding: 0.5rem; border-radius: 0.25rem; text-align: center; }
        .stat-val { font-size: 1.25rem; font-weight: 700; color: #38bdf8; }
        .stat-label { font-size: 0.625rem; color: #94a3b8; }
        .routing-table { font-size: 0.75rem; }
        .routing-row { display: flex; justify-content: space-between; padding: 0.375rem; background: #0f172a; border-radius: 0.25rem; margin-bottom: 0.25rem; }
        .routing-row .dest { color: #38bdf8; }
        .routing-row .via { color: #f59e0b; }
        .routing-row .metric { color: #94a3b8; }
    </style>
</head>
<body>
    <div class="header">
        <h1>RelayMesh</h1>
        <div class="status">
            <div class="dot" id="statusDot"></div>
            <span id="statusText">Disconnected</span>
        </div>
    </div>

    <div id="loginView">
        <div class="content">
            <div class="card">
                <h2>Join the Mesh Network</h2>
                <div class="form-group">
                    <label>Your Device Name</label>
                    <input type="text" id="deviceName" placeholder="e.g., John's Phone" />
                </div>
                <button class="btn btn-primary" onclick="connect()">Join Network</button>
                <div id="statusMsg"></div>
            </div>
        </div>
    </div>

    <div id="meshView" class="hidden">
        <div class="tabs">
            <div class="tab active" onclick="showTab('topology')">Topology</div>
            <div class="tab" onclick="showTab('chat')">Messages</div>
            <div class="tab" onclick="showTab('devices')">Devices</div>
            <div class="tab" onclick="showTab('routing')">Routing</div>
        </div>

        <div class="content">
            <div id="tab-topology">
                <div class="card">
                    <h2>Mesh Topology</h2>
                    <canvas id="topoCanvas"></canvas>
                </div>
            </div>

            <div id="tab-chat" class="hidden">
                <div class="card">
                    <h2>Messages <span id="msgMode" style="float:right;font-size:0.625rem;color:#f59e0b;">BROADCAST</span></h2>
                    <div class="chat" id="chatMessages">
                        <div class="empty">No messages yet</div>
                    </div>
                    <div class="form-group">
                        <select id="msgTarget" onchange="updateMsgMode()">
                            <option value="all">Broadcast to All</option>
                        </select>
                    </div>
                    <div class="chat-input">
                        <input type="text" id="chatInput" placeholder="Type a message..." onkeypress="if(event.key==='Enter')sendChat()" />
                        <button class="btn btn-send" onclick="sendChat()">Send</button>
                    </div>
                </div>
            </div>

            <div id="tab-devices" class="hidden">
                <div class="card">
                    <h2>Your Device</h2>
                    <div class="peer-item you">
                        <span id="yourName">-</span>
                        <span class="badge">YOU</span>
                    </div>
                </div>
                <div class="card">
                    <h2>Mesh Nodes (<span id="peerCount">0</span>)</h2>
                    <div class="peer-list" id="peerList">
                        <div class="empty">Waiting for others...</div>
                    </div>
                </div>
                <button class="btn btn-danger" onclick="disconnect()">Leave Network</button>
            </div>

            <div id="tab-routing" class="hidden">
                <div class="card">
                    <h2>Routing Table</h2>
                    <div class="routing-table" id="routingTable">
                        <div class="empty">No routes yet</div>
                    </div>
                </div>
                <div class="card">
                    <h2>Network Stats</h2>
                    <div class="stats">
                        <div class="stat"><div class="stat-val" id="statPeers">0</div><div class="stat-label">Nodes</div></div>
                        <div class="stat"><div class="stat-val" id="statMsgs">0</div><div class="stat-label">Messages</div></div>
                        <div class="stat"><div class="stat-val" id="statRelayed">0</div><div class="stat-label">Relayed</div></div>
                        <div class="stat"><div class="stat-val" id="statUptime">0s</div><div class="stat-label">Uptime</div></div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
        var ws = null, nodeId = null, deviceName = '', peers = [], messages = [], startTime = null, selectedTarget = 'all';
        var canvas, ctx, relayCount = 0;

        function generateId() { return 'web-' + Math.random().toString(36).substr(2, 9); }

        function showTab(name) {
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('[id^="tab-"]').forEach(t => t.classList.add('hidden'));
            event.target.classList.add('active');
            document.getElementById('tab-' + name).classList.remove('hidden');
            if (name === 'topology') drawTopology();
        }

        function showStatus(msg, type) {
            document.getElementById('statusMsg').innerHTML = '<div class="status-msg ' + type + '">' + msg + '</div>';
        }

        function connect() {
            deviceName = document.getElementById('deviceName').value.trim();
            if (!deviceName) { showStatus('Enter your device name', 'err'); return; }
            nodeId = generateId();
            startTime = Date.now();
            var protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            ws = new WebSocket(protocol + '//' + window.location.host + '/ws');
            showStatus('Connecting...', 'info');

            ws.onopen = function() {
                ws.send(JSON.stringify({ type: 'register', payload: { node_id: nodeId, device_name: deviceName } }));
                document.getElementById('loginView').classList.add('hidden');
                document.getElementById('meshView').classList.remove('hidden');
                document.getElementById('statusDot').classList.add('on');
                document.getElementById('statusText').textContent = 'Connected';
                document.getElementById('yourName').textContent = deviceName;
                canvas = document.getElementById('topoCanvas');
                ctx = canvas.getContext('2d');
                resizeCanvas();
                window.addEventListener('resize', resizeCanvas);
                setInterval(updateUptime, 1000);
            };

            ws.onmessage = function(e) {
                var msg = JSON.parse(e.data);
                switch(msg.type) {
                    case 'peer_list':
                        peers = msg.payload || [];
                        renderPeers();
                        drawTopology();
                        updateTargetSelect();
                        break;
                    case 'mesh_msg':
                        handleMeshMessage(msg);
                        break;
                    case 'heartbeat':
                        ws.send(JSON.stringify({ type: 'heartbeat' }));
                        break;
                }
            };

            ws.onclose = function() {
                document.getElementById('statusDot').classList.remove('on');
                document.getElementById('statusText').textContent = 'Disconnected';
            };
        }

        function disconnect() { if (ws) ws.close(); location.reload(); }

        function renderPeers() {
            document.getElementById('peerCount').textContent = peers.length;
            document.getElementById('statPeers').textContent = peers.length;
            var list = document.getElementById('peerList');
            if (!peers.length) { list.innerHTML = '<div class="empty">Waiting for others...</div>'; return; }
            list.innerHTML = peers.map(function(p) {
                var badges = '<span class="badge web">WEB</span>';
                if (p.hops > 1) badges += '<span class="badge relay">' + p.hops + ' HOPS</span>';
                return '<div class="peer-item" onclick="selectPeer(\'' + p.id + '\')"><span>' + p.device_name + '</span><div>' + badges + '</div></div>';
            }).join('');
            renderRoutingTable();
        }

        function selectPeer(id) {
            selectedTarget = id;
            document.getElementById('msgTarget').value = id;
            updateMsgMode();
            showTab('chat');
            document.querySelectorAll('.tab')[1].click();
        }

        function updateTargetSelect() {
            var sel = document.getElementById('msgTarget');
            sel.innerHTML = '<option value="all">Broadcast to All</option>';
            peers.forEach(function(p) {
                sel.innerHTML += '<option value="' + p.id + '">' + p.device_name + '</option>';
            });
            sel.value = selectedTarget;
        }

        function updateMsgMode() {
            selectedTarget = document.getElementById('msgTarget').value;
            document.getElementById('msgMode').textContent = selectedTarget === 'all' ? 'BROADCAST' : 'DIRECT → ' + getPeerName(selectedTarget);
        }

        function getPeerName(id) {
            var p = peers.find(function(p) { return p.id === id; });
            return p ? p.device_name : id;
        }

        function sendChat() {
            var input = document.getElementById('chatInput');
            var text = input.value.trim();
            if (!text || !ws) return;

            var msg = {
                type: 'mesh_msg',
                payload: {
                    from: nodeId,
                    from_name: deviceName,
                    to: selectedTarget,
                    text: text,
                    hops: 0,
                    path: [nodeId]
                }
            };

            ws.send(JSON.stringify(msg));
            messages.push({ payload: msg.payload, timestamp: new Date().toISOString(), local: true });
            renderChat();
            input.value = '';
        }

        function handleMeshMessage(msg) {
            var p = msg.payload;

            if (p.from === nodeId) return;

            if (p.to === 'all' || p.to === nodeId) {
                messages.push({ payload: p, timestamp: msg.timestamp || new Date().toISOString() });
                renderChat();
            }

            if (p.to !== 'all' && p.to !== nodeId && !p.path.includes(nodeId)) {
                relayCount++;
                document.getElementById('statRelayed').textContent = relayCount;
                p.hops++;
                p.path.push(nodeId);
                ws.send(JSON.stringify({ type: 'mesh_msg', payload: p }));
            }
        }

        function renderChat() {
            var el = document.getElementById('chatMessages');
            if (!messages.length) { el.innerHTML = '<div class="empty">No messages yet</div>'; return; }
            el.innerHTML = messages.map(function(m) {
                var p = m.payload;
                var isMe = p.from === nodeId;
                var time = new Date(m.timestamp).toLocaleTimeString();
                var route = p.hops > 0 ? '<div class="route">Relayed through ' + p.hops + ' hop(s): ' + p.path.join(' → ') + '</div>' : '<div class="route">Direct connection</div>';
                var target = p.to === 'all' ? ' (Broadcast)' : ' → ' + getPeerName(p.to);
                return '<div class="chat-msg ' + (isMe ? 'me' : 'other') + '"><div class="sender">' + p.from_name + target + '</div><div>' + p.text + '</div>' + route + '<div class="time">' + time + '</div></div>';
            }).join('');
            el.scrollTop = el.scrollHeight;
            document.getElementById('statMsgs').textContent = messages.length;
        }

        function renderRoutingTable() {
            var el = document.getElementById('routingTable');
            if (!peers.length) { el.innerHTML = '<div class="empty">No routes yet</div>'; return; }
            var html = '<div class="routing-row" style="font-weight:600;color:#94a3b8;"><span>Destination</span><span>Next Hop</span><span>Hops</span></div>';
            peers.forEach(function(p) {
                html += '<div class="routing-row"><span class="dest">' + p.device_name + '</span><span class="via">' + (p.hops === 1 ? 'Direct' : 'via relay') + '</span><span class="metric">' + p.hops + '</span></div>';
            });
            el.innerHTML = html;
        }

        function resizeCanvas() { canvas.width = canvas.offsetWidth; canvas.height = 200; drawTopology(); }

        function drawTopology() {
            if (!ctx || !canvas) return;
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            var allNodes = [{ id: nodeId, name: deviceName, isYou: true, hops: 0 }];
            peers.forEach(function(p) { allNodes.push({ id: p.id, name: p.device_name, isYou: false, hops: p.hops }); });
            if (allNodes.length === 1) { ctx.fillStyle = '#64748b'; ctx.font = '12px sans-serif'; ctx.textAlign = 'center'; ctx.fillText('Invite others to see the mesh', canvas.width/2, canvas.height/2); return; }
            var cx = canvas.width / 2, cy = canvas.height / 2, r = Math.min(cx, cy) - 30;
            var pos = {};
            allNodes.forEach(function(n, i) {
                var a = (2 * Math.PI * i) / allNodes.length - Math.PI/2;
                pos[n.id] = { x: cx + r * Math.cos(a), y: cy + r * Math.sin(a), name: n.name, isYou: n.isYou, hops: n.hops };
            });
            ctx.strokeStyle = '#334155'; ctx.lineWidth = 1;
            allNodes.forEach(function(n1) {
                allNodes.forEach(function(n2) {
                    if (n1.id !== n2.id) {
                        ctx.beginPath(); ctx.moveTo(pos[n1.id].x, pos[n1.id].y); ctx.lineTo(pos[n2.id].x, pos[n2.id].y);
                        ctx.strokeStyle = pos[n1.id].hops === 0 || pos[n2.id].hops === 0 ? '#22c55e' : '#38bdf8';
                        ctx.stroke();
                    }
                });
            });
            allNodes.forEach(function(n) {
                var p = pos[n.id];
                ctx.beginPath(); ctx.arc(p.x, p.y, 18, 0, 2*Math.PI);
                ctx.fillStyle = p.isYou ? '#22c55e' : p.hops > 1 ? '#f59e0b' : '#38bdf8';
                ctx.fill(); ctx.strokeStyle = '#0f172a'; ctx.lineWidth = 2; ctx.stroke();
                ctx.fillStyle = '#fff'; ctx.font = 'bold 10px sans-serif'; ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
                ctx.fillText(p.name.substring(0,2).toUpperCase(), p.x, p.y);
                ctx.fillStyle = '#94a3b8'; ctx.font = '9px sans-serif';
                ctx.fillText(p.isYou ? 'YOU' : p.hops + ' hop(s)', p.x, p.y + 28);
            });
        }

        function updateUptime() {
            if (!startTime) return;
            var s = Math.floor((Date.now() - startTime) / 1000);
            var m = Math.floor(s/60), h = Math.floor(m/60);
            document.getElementById('statUptime').textContent = h > 0 ? h+'h '+m%60+'m' : m > 0 ? m+'m '+s%60+'s' : s+'s';
        }
    </script>
</body>
</html>`

type Client struct {
	Conn       *websocket.Conn
	ID         string
	DeviceName string
	IsWeb      bool
	Hops       int
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
	json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "clients": count})
}

func (s *RelayServer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade failed: %v", err)
		return
	}

	client := &Client{Conn: conn, LastSeen: time.Now(), IsWeb: true, Hops: 0}

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
	case "mesh_msg":
		s.handleMeshMsg(client, rawMsg)
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

func (s *RelayServer) handleMeshMsg(client *Client, rawMsg []byte) {
	var msg Message
	json.Unmarshal(rawMsg, &msg)

	var payload struct {
		To       string   `json:"to"`
		From     string   `json:"from"`
		Text     string   `json:"text"`
		Hops     int      `json:"hops"`
		Path     []string `json:"path"`
	}
	json.Unmarshal(msg.Payload, &payload)

	s.clientsMu.RLock()
	defer s.clientsMu.RUnlock()

	if payload.To == "all" {
		for _, c := range s.clients {
			if c.ID != payload.From {
				c.Conn.WriteMessage(websocket.TextMessage, rawMsg)
			}
		}
		log.Printf("Broadcast from %s: %s", client.DeviceName, payload.Text)
	} else {
		for _, c := range s.clients {
			if c.ID == payload.To && !contains(payload.Path, c.ID) {
				c.Conn.WriteMessage(websocket.TextMessage, rawMsg)
				log.Printf("Direct message from %s to %s", client.DeviceName, c.DeviceName)
				return
			}
		}
		for _, c := range s.clients {
			if c.ID != payload.From && !contains(payload.Path, c.ID) {
				payload.Hops++
				payload.Path = append(payload.Path, client.ID)
				msg.Payload, _ = json.Marshal(payload)
				newMsg, _ := json.Marshal(msg)
				c.Conn.WriteMessage(websocket.TextMessage, newMsg)
				log.Printf("Relaying message from %s to %s via %s", client.DeviceName, payload.To, c.DeviceName)
				return
			}
		}
	}
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func (s *RelayServer) sendPeerList(client *Client) {
	s.clientsMu.RLock()
	peers := make([]map[string]interface{}, 0)
	for _, c := range s.clients {
		if c.ID != client.ID && c.ID != "" {
			hops := 1
			peers = append(peers, map[string]interface{}{
				"id":          c.ID,
				"device_name": c.DeviceName,
				"is_web":      c.IsWeb,
				"hops":        hops,
			})
		}
	}
	s.clientsMu.RUnlock()

	payload, _ := json.Marshal(peers)
	msg := Message{Type: "peer_list", Payload: payload, Timestamp: time.Now()}
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
			msg := Message{Type: "heartbeat", Timestamp: time.Now()}
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
	relay := NewRelayServer()
	relay.Start(":" + port)
}
