# RelayMesh

A distributed peer-to-peer mesh networking system with AI-assisted routing.

## What is RelayMesh?

RelayMesh creates self-healing mesh networks where devices communicate through multiple hops. If one node fails, traffic automatically reroutes through alternative paths. An AI control plane optimizes routing decisions in real-time.

## Architecture

```
┌─────────────────────────────────────────────────┐
│                 RELAYMESH                       │
├─────────────────────────────────────────────────┤
│  GO DATA PLANE                                  │
│  • Node Engine                                  │
│  • Peer Discovery (UDP)                         │
│  • Dijkstra Routing                             │
│  • Packet Forwarding                            │
│  • Gateway/NAT                                  │
│  • Telemetry Collection                         │
├─────────────────────────────────────────────────┤
│           gRPC + Protobuf                       │
├─────────────────────────────────────────────────┤
│  PYTHON AI CONTROL PLANE                        │
│  • RL Route Optimization                        │
│  • Network Simulation                           │
│  • Feature Processing                           │
│  • Inference Service                            │
└─────────────────────────────────────────────────┘
```

## Features

- **Peer Discovery**: Automatic node discovery on local networks
- **Deterministic Routing**: Dijkstra-based shortest path calculation
- **Multi-Hop Forwarding**: Packets traverse multiple nodes to reach destination
- **Gateway Support**: Connect mesh to internet through gateway nodes
- **Telemetry**: Collect latency, bandwidth, packet loss metrics
- **AI Ready**: Python service for RL-based route optimization

## Quick Start

### Prerequisites

- Go 1.21+
- Python 3.9+ (for AI service)
- Protobuf compiler (for regenerating proto files)

### Build

```bash
make build
```

### Run a Node

```bash
./bin/relaymesh --node-id node-a --port 9001
```

### Run Multiple Nodes

```bash
# Terminal 1
./bin/relaymesh --node-id node-a --port 9001

# Terminal 2
./bin/relaymesh --node-id node-b --port 9002

# Terminal 3
./bin/relaymesh --node-id node-c --port 9003
```

Nodes automatically discover each other and establish routes.

### Run AI Service (Optional)

```bash
source .venv/bin/activate
python -m ai.service.server
```

## Configuration

Generate a default config file:

```bash
./bin/relaymesh --generate-config
```

Example `relaymesh.json`:

```json
{
  "node": {
    "id": "node-1",
    "port": 9000,
    "max_peers": 10,
    "heartbeat_interval": 5000000000
  },
  "log": {
    "level": "info"
  }
}
```

## CLI Options

```
--node-id       Node identifier (default: node-1)
--port          Listen port (default: 9000)
--address       Listen address (default: 0.0.0.0)
--config        Path to config file
--log-level     Log level (debug, info, warn, error)
--generate-config  Generate default config file
--proxy        Local SOCKS5/HTTP proxy address (e.g., 127.0.0.1:1080)
--gateway      Gateway node ID to route proxied traffic through (required with --proxy)
```

## Phase 13: Ride the Mesh from Any App

A mesh node can expose a local SOCKS5 + HTTP CONNECT proxy. Local apps
(browser, `curl`, WhatsApp's HTTP proxy setting) point at it; traffic is
tunneled through the mesh to the gateway node and out to the internet.

Live demo (three terminals):

```bash
# Terminal 1 — the gateway (has real internet egress)
go run ./cmd/gateway --node-id node-GW --port 9001

# Terminal 2 — a mesh hop / relay
go run ./cmd/relaymesh --node-id node-B --port 9002 --known-ports 9001

# Terminal 3 — client node + local proxy
go run ./cmd/relaymesh --node-id node-A --port 9003 --known-ports 9002 \
  --proxy 127.0.0.1:1080 --gateway node-GW
```

Then from any app on your machine:

```bash
# SOCKS5 (curl resolves the hostname at the gateway, not locally)
curl --socks5-hostname 127.0.0.1:1080 https://www.facebook.com

# HTTP CONNECT (what a browser / WhatsApp proxy setting uses)
HTTPS_PROXY=http://127.0.0.1:1080 curl https://www.facebook.com
```

The client node never needs internet itself — only radio/IP reachability
of at least one mesh peer. The gateway node provides the real egress.

## Project Structure

```
relaymesh/
├── cmd/relaymesh/       # CLI entry point
├── internal/
│   ├── node/           # Node lifecycle & state machine
│   ├── discovery/      # UDP peer discovery
│   ├── transport/      # UDP transport layer
│   ├── routing/        # Dijkstra routing algorithm
│   ├── forwarding/     # Packet forwarding engine
│   ├── gateway/        # NAT & internet gateway
│   └── telemetry/      # Network metrics collection
├── api/proto/          # Protobuf & gRPC definitions
├── ai/service/         # Python AI inference service
├── tests/              # Integration tests
└── Makefile            # Build automation
```

## Testing

```bash
# Run all tests
make test

# Run Go tests only
make test-go

# Run specific test
go test -v ./tests/ -run TestMVPMeshNetwork
```

## How It Works

1. **Discovery**: Nodes broadcast heartbeats on UDP port 9001
2. **Routing**: Each node runs Dijkstra to find shortest paths
3. **Forwarding**: Packets include TTL and hop count for loop prevention
4. **Telemetry**: Nodes collect latency, packet loss, bandwidth metrics
5. **AI**: Python service receives metrics and recommends optimal routes

## License

MIT
