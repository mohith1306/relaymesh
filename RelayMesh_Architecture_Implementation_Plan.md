# RelayMesh — Architecture & Implementation Plan

## 1. Final Technology Decision

RelayMesh is a distributed peer-to-peer networking system with two major planes:

- **Go** — data plane, packet forwarding, peer discovery, routing, transport, gateway, telemetry, security, distributed-system logic.
- **Python** — AI control plane, reinforcement-learning training, simulation, inference, route optimization.
- **gRPC + Protobuf** — communication between Go nodes and the Python AI service.
- **QUIC / UDP** — peer-to-peer transport.
- **Deterministic routing fallback** — RelayMesh must continue operating if the AI service is unavailable or returns an invalid recommendation.

### Core architectural rule

> **Go is authoritative for networking and packet forwarding. Python provides route intelligence and recommendations.**

The RL model must never directly control packet forwarding.

---

# 2. High-Level Architecture

```text
                                      INTERNET
                                          │
                                ┌─────────▼─────────┐
                                │   Gateway Node    │
                                │       GO          │
                                │                   │
                                │ NAT / Forwarding  │
                                └─────────┬─────────┘
                                          │
══════════════════════════════════════════╪══════════════════════════════════
                              RELAYMESH DATA PLANE
                                          │
                 ┌────────────────────────┼───────────────────────┐
                 │                        │                       │
          ┌──────▼──────┐          ┌──────▼──────┐        ┌──────▼──────┐
          │   NODE A    │◄────────►│   NODE B    │◄──────►│   NODE C    │
          │     GO      │          │     GO      │        │     GO      │
          └──────┬──────┘          └──────┬──────┘        └──────┬──────┘
                 │                        │                       │
                 ▼                        ▼                       ▼
              Device                   Device                  Device


                 ┌─────────────────────────────────────┐
                 │          EVERY GO NODE               │
                 │                                     │
                 │  Application / CLI                   │
                 │          │                          │
                 │          ▼                          │
                 │  Node Manager                       │
                 │          │                          │
                 │          ▼                          │
                 │  Peer Discovery                     │
                 │          │                          │
                 │          ▼                          │
                 │  Routing Engine                     │
                 │    ┌───────────────┐                │
                 │    │ Deterministic │                │
                 │    │ Router        │                │
                 │    └───────┬───────┘                │
                 │            │                         │
                 │    ┌───────▼───────┐                │
                 │    │ AI Route      │                │
                 │    │ Decision      │                │
                 │    │ Client        │                │
                 │    └───────┬───────┘                │
                 │            │                         │
                 │       gRPC / Protobuf               │
                 │            │                         │
                 │    ┌───────▼───────┐                │
                 │    │ Packet        │                │
                 │    │ Forwarder     │                │
                 │    └───────┬───────┘                │
                 │            │                         │
                 │    ┌───────▼───────┐                │
                 │    │ QUIC / UDP    │                │
                 │    └───────────────┘                │
                 └─────────────────────────────────────┘

                              AI CONTROL PLANE

                       ┌────────────▼────────────┐
                       │   PYTHON AI SERVICE     │
                       │                         │
                       │ RL Inference            │
                       │ RL Policy               │
                       │ Feature Processing      │
                       │ Model Management        │
                       └────────────┬────────────┘
                                    │
                                    ▼
                            RL MODEL / POLICY
                                    │
                         ┌──────────┴──────────┐
                         │                     │
                  Training Environment      Inference
                         │                     │
                         ▼                     ▼
                    Simulator              Production
```

---

# 3. Data Plane vs AI Control Plane

```text
                    RELAYMESH
                        │
             ┌──────────┴──────────┐
             │                     │
        DATA PLANE             CONTROL PLANE
             │                     │
             ▼                     ▼
            GO                   PYTHON
             │                     │
       Actual packets          Intelligence
       Actual routing          Predictions
       Connections             RL
       Forwarding              Optimization
```

### Go responsibilities

- Node lifecycle
- Peer discovery
- Peer connections
- Packet creation
- Packet forwarding
- Routing table
- Deterministic routing
- Route validation
- QUIC/UDP transport
- Gateway/NAT handling
- Telemetry collection
- Security
- Failure recovery

### Python responsibilities

- RL environment
- Network simulation
- Feature processing
- RL training
- Policy evaluation
- Route scoring
- Inference
- Model management
- Offline experimentation

---

# 4. Repository Architecture

```text
relaymesh/
│
├── cmd/
│   ├── relaymesh/
│   │   └── main.go
│   │
│   └── gateway/
│       └── main.go
│
├── internal/
│   │
│   ├── node/
│   │   ├── node.go
│   │   ├── manager.go
│   │   └── state.go
│   │
│   ├── discovery/
│   │   ├── discovery.go
│   │   ├── peer.go
│   │   └── heartbeat.go
│   │
│   ├── routing/
│   │   ├── router.go
│   │   ├── route.go
│   │   ├── table.go
│   │   ├── deterministic.go
│   │   ├── ai_router.go
│   │   └── safety.go
│   │
│   ├── forwarding/
│   │   ├── forwarder.go
│   │   ├── packet.go
│   │   └── queue.go
│   │
│   ├── transport/
│   │   ├── transport.go
│   │   ├── udp.go
│   │   └── quic.go
│   │
│   ├── security/
│   │   ├── identity.go
│   │   ├── keys.go
│   │   └── encryption.go
│   │
│   ├── telemetry/
│   │   ├── metrics.go
│   │   └── collector.go
│   │
│   ├── gateway/
│   │   ├── gateway.go
│   │   └── nat.go
│   │
│   └── config/
│       └── config.go
│
├── api/
│   └── proto/
│       ├── routing.proto
│       ├── telemetry.proto
│       └── node.proto
│
├── ai/
│   │
│   ├── service/
│   │   ├── server.py
│   │   ├── inference.py
│   │   └── feature_processor.py
│   │
│   ├── models/
│   │   ├── policy.py
│   │   └── model_loader.py
│   │
│   ├── training/
│   │   ├── environment.py
│   │   ├── train.py
│   │   ├── reward.py
│   │   └── evaluation.py
│   │
│   └── simulation/
│       ├── topology.py
│       ├── network.py
│       └── traffic.py
│
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── routing/
│   ├── transport/
│   └── simulation/
│
├── deployments/
│   ├── docker/
│   └── local/
│
├── scripts/
│
├── docs/
│
├── go.mod
├── go.sum
├── pyproject.toml
├── Makefile
└── README.md
```

---

# 5. Implementation Phases

## Phase 1 — Go Node Foundation

### Goal

Create a functioning RelayMesh node.

### Implement

- Node
- NodeID
- NodeState
- Configuration
- Logger
- Lifecycle management

### Node states

```text
STARTING
  ↓
DISCOVERING
  ↓
CONNECTED
  ↓
RELAYING
  ↓
DEGRADED
  ↓
SHUTDOWN
```

### Deliverable

```bash
relaymesh --node-id node-a
```

---

# Phase 2 — Peer Discovery

Implement Go-based peer discovery.

```text
Node A
  │
  │ discovery
  ▼
Node B
  │
  │ advertisement
  ▼
Node A
```

Example peer structure:

```go
type Peer struct {
    ID           NodeID
    Address      string
    Port         uint16
    Latency      time.Duration
    Bandwidth    float64
    PacketLoss   float64
    LastSeen     time.Time
    Capabilities []Capability
}
```

### Deliverable

```text
Node A discovers Node B
Node B discovers Node C
```

---

# Phase 3 — Go Transport Layer

Start with UDP and add QUIC.

```text
UDP
 ↓
QUIC
```

Interface:

```go
type Transport interface {
    Listen() error
    Connect(peer Peer) error
    Send(packet Packet) error
    Receive() (Packet, error)
    Close() error
}
```

Routing must not depend directly on UDP or QUIC.

---

# Phase 4 — RelayMesh Protocol

Define the protocol using Protobuf.

Example:

```protobuf
message PeerAdvertisement {
    string node_id = 1;
    string address = 2;
    uint32 port = 3;
    repeated string capabilities = 4;
}
```

Packet:

```protobuf
message RelayPacket {
    string source = 1;
    string destination = 2;
    uint64 sequence = 3;
    uint32 ttl = 4;
    bytes payload = 5;
}
```

---

# Phase 5 — Basic Routing

Implement deterministic routing before AI.

Use a standard graph routing algorithm such as Dijkstra initially.

Example:

```text
A → B → C
```

Possible route cost:

```text
cost =
    latency
    + packet loss
    + hop penalty
```

### Deliverable

RelayMesh can calculate a valid route without Python.

---

# Phase 6 — Multi-Hop Forwarding

Implement:

```text
A
 ↓
B
 ↓
C
 ↓
D
```

Packet flow:

```text
A creates packet
       ↓
Routing table
       ↓
B
       ↓
Routing table
       ↓
C
       ↓
Routing table
       ↓
D
```

Implement:

- TTL
- Sequence numbers
- Duplicate detection
- Route expiry
- Forwarding queues
- Loop prevention

---

# Phase 7 — Gateway

Create a `GatewayNode`.

```text
Client
   ↓
Relay
   ↓
Relay
   ↓
Gateway
   ↓
NAT
   ↓
Internet
```

### Major milestone

A device connected through multiple RelayMesh hops can reach the Internet through a gateway node.

---

# Phase 8 — Network Telemetry

Go collects:

- Latency
- Bandwidth
- Packet loss
- Jitter
- Hop count
- Queue size
- CPU
- Memory
- Battery where available
- Connection stability
- Congestion

Example:

```go
type NetworkMetrics struct {
    Latency      float64
    PacketLoss   float64
    Bandwidth    float64
    Jitter       float64
    Congestion   float64
    HopCount     int
    Stability    float64
}
```

These metrics become RL inputs.

---

# Phase 9 — Python AI Service

Introduce the Python control plane.

```text
Go
 │
 │ gRPC
 ▼
Python
```

Python service:

```text
ai/service/server.py
ai/service/inference.py
ai/service/feature_processor.py
```

Input:

```text
Network state
+
Candidate routes
```

Output:

```text
Selected route
+
Score
+
Confidence
```

---

# Phase 10 — RL Simulation Environment

Do not train RL directly on a real network initially.

Build a simulator.

```text
             RL Environment
                  │
        ┌─────────┼─────────┐
        ▼         ▼         ▼
     Nodes     Links      Traffic
        │         │         │
        └─────────┼─────────┘
                  ▼
             Network State
                  │
                  ▼
               RL Agent
                  │
                  ▼
                Action
                  │
                  ▼
            Next Network State
                  │
                  ▼
                Reward
```

Simulate:

- Node failures
- Congestion
- Changing bandwidth
- Packet loss
- Latency changes
- Gateway failures
- Topology changes
- Traffic patterns

---

# Phase 11 — RL Routing

## State

```text
S =
[
    latency,
    bandwidth,
    packet_loss,
    jitter,
    congestion,
    hop_count,
    node_stability,
    gateway_quality,
    ...
]
```

## Action

```text
A = select next hop
```

## Reward

Initial reward:

```text
reward =
    + throughput
    - latency
    - packet_loss
    - congestion
    - route_changes
```

The reward function should be tuned through simulation and benchmarked against deterministic routing.

---

# Phase 12 — Go ↔ Python AI Integration

Production loop:

```text
Go telemetry
      │
      ▼
Feature extraction
      │
      ▼
gRPC
      │
      ▼
Python RL inference
      │
      ▼
Route recommendation
      │
      ▼
gRPC
      │
      ▼
Go safety validation
      │
      ▼
Routing table
```

### Important rule

Do not call Python for every packet.

Packets use the existing Go routing table.

AI is invoked when the system needs a new or improved routing decision.

---

# Phase 13 — AI Safety Layer

Architecture:

```text
             AI recommendation
                    │
                    ▼
             Safety Validator
                    │
          ┌─────────┴─────────┐
          │                   │
        VALID               INVALID
          │                   │
          ▼                   ▼
      AI route          Deterministic
                           router
```

Validate:

- Node exists
- Node is reachable
- Route is not expired
- TTL is valid
- Destination is reachable
- Route is loop-free
- Node is not overloaded
- Network conditions satisfy minimum constraints

The Go routing engine remains authoritative.

---

# Phase 14 — Dynamic Route Adaptation

Allow RL to react to changing conditions.

```text
Route A
  ↓
Performance drops
  ↓
Go telemetry
  ↓
Python RL
  ↓
Alternative route
  ↓
Go validates
  ↓
Route switch
```

Example:

```text
A → B → C → Gateway

B becomes congested

A → D → C → Gateway
```

The application should remain connected if a valid alternative route exists.

---

# Phase 15 — Failure Recovery

Test:

- Node failure
- Link failure
- Gateway failure
- Network partition
- Packet loss
- High congestion
- AI service failure
- AI timeout
- Invalid AI recommendation

Critical requirement:

```text
Python AI service OFF
        ↓
RelayMesh continues operating
        ↓
Deterministic routing
```

---

# Phase 16 — Security

Implement:

- Node identity
- Public/private keys
- Mutual authentication
- Encrypted transport
- Packet authentication
- Replay protection

Do not invent cryptographic algorithms.

Use established cryptographic libraries and protocols.

---

# Phase 17 — Performance Engineering

Benchmark:

```text
packets/sec
throughput
latency
CPU
memory
route calculation time
AI inference latency
gRPC latency
route recovery time
```

Compare:

```text
Deterministic routing
        vs
AI-assisted routing
```

Measure whether the AI improves routing outcomes without becoming the bottleneck.

---

# Phase 18 — Real-World Mesh Testing

Testing progression:

```text
Simulation
    ↓
Multiple localhost processes
    ↓
Multiple laptops
    ↓
WiFi LAN
    ↓
Multiple network interfaces
    ↓
Real heterogeneous network
```

---

# 6. RL Development Strategy

Do not immediately start with the most complicated RL algorithm.

Use progressive versions:

```text
Version 0
Deterministic routing
        ↓
Version 1
Weighted heuristic routing
        ↓
Version 2
Supervised route prediction
        ↓
Version 3
RL simulation
        ↓
Version 4
RL inference
        ↓
Version 5
Adaptive / online RL
```

Every AI version should be compared against the deterministic baseline.

---

# 7. Final Production Architecture

```text
                         INTERNET
                             │
                       ┌─────▼─────┐
                       │  Gateway  │
                       │    Go     │
                       └─────┬─────┘
                             │
              ═══════════════╪═══════════════
                       GO DATA PLANE
                             │
                 ┌───────────┼───────────┐
                 │           │           │
                 ▼           ▼           ▼
                Go          Go          Go
               Node        Node        Node
                 │           │           │
                 └───────────┼───────────┘
                             │
                        gRPC / TLS
                             │
                             ▼
                  ┌─────────────────────┐
                  │ Python AI Cluster   │
                  │                     │
                  │ RL Inference        │
                  │ Model Registry      │
                  │ Model Monitoring     │
                  └─────────────────────┘
```

---

# 8. Implementation Milestones

| Milestone | Component | Output |
|---|---|---|
| M1 | Go Node | Running node |
| M2 | Discovery | Nodes discover each other |
| M3 | Transport | Peer-to-peer connection |
| M4 | Protocol | RelayMesh packets |
| M5 | Routing | Single-path routing |
| M6 | Multi-hop | A → B → C |
| M7 | Gateway | Mesh → Internet |
| M8 | Telemetry | Network metrics |
| M9 | Python AI | Inference service |
| M10 | Simulator | RL environment |
| M11 | RL | Trained routing policy |
| M12 | gRPC | Go ↔ Python |
| M13 | AI Router | AI-assisted routing |
| M14 | Safety | AI fallback |
| M15 | Adaptation | Dynamic route switching |
| M16 | Security | Authenticated/encrypted mesh |
| M17 | Benchmark | Performance evaluation |
| M18 | Real Network | Multi-device demonstration |

---

# 9. MVP Definition

The first meaningful RelayMesh demonstration should be:

```text
             INTERNET
                 │
                 ▼
             Gateway
                 │
                 ▼
              Node C
                 ▲
                 │
              Node B
                 ▲
                 │
              Node A
                 │
              Laptop
```

Demonstrate:

```text
A discovers B
B discovers C
C discovers Gateway
        ↓
A learns route
        ↓
A → B → C → Gateway
        ↓
Internet works
```

Then deliberately kill B:

```text
A → B → C

B DEAD

        ↓

A → D → C
```

Finally introduce the RL layer:

```text
                 Network
                    │
                    ▼
               Go telemetry
                    │
                    ▼
                Python RL
                    │
                    ▼
             Route decision
                    │
                    ▼
              Go validator
                    │
                    ▼
               Go router
                    │
                    ▼
                Network
```

---

# 10. Final Technology Stack

```text
┌───────────────────────────────────────────┐
│                 RELAYMESH                 │
├───────────────────────────────────────────┤
│                                           │
│ DATA PLANE — GO                           │
│                                           │
│ Node Engine                               │
│ Peer Discovery                            │
│ Routing                                   │
│ Packet Forwarding                         │
│ QUIC / UDP                                │
│ Gateway                                   │
│ Telemetry                                 │
│ Security                                  │
│                                           │
├───────────────────────────────────────────┤
│            gRPC + Protobuf                │
├───────────────────────────────────────────┤
│                                           │
│ AI CONTROL PLANE — PYTHON                 │
│                                           │
│ Feature Processing                        │
│ RL Environment                            │
│ RL Training                               │
│ Policy                                    │
│ Inference                                 │
│ Model Evaluation                          │
│                                           │
└───────────────────────────────────────────┘
```

---

# 11. Immediate Next Step for OpenCode

Before OpenCode starts implementing the full system, create a **technical specification** containing:

1. Protobuf schemas
2. Go interfaces
3. Python gRPC API
4. Node state machine
5. Packet lifecycle
6. Peer discovery protocol
7. Routing-table format
8. Deterministic routing algorithm
9. RL state definition
10. RL action definition
11. RL reward definition
12. AI safety constraints
13. Failure/recovery behavior
14. Telemetry schema
15. Phase-by-phase acceptance tests

This specification should be treated as the contract between the Go data plane and Python AI control plane.
