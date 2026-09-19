#!/bin/bash
# RelayMesh Real-World Demo
# Simulates: Laptop -> Node A -> Node B -> Node C -> Gateway -> Internet

set -e

echo "============================================"
echo "  RelayMesh Real-World Simulation Demo"
echo "============================================"
echo ""
echo "Topology:"
echo "  [Laptop] -> [A] -> [B] -> [C] -> [Gateway]"
echo "                \\-> [D] -/"
echo ""

# Build first
echo "[1/4] Building..."
make build --no-print-directory 2>/dev/null

echo "[2/4] Starting nodes..."
echo ""

# Start Gateway
./bin/relaymesh --node-id gateway --port 9104 --log-level info &
GW_PID=$!
sleep 0.5

# Start Node C
./bin/relaymesh --node-id node-c --port 9102 --log-level info &
C_PID=$!
sleep 0.5

# Start Node B
./bin/relaymesh --node-id node-b --port 9101 --log-level info &
B_PID=$!
sleep 0.5

# Start Node D (backup path)
./bin/relaymesh --node-id node-d --port 9103 --log-level info &
D_PID=$!
sleep 0.5

# Start Node A (source)
./bin/relaymesh --node-id node-a --port 9100 --log-level info &
A_PID=$!

sleep 2

echo ""
echo "[3/4] All nodes running. PIDs:"
echo "  Node A:      $A_PID"
echo "  Node B:      $B_PID"
echo "  Node C:      $C_PID"
echo "  Node D:      $D_PID"
echo "  Gateway:     $GW_PID"
echo ""
echo "Nodes are discovering peers via UDP heartbeats..."
echo ""

sleep 5

echo "[4/4] Simulation complete. Cleaning up..."
echo ""

kill $A_PID $B_PID $C_PID $D_PID $GW_PID 2>/dev/null
wait $A_PID $B_PID $C_PID $D_PID $GW_PID 2>/dev/null

echo "All nodes stopped."
