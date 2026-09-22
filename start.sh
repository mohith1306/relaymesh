#!/bin/bash

# RelayMesh Quick Start Script

echo "============================================"
echo "  RelayMesh - Distributed Mesh Network"
echo "============================================"
echo ""

# Check if binary exists
if [ ! -f "./bin/relaymesh" ]; then
    echo "Building RelayMesh..."
    make build
fi

# Get local IP
LOCAL_IP=$(ifconfig | grep "inet " | grep -v 127.0.0.1 | head -1 | awk '{print $2}')

echo "Starting RelayMesh network on $LOCAL_IP..."
echo ""

# Start Node A with dashboard
./bin/relaymesh --node-id node-a --port 9001 --dashboard :8080 --log-level info > /tmp/node-a.log 2>&1 &
echo "Started node-a on port 9001 (dashboard host)"

# Start Node B
./bin/relaymesh --node-id node-b --port 9002 --log-level info > /tmp/node-b.log 2>&1 &
echo "Started node-b on port 9002"

# Start Node C
./bin/relaymesh --node-id node-c --port 9003 --log-level info > /tmp/node-c.log 2>&1 &
echo "Started node-c on port 9003"

sleep 3

echo ""
echo "============================================"
echo "  Network Ready!"
echo "============================================"
echo ""
echo "  Dashboard: http://$LOCAL_IP:8080"
echo ""
echo "  To add more nodes from other machines:"
echo "    ./bin/relaymesh --node-id <name> --port <port>"
echo ""
echo "  Ports 9001-9005 are auto-discovered"
echo "============================================"
