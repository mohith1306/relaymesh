package gateway

import (
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/relaymesh/relaymesh/internal/node"
	"github.com/relaymesh/relaymesh/internal/transport"
)

type GatewayNode struct {
	nodeID       node.NodeID
	internetConn *net.UDPConn
	transport    transport.Transport
	natTable     *NATTable
	logger       *slog.Logger
	mu           sync.RWMutex
}

func NewGateway(nodeID node.NodeID, transport transport.Transport, logger *slog.Logger) *GatewayNode {
	return &GatewayNode{
		nodeID:    nodeID,
		transport: transport,
		natTable:  NewNATTable(),
		logger:    logger,
	}
}

func (g *GatewayNode) Start(internetAddr string) error {
	addr, err := net.ResolveUDPAddr("udp4", internetAddr)
	if err != nil {
		return err
	}

	g.internetConn, err = net.ListenUDP("udp4", addr)
	if err != nil {
		return err
	}

	go g.internetListener()
	go g.natCleanup()

	g.logger.Info("gateway started",
		"node_id", g.nodeID,
		"internet_addr", internetAddr,
	)

	return nil
}

func (g *GatewayNode) Stop() error {
	if g.internetConn != nil {
		return g.internetConn.Close()
	}
	return nil
}

func (g *GatewayNode) internetListener() {
	buf := make([]byte, 65536)
	for {
		n, remoteAddr, err := g.internetConn.ReadFromUDP(buf)
		if err != nil {
			return
		}

		internalAddr, exists := g.natTable.GetInternalAddr(remoteAddr)
		if !exists {
			g.logger.Warn("no NAT entry for incoming packet",
				"from", remoteAddr,
			)
			continue
		}

		g.logger.Debug("forwarding internet packet to mesh",
			"from", remoteAddr,
			"to", internalAddr,
			"size", n,
		)

		g.forwardToMesh(buf[:n], internalAddr)
	}
}

func (g *GatewayNode) forwardToMesh(data []byte, dest *net.UDPAddr) {
	g.logger.Debug("forwarding data to mesh destination",
		"destination", dest,
		"size", len(data),
	)
}

func (g *GatewayNode) ForwardToInternet(pkt *transport.Packet, from node.NodeID) {
	g.logger.Debug("forwarding packet to internet",
		"source", pkt.Source,
		"from", from,
		"size", len(pkt.Payload),
	)

	internalAddr := &net.UDPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: int(9001),
	}

	externalAddr := &net.UDPAddr{
		IP:   net.ParseIP("8.8.8.8"),
		Port: 53,
	}

	g.natTable.AddEntry(&NATEntry{
		InternalAddr: internalAddr,
		ExternalAddr: externalAddr,
		Protocol:     "udp",
		CreatedAt:    time.Now(),
		ExpiresAt:    time.Now().Add(5 * time.Minute),
	})

	if g.internetConn != nil {
		g.internetConn.WriteToUDP(pkt.Payload, externalAddr)
	}
}

func (g *GatewayNode) natCleanup() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		removed := g.natTable.Cleanup()
		if removed > 0 {
			g.logger.Debug("cleaned up NAT entries", "removed", removed)
		}
	}
}

func (g *GatewayNode) GetNATTable() *NATTable {
	return g.natTable
}
