package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/relaymesh/relaymesh/api/proto"
	"github.com/relaymesh/relaymesh/internal/forwarding"
	"github.com/relaymesh/relaymesh/internal/mesh"
	"github.com/relaymesh/relaymesh/internal/node"
)

const (
	defaultDialTimeout = 5 * time.Second
	maxDialTimeout     = 10 * time.Second
	flowTTL            = 5 * time.Minute
	maxDatagram        = 65536
)

// Gateway is a full mesh participant that additionally serves internet
// egress: mesh packets addressed to it carrying an EgressRequest are
// dialed out to the internet target, and the reply is routed back
// through the mesh to the requesting node:
//
//	Mesh node A -> relay B -> Gateway -> Internet target
//	Internet target -> Gateway -> relay B -> Mesh node A
type Gateway struct {
	mesh   *mesh.MeshNode
	nat    *NATTable
	logger *slog.Logger
}

func New(cfg mesh.Config, logger *slog.Logger) *Gateway {
	return &Gateway{
		mesh:   mesh.New(cfg, logger),
		nat:    NewNATTable(),
		logger: logger,
	}
}

func (g *Gateway) Start(ctx context.Context) error {
	if err := g.mesh.Start(ctx); err != nil {
		return err
	}
	g.mesh.OnDelivered(func(p forwarding.DeliveredPacket) {
		req, ok := parseEgressRequest(p.Payload)
		if !ok {
			return
		}
		go g.handleEgress(p.Source, req)
	})
	go g.natJanitor(ctx)
	return nil
}

func (g *Gateway) Stop() error { return g.mesh.Stop() }

// Mesh exposes the underlying mesh node for routing, stats, and shutdown.
func (g *Gateway) Mesh() *mesh.MeshNode { return g.mesh }

// NAT exposes the flow table for observability and tests.
func (g *Gateway) NAT() *NATTable { return g.nat }

func parseEgressRequest(payload []byte) (*pb.EgressRequest, bool) {
	var req pb.EgressRequest
	if err := proto.Unmarshal(payload, &req); err != nil {
		return nil, false
	}
	if req.RequestId == "" || req.TargetHost == "" || req.TargetPort == 0 {
		return nil, false
	}
	return &req, true
}

func (g *Gateway) handleEgress(source node.NodeID, req *pb.EgressRequest) {
	timeout := time.Duration(req.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = defaultDialTimeout
	}
	if timeout > maxDialTimeout {
		timeout = maxDialTimeout
	}

	target := net.JoinHostPort(req.TargetHost, fmt.Sprintf("%d", req.TargetPort))
	g.nat.Track(source, req.RequestId, target, flowTTL)

	g.logger.Info("egress dial",
		"source", source,
		"request_id", req.RequestId,
		"target", target,
	)

	respond := func(ok bool, data []byte, errMsg string) {
		resp := &pb.EgressResponse{
			RequestId: req.RequestId,
			Ok:        ok,
			Data:      data,
			Error:     errMsg,
		}
		raw, err := proto.Marshal(resp)
		if err != nil {
			g.logger.Error("failed to marshal egress response", "error", err)
			return
		}
		if err := g.mesh.Send(source, raw); err != nil {
			g.logger.Error("failed to return egress response",
				"dest", source, "error", err)
		}
	}

	conn, err := net.DialTimeout("udp", target, timeout)
	if err != nil {
		respond(false, nil, fmt.Sprintf("dial %s: %v", target, err))
		return
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		respond(false, nil, fmt.Sprintf("set deadline: %v", err))
		return
	}
	if _, err := conn.Write(req.Data); err != nil {
		respond(false, nil, fmt.Sprintf("write to %s: %v", target, err))
		return
	}
	buf := make([]byte, maxDatagram)
	n, err := conn.Read(buf)
	if err != nil {
		respond(false, nil, fmt.Sprintf("read from %s: %v", target, err))
		return
	}
	g.logger.Info("egress reply",
		"source", source,
		"request_id", req.RequestId,
		"bytes", n,
	)
	respond(true, buf[:n], "")
}

func (g *Gateway) natJanitor(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if removed := g.nat.Cleanup(); removed > 0 {
				g.logger.Debug("cleaned up NAT flows", "removed", removed)
			}
		}
	}
}
