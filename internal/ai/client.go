package ai

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	pb "github.com/relaymesh/relaymesh/api/proto"
	"github.com/relaymesh/relaymesh/internal/node"
	"github.com/relaymesh/relaymesh/internal/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn   *grpc.ClientConn
	client pb.AIServiceClient
	logger *slog.Logger
	addr   string
}

func NewClient(addr string, logger *slog.Logger) *Client {
	return &Client{
		addr:   addr,
		logger: logger,
	}
}

func (c *Client) Connect(ctx context.Context) error {
	var err error
	c.conn, err = grpc.DialContext(ctx, c.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("failed to connect to AI service: %w", err)
	}

	c.client = pb.NewAIServiceClient(c.conn)
	c.logger.Info("connected to AI service", "addr", c.addr)
	return nil
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *Client) GetRecommendation(ctx context.Context,
	source node.NodeID,
	destination node.NodeID,
	collectors map[node.NodeID]*telemetry.MetricsCollector,
	peers []node.NodeID,
) (*pb.RouteRecommendation, error) {

	state := &pb.NetworkState{
		NodeId:        string(source),
		FeatureVector: collectors[source].ToFeatureVector(),
		Timestamp:     time.Now().Unix(),
	}

	for _, peer := range peers {
		if peer == source {
			continue
		}
		metrics, exists := collectors[peer]
		if !exists {
			continue
		}
		m := metrics.GetMetrics()
		state.Peers = append(state.Peers, &pb.PeerState{
			PeerId:    string(peer),
			Latency:   float32(m.Latency),
			PacketLoss: float32(m.PacketLoss),
			Bandwidth: float32(m.Bandwidth),
			LastSeen:  time.Now().Unix(),
		})
	}

	var candidates []string
	for _, peer := range peers {
		candidates = append(candidates, string(peer))
	}

	req := &pb.InferenceRequest{
		State:                 state,
		CandidateDestinations: candidates,
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := c.client.GetRecommendation(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to get recommendation: %w", err)
	}

	if len(resp.Recommendations) == 0 {
		return nil, fmt.Errorf("no recommendations returned")
	}

	rec := resp.Recommendations[0]
	c.logger.Info("AI recommendation",
		"destination", rec.Destination,
		"confidence", rec.Confidence,
		"score", rec.Score,
		"inference_time_ms", resp.InferenceTimeMs,
	)

	return rec, nil
}

func (c *Client) ReportFeedback(ctx context.Context, routeID string, reward float64, success bool) error {
	feedback := &pb.TrainingFeedback{
		RouteId:   routeID,
		Reward:    reward,
		Success:   success,
		Timestamp: time.Now().Unix(),
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := c.client.ReportFeedback(ctx, feedback)
	if err != nil {
		return fmt.Errorf("failed to report feedback: %w", err)
	}

	return nil
}

func (c *Client) GetModelInfo(ctx context.Context) (*pb.ModelInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return c.client.GetModelInfo(ctx, &pb.ModelInfoRequest{NodeId: "relaymesh"})
}
