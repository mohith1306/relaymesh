package gateway

import (
	"fmt"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/relaymesh/relaymesh/api/proto"
	"github.com/relaymesh/relaymesh/internal/mesh"
	"github.com/relaymesh/relaymesh/internal/node"
)

var egressSeq atomic.Uint64

// SendEgressRequest sends data to an internet target through a mesh
// gateway and waits for the reply:
//
//	local node -> ... -> gateway -> target -> gateway -> ... -> local node
//
// NOTE: responses arrive on the mesh node's Delivered channel, so the
// caller must be the sole reader of that channel while waiting (any
// other reader may swallow the response).
func SendEgressRequest(m *mesh.MeshNode, gatewayID node.NodeID, host string, port uint16, data []byte, timeout time.Duration) ([]byte, error) {
	if timeout <= 0 {
		timeout = defaultDialTimeout
	}
	requestID := fmt.Sprintf("%s-%d-%d", m.ID(), time.Now().UnixNano(), egressSeq.Add(1))

	req := &pb.EgressRequest{
		RequestId:  requestID,
		TargetHost: host,
		TargetPort: uint32(port),
		Data:       data,
		TimeoutMs:  uint32(timeout.Milliseconds()),
	}
	raw, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal egress request: %w", err)
	}
	if err := m.Send(gatewayID, raw); err != nil {
		return nil, fmt.Errorf("send to gateway %s: %w", gatewayID, err)
	}

	deadline := time.Now().Add(timeout + 2*time.Second)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, fmt.Errorf("egress response timeout for %s", requestID)
		}
		select {
		case d := <-m.Delivered():
			resp, ok := parseEgressResponse(d.Payload)
			if !ok || resp.RequestId != requestID {
				continue
			}
			if !resp.Ok {
				return nil, fmt.Errorf("egress failed: %s", resp.Error)
			}
			return resp.Data, nil
		case <-time.After(remaining):
			return nil, fmt.Errorf("egress response timeout for %s", requestID)
		}
	}
}

func parseEgressResponse(payload []byte) (*pb.EgressResponse, bool) {
	var resp pb.EgressResponse
	if err := proto.Unmarshal(payload, &resp); err != nil {
		return nil, false
	}
	if resp.RequestId == "" {
		return nil, false
	}
	return &resp, true
}
