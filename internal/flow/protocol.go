// Package flow implements the RelayMesh flow tunnel: continuous TCP
// and UDP sessions from a mesh node through a gateway to the internet,
// carried as chunked frames over the lossy UDP mesh data plane.
//
// TCP frames are reliable (per-direction sequence numbers, cumulative
// ACKs, RTO retransmit). UDP frames are best-effort datagrams capped
// at one chunk. All frames ride the same magic envelope style as mesh
// probes ("RMF") so the mesh classifier can dispatch them without
// colliding with legacy EgressRequest payloads.
package flow

import (
	"bytes"
	"time"

	"google.golang.org/protobuf/proto"
)

// flowMagic marks flow-tunnel envelopes ("RMF"). Kept distinct from
// the probe magic ("RMP") so neither classifier can swallow the
// other's frames.
var flowMagic = []byte{0x52, 0x4D, 0x46}

// Frame kinds, mirroring the Flow* messages in relaymesh.proto.
const (
	kindFlowOpen     byte = 0x10
	kindFlowAccept   byte = 0x11
	kindFlowReject   byte = 0x12
	kindFlowData     byte = 0x13
	kindFlowAck      byte = 0x14
	kindFlowFIN      byte = 0x15
	kindFlowRST      byte = 0x16
	kindFlowDatagram byte = 0x17
)

const (
	// ChunkSize is the max mesh payload chunk per FlowData frame,
	// safely under the ~1400B UDP MTU (the data plane does not
	// fragment).
	ChunkSize = 1200
	// WindowSize bounds in-flight unacked frames per direction.
	WindowSize = 16
	// RTO is the retransmit timeout for unacked frames.
	RTO = 500 * time.Millisecond
	// MaxRetries before a flow is declared dead and reset.
	MaxRetries = 8
	// IdleTimeout closes flows with no frames in either direction.
	IdleTimeout = 120 * time.Second
	// MaxFlows caps concurrent flows through one gateway.
	MaxFlows = 256
	// DialTimeout bounds the gateway's internet dial.
	DialTimeout = 10 * time.Second
	// AcceptTimeout bounds how long OpenTCP waits for FlowAccept.
	AcceptTimeout = 15 * time.Second
	// sendQueueDepth bounds the Conn.Write backlog before backpressure.
	sendQueueDepth = 64
	// recvBufferFrames bounds out-of-order receive buffering.
	recvBufferFrames = WindowSize
)

// WrapEnvelope frames a flow message with the RMF magic and kind byte.
func Wrap(kind byte, msg proto.Message) ([]byte, error) {
	raw, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(flowMagic)+1+len(raw))
	out = append(out, flowMagic...)
	out = append(out, kind)
	out = append(out, raw...)
	return out, nil
}

// UnwrapEnvelope reports whether payload is a flow frame and returns
// its kind and protobuf body.
func UnwrapEnvelope(payload []byte) (byte, []byte, bool) {
	if len(payload) < len(flowMagic)+1 {
		return 0, nil, false
	}
	if !bytes.Equal(payload[:len(flowMagic)], flowMagic) {
		return 0, nil, false
	}
	return payload[len(flowMagic)], payload[len(flowMagic)+1:], true
}

// IsFlowPayload reports whether payload starts with the flow magic.
func IsFlowPayload(payload []byte) bool {
	_, _, ok := UnwrapEnvelope(payload)
	return ok
}
