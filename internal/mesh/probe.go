package mesh

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/relaymesh/relaymesh/api/proto"
	"github.com/relaymesh/relaymesh/internal/forwarding"
	"github.com/relaymesh/relaymesh/internal/node"
)

const (
	probeInterval = 500 * time.Millisecond
	// missThreshold is how many unanswered probes mark a data-plane
	// link down. At 500ms intervals this detects a dead data link in
	// ~1.5s, independent of the (slower) discovery heartbeat timeout.
	missThreshold = 3
)

// probeFlight tracks one in-flight probe. The entry is pre-registered
// before sending so a fast pong can never slip the accounting.
type probeFlight struct {
	peer  node.NodeID
	at    time.Time
	acked bool
}
type linkState struct {
	firstSeen time.Time
	lastRTT   time.Duration
	hasRTT    bool
	sent      uint64
	recv      uint64
	unacked   int
}

// Probe frames carry a magic envelope so the classifier can never
// mistake application or egress payloads for probes: EgressRequest
// shares the same first wire tag as LinkPong, so content sniffing
// alone would swallow gateway traffic.
var probeMagic = []byte{0x52, 0x4D, 0x50} // "RMP"

const (
	probeKindProbe byte = 1
	probeKindPong  byte = 2
)

func wrapProbe(kind byte, msg proto.Message) ([]byte, error) {
	raw, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(probeMagic)+1+len(raw))
	out = append(out, probeMagic...)
	out = append(out, kind)
	out = append(out, raw...)
	return out, nil
}

func unwrapProbe(payload []byte) (byte, []byte, bool) {
	if len(payload) < len(probeMagic)+1 {
		return 0, nil, false
	}
	if !bytes.Equal(payload[:len(probeMagic)], probeMagic) {
		return 0, nil, false
	}
	return payload[len(probeMagic)], payload[len(probeMagic)+1:], true
}

type LinkInfo struct {
	Peer    node.NodeID
	Down    bool
	Manual  bool
	LastRTT time.Duration
	HasRTT  bool
	Sent    uint64
	Recv    uint64
	Unacked int
}

// SetLinkDown administratively forces a data-plane link down or clears
// the override. A down link is excluded from routing until cleared,
// even while discovery heartbeats still arrive (simulates and manages
// data-plane breaks with a live control plane).
func (m *MeshNode) SetLinkDown(peer node.NodeID, down bool) {
	m.linkMu.Lock()
	if down {
		if m.manualDown == nil {
			m.manualDown = make(map[node.NodeID]bool)
		}
		m.manualDown[peer] = true
	} else {
		delete(m.manualDown, peer)
	}
	m.linkMu.Unlock()

	if down {
		m.router.RemoveLink(m.config.NodeID, peer)
		m.syncForwardingTable()
		m.logger.Warn("link administratively down", "peer", peer)
	} else {
		m.logger.Info("link administrative override cleared", "peer", peer)
	}
}

// LinkStates returns health snapshots for all known direct peers.
func (m *MeshNode) LinkStates() []LinkInfo {
	peers := m.discovery.Snapshot()
	out := make([]LinkInfo, 0, len(peers))
	m.linkMu.Lock()
	defer m.linkMu.Unlock()
	for _, p := range peers {
		st := m.peerStats[p.ID]
		info := LinkInfo{Peer: p.ID}
		if st != nil {
			info.LastRTT = st.lastRTT
			info.HasRTT = st.hasRTT
			info.Sent = st.sent
			info.Recv = st.recv
			info.Unacked = st.unacked
		}
		_, info.Manual = m.manualDown[p.ID]
		_, auto := m.autoDown[p.ID]
		info.Down = info.Manual || auto
		out = append(out, info)
	}
	return out
}

func (m *MeshNode) isDownLocked(peer node.NodeID) bool {
	if m.manualDown != nil {
		if m.manualDown[peer] {
			return true
		}
	}
	if m.autoDown != nil {
		if m.autoDown[peer] {
			return true
		}
	}
	return false
}

// linkLatency returns the measured RTT-based cost for a peer, falling
// back to the default when no measurement exists yet.
func (m *MeshNode) linkLatency(peer node.NodeID) float64 {
	m.linkMu.Lock()
	defer m.linkMu.Unlock()
	if st, ok := m.peerStats[peer]; ok && st.hasRTT {
		ms := float64(st.lastRTT.Microseconds()) / 1000.0
		if ms < 0.1 {
			ms = 0.1
		}
		return ms
	}
	return DefaultLatency
}

// classify is the single forwarder delivery hook: link probes/pongs are
// handled internally, everything else flows to the app channel/callback.
func (m *MeshNode) classify(p forwarding.DeliveredPacket) {
	if kind, raw, ok := unwrapProbe(p.Payload); ok {
		switch kind {
		case probeKindPong:
			var pong pb.LinkPong
			if err := proto.Unmarshal(raw, &pong); err == nil && pong.ProbeId != "" {
				m.recordPong(p.Source, &pong)
			}
		case probeKindProbe:
			var probe pb.LinkProbe
			if err := proto.Unmarshal(raw, &probe); err == nil && probe.ProbeId != "" {
				m.replyProbe(p.Source, &probe)
			}
		}
		return
	}
	select {
	case m.delivered <- p:
	default:
		m.logger.Warn("delivered channel full, dropping notification", "packet_id", p.ID)
	}
	m.mu.Lock()
	cb := m.extraCB
	m.mu.Unlock()
	if cb != nil {
		cb(p)
	}
}

func (m *MeshNode) replyProbe(source node.NodeID, probe *pb.LinkProbe) {
	pong := &pb.LinkPong{
		ProbeId:           probe.ProbeId,
		SentAtUnixNano:    probe.SentAtUnixNano,
		RepliedAtUnixNano: time.Now().UnixNano(),
	}
	raw, err := wrapProbe(probeKindPong, pong)
	if err != nil {
		return
	}
	pkt := forwarding.NewPacket(m.config.NodeID, source, raw, forwarding.DefaultTTL)
	pkt.Sequence = m.seq.Add(1)
	// Direct peer: bypass routing, exercise this exact data link.
	if err := m.forwarder.Forward(pkt, source); err != nil {
		m.logger.Debug("probe reply failed", "peer", source, "error", err)
	}
}

func (m *MeshNode) recordPong(source node.NodeID, pong *pb.LinkPong) {
	sentAt := time.Unix(0, pong.SentAtUnixNano)

	m.linkMu.Lock()
	st := m.statsForLocked(source)
	if f, ok := m.outstanding[pong.ProbeId]; ok {
		f.acked = true
		if st.unacked > 0 {
			st.unacked--
		}
	}
	st.recv++
	st.lastRTT = time.Since(sentAt)
	if st.lastRTT < 0 {
		st.lastRTT = 0
	}
	st.hasRTT = true
	wasAuto := m.autoDown[source]
	if wasAuto {
		if _, manual := m.manualDown[source]; !manual {
			delete(m.autoDown, source)
		} else {
			wasAuto = false
		}
	}
	m.linkMu.Unlock()

	if wasAuto {
		m.logger.Info("data link recovered", "peer", source, "rtt", st.lastRTT)
		m.router.UpdateLink(m.config.NodeID, source, m.linkLatency(source), DefaultBandwidth, DefaultLoss)
		m.syncForwardingTable()
	}
}

func (m *MeshNode) statsForLocked(peer node.NodeID) *linkState {
	st, ok := m.peerStats[peer]
	if !ok {
		st = &linkState{firstSeen: time.Now()}
		m.peerStats[peer] = st
	}
	return st
}

func (m *MeshNode) probeLoop(ctx context.Context) {
	ticker := time.NewTicker(probeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.probeTick()
		}
	}
}

func (m *MeshNode) probeTick() {
	peers := m.discovery.Snapshot()
	now := time.Now()
	m.linkMu.Lock()
	for id, f := range m.outstanding {
		if now.Sub(f.at) > 10*time.Second {
			if !f.acked {
				if st, ok := m.peerStats[f.peer]; ok && st.unacked > 0 {
					st.unacked--
				}
			}
			delete(m.outstanding, id)
		}
	}
	m.linkMu.Unlock()

	for _, p := range peers {
		m.linkMu.Lock()
		if m.manualDown[p.ID] {
			m.linkMu.Unlock()
			continue
		}
		m.statsForLocked(p.ID)
		probeID := fmt.Sprintf("%s-probe-%d", m.config.NodeID, m.seq.Add(1))
		// Pre-register the flight so a fast pong can never slip the
		// accounting below.
		m.outstanding[probeID] = &probeFlight{peer: p.ID, at: time.Now()}
		m.linkMu.Unlock()

		probe := &pb.LinkProbe{
			ProbeId:        probeID,
			SentAtUnixNano: time.Now().UnixNano(),
		}
		raw, err := wrapProbe(probeKindProbe, probe)
		if err != nil {
			m.linkMu.Lock()
			delete(m.outstanding, probeID)
			m.linkMu.Unlock()
			continue
		}
		pkt := forwarding.NewPacket(m.config.NodeID, p.ID, raw, forwarding.DefaultTTL)
		pkt.Sequence = m.seq.Add(1)
		// Direct peer: bypass routing so the probe tests this link.
		// A failed send says nothing about the link (e.g. transport
		// not connected yet), so it is not counted.
		sendErr := m.forwarder.Forward(pkt, p.ID)

		m.linkMu.Lock()
		st := m.statsForLocked(p.ID)
		f := m.outstanding[probeID]
		if sendErr != nil {
			delete(m.outstanding, probeID)
			m.linkMu.Unlock()
			m.logger.Debug("probe send failed", "peer", p.ID, "error", sendErr)
			continue
		}
		st.sent++
		if f != nil && f.acked {
			// Pong beat the bookkeeping: already counted, just collect.
			delete(m.outstanding, probeID)
		} else {
			st.unacked++
		}
		down := st.unacked >= missThreshold
		_, already := m.autoDown[p.ID]
		if down && !already {
			m.autoDown[p.ID] = true
		}
		m.linkMu.Unlock()

		if down && !already {
			m.logger.Warn("data link down (probes unanswered)",
				"peer", p.ID, "unacked", st.unacked)
			m.router.RemoveLink(m.config.NodeID, p.ID)
			m.syncForwardingTable()
		}
	}
}
