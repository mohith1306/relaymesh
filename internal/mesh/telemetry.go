package mesh

import (
	"github.com/relaymesh/relaymesh/internal/telemetry"
)

// SyncTelemetry pushes live data-plane measurements into a telemetry
// collector: probe RTTs become latency, probe sent/recv ratios become
// packet loss, routing-table depth becomes hop count. This is what the
// dashboard and the AI feature vectors consume — no simulated values.
func (m *MeshNode) SyncTelemetry(c *telemetry.MetricsCollector) {
	links := m.LinkStates()

	var rttSum, upCount float64
	var sent, recv uint64
	for _, l := range links {
		sent += l.Sent
		recv += l.Recv
		if l.Down {
			continue
		}
		upCount++
		if l.HasRTT {
			rttSum += float64(l.LastRTT.Microseconds()) / 1000.0
		}
		c.UpdatePeerMetrics(l.Peer, linkLatencyMs(l), linkLoss(l), DefaultBandwidth)
	}

	if upCount > 0 {
		c.UpdateLatency(rttSum / upCount)
	} else {
		c.UpdateLatency(0)
	}
	if sent > 0 {
		c.UpdatePacketLoss(1 - float64(recv)/float64(sent))
	} else {
		c.UpdatePacketLoss(0)
	}
	c.UpdateBandwidth(DefaultBandwidth)

	var hopSum float64
	var routes int
	for _, dest := range m.router.GetTable().GetAllDestinations() {
		if r, ok := m.router.GetTable().GetBestRoute(dest); ok {
			hopSum += float64(r.HopCount)
			routes++
		}
	}
	if routes > 0 {
		c.UpdateHopCount(uint32(hopSum / float64(routes)))
	} else {
		c.UpdateHopCount(0)
	}

	if len(links) > 0 {
		c.UpdateStability(upCount / float64(len(links)))
	} else {
		c.UpdateStability(1.0)
	}
}

func linkLatencyMs(l LinkInfo) float64 {
	if !l.HasRTT {
		return 0
	}
	ms := float64(l.LastRTT.Microseconds()) / 1000.0
	if ms < 0 {
		return 0
	}
	return ms
}

func linkLoss(l LinkInfo) float64 {
	if l.Sent == 0 {
		return 0
	}
	loss := 1 - float64(l.Recv)/float64(l.Sent)
	if loss < 0 {
		return 0
	}
	if loss > 1 {
		return 1
	}
	return loss
}
