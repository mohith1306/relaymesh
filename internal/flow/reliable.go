package flow

import (
	"sync"
	"time"
)

// sendDataFn sends a FlowData frame to the peer.
type sendDataFn func(seq uint32, chunk []byte) error

// ackFn sends a cumulative FlowAck frame to the peer.
type ackFn func(ackSeq uint32) error

// deadFn reports that a direction is unrecoverable (retries
// exhausted); the owner should reset the whole flow.
type deadFn func(reason string)

// reliableSender implements the per-direction sender window: chunked
// frames get sequence numbers, ride an in-flight map bounded by
// WindowSize, and are retransmitted on RTO until cumulatively
// acknowledged or MaxRetries is exhausted.
type reliableSender struct {
	flowID     string
	send       sendDataFn
	onDead     deadFn
	mu         sync.Mutex
	unsent     [][]byte
	inflight   map[uint32]*sendSlot
	base       uint32 // oldest unacked seq
	next       uint32 // next seq to assign
	dead       bool
	deadReason string
	cond       *sync.Cond
}

type sendSlot struct {
	seq     uint32
	chunk   []byte
	sentAt  time.Time
	retries int
}

func newReliableSender(flowID string, send sendDataFn, onDead deadFn) *reliableSender {
	s := &reliableSender{
		flowID:   flowID,
		send:     send,
		onDead:   onDead,
		inflight: make(map[uint32]*sendSlot),
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// enqueue chunks data and queues it for transmission, applying
// backpressure when the combined unsent + in-flight backlog exceeds
// the send queue depth.
func (s *reliableSender) enqueue(data []byte) error {
	for len(data) > 0 {
		n := ChunkSize
		if len(data) < n {
			n = len(data)
		}
		chunk := make([]byte, n)
		copy(chunk, data[:n])
		data = data[n:]

		s.mu.Lock()
		for !s.dead && len(s.unsent)+len(s.inflight) >= sendQueueDepth {
			s.cond.Wait()
		}
		if s.dead {
			reason := s.deadReason
			s.mu.Unlock()
			return errFlowDead(reason)
		}
		s.unsent = append(s.unsent, chunk)
		s.cond.Signal()
		s.mu.Unlock()
	}
	return nil
}

// trySend transmits queued chunks while window space is available and
// retransmits timed-out in-flight frames. Called from the flow's
// writer goroutine.
func (s *reliableSender) trySend(now time.Time) (dead bool, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dead {
		return true, s.deadReason
	}
	for len(s.inflight) < WindowSize && len(s.unsent) > 0 {
		chunk := s.unsent[0]
		s.unsent = s.unsent[1:]
		seq := s.next
		s.next++
		if err := s.send(seq, chunk); err != nil {
			s.markDeadLocked("send failed: " + err.Error())
			return true, s.deadReason
		}
		s.inflight[seq] = &sendSlot{seq: seq, chunk: chunk, sentAt: now}
	}
	for _, slot := range s.inflight {
		if now.Sub(slot.sentAt) < RTO {
			continue
		}
		if slot.retries >= MaxRetries {
			s.markDeadLocked("retransmit limit")
			return true, s.deadReason
		}
		if err := s.send(slot.seq, slot.chunk); err != nil {
			s.markDeadLocked("send failed: " + err.Error())
			return true, s.deadReason
		}
		slot.retries++
		slot.sentAt = now
	}
	return false, ""
}

// onAck advances the cumulative window and wakes blocked writers.
func (s *reliableSender) onAck(ackSeq uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ackSeq <= s.base {
		return
	}
	s.base = ackSeq
	for seq := range s.inflight {
		if seq < ackSeq {
			delete(s.inflight, seq)
		}
	}
	s.cond.Broadcast()
}

// finish blocks until the backlog and in-flight window drain, or the
// flow dies. Used before sending FIN.
func (s *reliableSender) finish(deadline time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for !s.dead && (len(s.unsent) > 0 || len(s.inflight) > 0) {
		if time.Now().After(deadline) {
			return errFlowDead("finish timeout")
		}
		s.cond.Wait()
	}
	if s.dead {
		return errFlowDead(s.deadReason)
	}
	return nil
}

func (s *reliableSender) markDeadLocked(reason string) {
	if s.dead {
		return
	}
	s.dead = true
	s.deadReason = reason
	s.cond.Broadcast()
}

// markDead triggers the dead callback from outside the writer (e.g.
// peer RST).
func (s *reliableSender) markDead(reason string) {
	s.mu.Lock()
	s.markDeadLocked(reason)
	s.mu.Unlock()
	if s.onDead != nil {
		s.onDead(reason)
	}
}

// kill marks the sender dead WITHOUT firing the onDead callback.
// Teardown paths use this to avoid re-entering their own close
// sequence through the callback.
func (s *reliableSender) kill(reason string) {
	s.mu.Lock()
	s.markDeadLocked(reason)
	s.mu.Unlock()
}

func (s *reliableSender) isDead() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dead
}

// hasBacklog reports whether unsent or in-flight frames remain.
func (s *reliableSender) hasBacklog() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.unsent) > 0 || len(s.inflight) > 0
}

// reliableReceiver reassembles out-of-order chunks into a cumulative
// stream and emits duplicate ACKs for gaps.
type reliableReceiver struct {
	expect  uint32
	got     map[uint32][]byte
	deliver func([]byte)
	ack     ackFn
	mu      sync.Mutex
}

func newReliableReceiver(deliver func([]byte), ack ackFn) *reliableReceiver {
	return &reliableReceiver{
		got:     make(map[uint32][]byte),
		deliver: deliver,
		ack:     ack,
	}
}

func (r *reliableReceiver) onData(seq uint32, chunk []byte) {
	r.mu.Lock()
	switch {
	case seq < r.expect:
		// Duplicate of an already-delivered frame: re-ACK.
	case seq == r.expect:
		r.deliver(chunk)
		r.expect++
		for {
			next, ok := r.got[r.expect]
			if !ok {
				break
			}
			delete(r.got, r.expect)
			r.deliver(next)
			r.expect++
		}
	case seq < r.expect+recvBufferFrames:
		if _, exists := r.got[seq]; !exists {
			r.got[seq] = chunk
		}
	default:
		// Far future: drop; sender will retransmit after RTO.
	}
	ackSeq := r.expect
	r.mu.Unlock()
	if r.ack != nil {
		_ = r.ack(ackSeq)
	}
}

// flowError is returned when a flow is dead.
type flowError struct{ reason string }

func (e *flowError) Error() string { return "flow dead: " + e.reason }

func errFlowDead(reason string) error { return &flowError{reason: reason} }

// IsFlowDead reports whether err is a flow-dead error.
func IsFlowDead(err error) bool {
	_, ok := err.(*flowError)
	return ok
}
