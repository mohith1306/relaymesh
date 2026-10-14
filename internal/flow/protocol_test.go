package flow

import (
	"bytes"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/relaymesh/relaymesh/api/proto"
)

func TestWrapUnwrapRoundtrip(t *testing.T) {
	msg := &pb.FlowData{FlowId: "f1", Seq: 7, Chunk: []byte("hello")}
	raw, err := Wrap(kindFlowData, msg)
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}
	kind, body, ok := UnwrapEnvelope(raw)
	if !ok {
		t.Fatalf("unwrap failed")
	}
	if kind != kindFlowData {
		t.Fatalf("kind = %x, want %x", kind, kindFlowData)
	}
	var back pb.FlowData
	if err := proto.Unmarshal(body, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.FlowId != "f1" || back.Seq != 7 || !bytes.Equal(back.Chunk, []byte("hello")) {
		t.Fatalf("roundtrip mismatch: %+v", &back)
	}
}

func TestUnwrapRejectsForeignPayloads(t *testing.T) {
	if _, _, ok := UnwrapEnvelope([]byte("RMP\x01junk")); ok {
		t.Fatalf("probe magic must not parse as flow")
	}
	if _, _, ok := UnwrapEnvelope(nil); ok {
		t.Fatalf("nil payload must not parse")
	}
	if _, _, ok := UnwrapEnvelope([]byte{0x52, 0x4D}); ok {
		t.Fatalf("truncated magic must not parse")
	}
	if IsFlowPayload([]byte("RMF")) {
		t.Fatalf("magic-only payload should be false")
	}
	if !IsFlowPayload([]byte{0x52, 0x4D, 0x46, kindFlowAck}) {
		t.Fatalf("valid prefix should be true")
	}
}

// testSnapshot helpers lock the primitives for assertion.
func (r *reliableReceiver) expectSnapshot() uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.expect
}

func (s *reliableSender) nextSnapshot() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.next
}

// Happy path: chunk splitting, sequencing, cumulative ACKs.
func TestReliableStreamHappyPath(t *testing.T) {
	var received bytes.Buffer
	recv := newReliableReceiver(func(chunk []byte) {
		received.Write(chunk)
	}, nil)

	sent := make(chan *pb.FlowData, 64)
	sender := newReliableSender("f1", func(seq uint32, chunk []byte) error {
		sent <- &pb.FlowData{FlowId: "f1", Seq: seq, Chunk: chunk}
		return nil
	}, func(string) {})

	payload := make([]byte, ChunkSize*3+100)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	if err := sender.enqueue(payload); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	for i := 0; i < 100; i++ {
		dead, _ := sender.trySend(time.Now())
		if dead {
			t.Fatalf("sender died on happy path")
		}
		for drained := false; !drained; {
			select {
			case fr := <-sent:
				recv.onData(fr.Seq, fr.Chunk)
				sender.onAck(recv.expectSnapshot())
			default:
				drained = true
			}
		}
		if !sender.hasBacklog() {
			break
		}
	}

	if !bytes.Equal(received.Bytes(), payload) {
		t.Fatalf("received %d bytes, want %d", received.Len(), len(payload))
	}
	if sender.hasBacklog() {
		t.Fatalf("sender still has backlog after delivery")
	}
}

// A dropped frame must be retransmitted after RTO and still arrive.
func TestReliableRetransmit(t *testing.T) {
	var received bytes.Buffer
	recv := newReliableReceiver(func(chunk []byte) {
		received.Write(chunk)
	}, nil)

	attempts := map[uint32]int{}
	sent := make(chan *pb.FlowData, 16)
	sender := newReliableSender("f2", func(seq uint32, chunk []byte) error {
		attempts[seq]++
		if attempts[seq] == 1 {
			return nil // first transmission lost on the wire
		}
		sent <- &pb.FlowData{FlowId: "f2", Seq: seq, Chunk: chunk}
		return nil
	}, func(string) {})

	payload := []byte("retransmit-me")
	if err := sender.enqueue(payload); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// First pass: frame goes out (and is lost).
	dead, _ := sender.trySend(time.Now())
	if dead {
		t.Fatalf("unexpected death")
	}
	// Drain nothing; wait past RTO and retry.
	time.Sleep(RTO + 50*time.Millisecond)
	for i := 0; i < 10; i++ {
		dead, _ := sender.trySend(time.Now())
		if dead {
			t.Fatalf("unexpected death on retransmit")
		}
		drained := false
		for !drained {
			select {
			case fr := <-sent:
				recv.onData(fr.Seq, fr.Chunk)
				sender.onAck(recv.expectSnapshot())
			default:
				drained = true
			}
		}
		if !sender.hasBacklog() {
			break
		}
	}

	if received.String() != "retransmit-me" {
		t.Fatalf("received %q, want %q", received.String(), "retransmit-me")
	}
	if attempts[0] < 2 {
		t.Fatalf("seq 0 sent %d times, want >= 2 (retransmit)", attempts[0])
	}
}

// Out-of-order chunks buffer and release in order once the gap fills.
func TestReceiverOutOfOrder(t *testing.T) {
	var received bytes.Buffer
	recv := newReliableReceiver(func(chunk []byte) {
		received.Write(chunk)
	}, nil)

	recv.onData(1, []byte("B"))
	if received.Len() != 0 {
		t.Fatalf("delivered before gap filled")
	}
	recv.onData(2, []byte("C"))
	if received.Len() != 0 {
		t.Fatalf("delivered before gap filled")
	}
	recv.onData(0, []byte("A"))
	if received.String() != "ABC" {
		t.Fatalf("got %q, want ABC", received.String())
	}
	if recv.expectSnapshot() != 3 {
		t.Fatalf("expect = %d, want 3", recv.expectSnapshot())
	}
}

// Backpressure: enqueue blocks once the queue depth is exceeded and
// unblocks when the window drains.
func TestSenderBackpressure(t *testing.T) {
	sender := newReliableSender("f3", func(seq uint32, chunk []byte) error {
		return nil
	}, func(string) {})

	big := make([]byte, 200*1024) // ~166 frames, queue cap is 64
	done := make(chan error, 1)
	go func() {
		done <- sender.enqueue(big)
	}()

	select {
	case err := <-done:
		t.Fatalf("enqueue returned early (%v), expected backpressure", err)
	case <-time.After(100 * time.Millisecond):
	}

	// Continuously drain the window so enqueue can make progress.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				_, _ = sender.trySend(time.Now())
				sender.onAck(sender.nextSnapshot())
			}
		}
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("enqueue failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("enqueue never unblocked")
	}
}
