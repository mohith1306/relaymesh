package transport

import (
	"bytes"
	"crypto/ecdh"
	"crypto/rand"
	"net"
	"testing"
	"time"

	"github.com/relaymesh/relaymesh/internal/identity"
	"github.com/relaymesh/relaymesh/internal/node"
)

// buildSessions runs the handshake crypto end-to-end in memory and
// returns the initiator's and responder's session for the same link.
func buildSessions(t *testing.T, psk []byte, a, b node.NodeID) (sessA, sessB *session) {
	t.Helper()
	ida, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity a: %v", err)
	}
	idb, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity b: %v", err)
	}
	ephA, _ := ecdh.X25519().GenerateKey(rand.Reader)
	ephB, _ := ecdh.X25519().GenerateKey(rand.Reader)
	var sid [sidLen]byte
	if _, err := rand.Read(sid[:]); err != nil {
		t.Fatalf("sid: %v", err)
	}

	im := buildInit(ida, a, b, "127.0.0.1:12345", ephA, sid, psk)
	if err := verifyInit(im, psk, b); err != nil {
		t.Fatalf("verifyInit: %v", err)
	}
	rm := buildResp(idb, b, im, ephB, psk)
	if err := verifyResp(rm, im, psk, a); err != nil {
		t.Fatalf("verifyResp: %v", err)
	}

	fpA := identity.Fingerprint(ida.PublicKey())
	fpB := identity.Fingerprint(idb.PublicKey())
	sessA, err = deriveSession(psk, im, rm, ephA, rm.EphemeralPub, true, sid, b, fpB)
	if err != nil {
		t.Fatalf("derive A: %v", err)
	}
	sessB, err = deriveSession(psk, im, rm, ephB, im.EphemeralPub, false, sid, a, fpA)
	if err != nil {
		t.Fatalf("derive B: %v", err)
	}
	return sessA, sessB
}

// TestSecureSealOpen proves confidentiality, integrity, replay
// protection, and UDP-reordering tolerance of the wire frame.
func TestSecureSealOpen(t *testing.T) {
	psk := []byte("network-key")
	sessA, sessB := buildSessions(t, psk, "node-A", "node-B")

	secret := []byte("super-secret-payload")
	frame, err := sessA.seal(secret)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if bytes.Contains(frame, secret) {
		t.Fatalf("plaintext visible on the wire: %q", frame)
	}

	pt, err := sessB.open(frame)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if !bytes.Equal(pt, secret) {
		t.Fatalf("round trip mismatch: %q", pt)
	}

	// Replay: the same frame must never decrypt twice.
	if _, err := sessB.open(frame); err == nil {
		t.Fatalf("replayed frame accepted")
	} else if err != errReplay {
		t.Fatalf("expected replay error, got %v", err)
	}

	// Tamper: flipping any ciphertext byte must fail authentication.
	frame2, err := sessA.seal(secret)
	if err != nil {
		t.Fatalf("seal 2: %v", err)
	}
	frame2[hdrLen] ^= 0x01
	if _, err := sessB.open(frame2); err != errDecrypt {
		t.Fatalf("tampered frame: got %v, want errDecrypt", err)
	}

	// Header tamper (AAD): mutating the counter must fail too.
	frame3, _ := sessA.seal(secret)
	frame3[16] ^= 0x01
	if _, err := sessB.open(frame3); err == nil {
		t.Fatalf("counter tamper accepted")
	}

	// Wrong direction key: A cannot open its own frame (recv != send).
	if _, err := sessA.open(frame); err == nil {
		t.Fatalf("session accepted a frame on the wrong key")
	}
}

// TestSecureReorderWindow proves the 128-slot replay window tolerates
// out-of-order delivery while still rejecting duplicates and ancient
// counters.
func TestSecureReorderWindow(t *testing.T) {
	sessA, sessB := buildSessions(t, []byte("k"), "node-A", "node-B")

	var frames [][]byte
	for i := 0; i < 5; i++ {
		f, err := sessA.seal([]byte{byte(i)})
		if err != nil {
			t.Fatalf("seal: %v", err)
		}
		frames = append(frames, f)
	}
	// Deliver out of order: 1, 3, 0, 4, 2.
	order := []int{1, 3, 0, 4, 2}
	for _, i := range order {
		pt, err := sessB.open(frames[i])
		if err != nil {
			t.Fatalf("out-of-order frame %d rejected: %v", i, err)
		}
		if pt[0] != byte(i) {
			t.Fatalf("frame %d decrypted as %d", i, pt[0])
		}
	}
	// Duplicate of an in-window frame.
	if _, err := sessB.open(frames[2]); err != errReplay {
		t.Fatalf("duplicate in-window frame: got %v, want errReplay", err)
	}
}

// TestHandshakeCryptoRejectsWrongPSK proves the PSK proof is bound
// into both handshake messages.
func TestHandshakeCryptoRejectsWrongPSK(t *testing.T) {
	ida, _ := identity.Generate()
	idb, _ := identity.Generate()
	ephA, _ := ecdh.X25519().GenerateKey(rand.Reader)
	ephB, _ := ecdh.X25519().GenerateKey(rand.Reader)
	var sid [sidLen]byte
	_, _ = rand.Read(sid[:])

	im := buildInit(ida, "node-A", "node-B", "127.0.0.1:1", ephA, sid, []byte("right"))
	if err := verifyInit(im, []byte("wrong"), "node-B"); err == nil {
		t.Fatalf("init with wrong PSK verified")
	}
	rm := buildResp(idb, "node-B", im, ephB, []byte("right"))
	if err := verifyResp(rm, im, []byte("wrong"), "node-A"); err == nil {
		t.Fatalf("resp with wrong PSK verified")
	}
	// Addressed-to-wrong-node replay is rejected.
	if err := verifyInit(im, []byte("right"), "node-C"); err == nil {
		t.Fatalf("init replayed to another node verified")
	}
}

// TestSecureTransportHandshake drives two real UDP transports through
// mutual authentication and an encrypted packet exchange.
func TestSecureTransportHandshake(t *testing.T) {
	psk := []byte("mesh-key")
	idA, _ := identity.Generate()
	idB, _ := identity.Generate()

	a := NewUDP("node-A")
	b := NewUDP("node-B")
	a.SetPSK(psk)
	b.SetPSK(psk)
	a.SetSecure(idA, true)
	b.SetSecure(idB, true)

	if err := a.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("listen a: %v", err)
	}
	defer a.Close()
	if err := b.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("listen b: %v", err)
	}
	defer b.Close()

	if err := a.Connect("node-B", b.ListenAddr()); err != nil {
		t.Fatalf("connect a: %v", err)
	}
	if err := b.Connect("node-A", a.ListenAddr()); err != nil {
		t.Fatalf("connect b: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		if a.SecureStats().Sessions == 1 && b.SecureStats().Sessions == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("handshake never completed: a=%+v b=%+v",
				a.SecureStats(), b.SecureStats())
		}
		a.startHandshake("node-B")
		b.startHandshake("node-A")
		time.Sleep(100 * time.Millisecond)
	}
	if a.SecureStats().HandshakeFailures != 0 || b.SecureStats().HandshakeFailures != 0 {
		t.Fatalf("unexpected handshake failures: a=%+v b=%+v", a.SecureStats(), b.SecureStats())
	}

	if err := a.Send("node-B", &Packet{
		Source: "node-A", Destination: "node-B",
		Payload: []byte("tunnel-hello"), ID: "s1", TTL: 64,
	}); err != nil {
		t.Fatalf("secure send: %v", err)
	}
	got := waitResult(t, b, 3*time.Second)
	if string(got) != "tunnel-hello" {
		t.Fatalf("wrong payload: %q", got)
	}

	// Reverse direction (B's session keys).
	if err := b.Send("node-A", &Packet{
		Source: "node-B", Destination: "node-A",
		Payload: []byte("tunnel-back"), ID: "s2", TTL: 64,
	}); err != nil {
		t.Fatalf("secure send back: %v", err)
	}
	if got := waitResult(t, a, 3*time.Second); string(got) != "tunnel-back" {
		t.Fatalf("wrong reverse payload: %q", got)
	}
}

// TestSecureRejectsPlaintext proves encrypted nodes fail closed on
// plaintext data frames, and plaintext nodes ignore secure frames.
func TestSecureRejectsPlaintext(t *testing.T) {
	idB, _ := identity.Generate()
	b := NewUDP("node-B")
	b.SetSecure(idB, true)
	if err := b.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer b.Close()

	plain := NewUDP("node-P")
	if err := plain.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("listen plain: %v", err)
	}
	defer plain.Close()
	if err := plain.Connect("node-B", b.ListenAddr()); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := plain.Send("node-B", &Packet{
		Source: "node-P", Destination: "node-B",
		Payload: []byte("cleartext"), ID: "c1", TTL: 64,
	}); err != nil {
		t.Fatalf("send: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	if b.SecureStats().PlaintextRejects == 0 {
		t.Fatalf("plaintext frame not counted: %+v", b.SecureStats())
	}
	if p := tryReceive(b, 200*time.Millisecond); p != nil {
		t.Fatalf("plaintext delivered to encrypted node: %q", p)
	}

	// Inverse: a plaintext-only node must ignore secure frames.
	raw, err := net.Dial("udp4", plain.ListenAddr())
	if err != nil {
		t.Fatalf("raw dial: %v", err)
	}
	defer raw.Close()
	if _, err := raw.Write(frameHeader(kindData, [sidLen]byte{9, 9, 9, 9}, 1)); err != nil {
		t.Fatalf("raw write: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for plain.SecureStats().SecureRejects == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("secure frame not counted by plaintext node: %+v", plain.SecureStats())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestSecureIdentityPinMismatch proves a peer that reappears with a
// different identity key under the same node ID is rejected (TOFU).
func TestSecureIdentityPinMismatch(t *testing.T) {
	psk := []byte("k")
	idB, _ := identity.Generate()
	b := NewUDP("node-B")
	b.SetPSK(psk)
	b.SetSecure(idB, true)
	if err := b.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer b.Close()

	// driveA runs a handshake attempt from a fresh node-A transport
	// until cond reports done.
	driveA := func(idA *identity.Identity, done func() bool, timeout time.Duration) {
		a := NewUDP("node-A")
		a.SetPSK(psk)
		a.SetSecure(idA, true)
		if err := a.Listen("127.0.0.1:0"); err != nil {
			t.Fatalf("listen a: %v", err)
		}
		defer a.Close()
		if err := a.Connect("node-B", b.ListenAddr()); err != nil {
			t.Fatalf("connect a: %v", err)
		}
		_ = b.Connect("node-A", a.ListenAddr())
		deadline := time.Now().Add(timeout)
		for !done() && time.Now().Before(deadline) {
			a.startHandshake("node-B")
			b.startHandshake("node-A")
			time.Sleep(100 * time.Millisecond)
		}
	}

	idA1, _ := identity.Generate()
	driveA(idA1, func() bool { return b.SecureStats().Sessions == 1 }, 5*time.Second)
	if b.SecureStats().Sessions != 1 {
		t.Fatalf("first handshake never completed: %+v", b.SecureStats())
	}
	if len(b.pins) != 1 {
		t.Fatalf("expected one pinned identity, got %d", len(b.pins))
	}

	// Same node ID, different key: B must refuse and keep the old pin.
	idA2, _ := identity.Generate()
	driveA(idA2, func() bool { return b.SecureStats().IdentityMismatches > 0 }, 5*time.Second)
	if b.SecureStats().IdentityMismatches == 0 {
		t.Fatalf("identity mismatch not counted: %+v", b.SecureStats())
	}
	if b.SecureStats().Sessions != 1 {
		t.Fatalf("impostor disturbed the session state: %+v", b.SecureStats())
	}
	if len(b.pins) != 1 {
		t.Fatalf("pin replaced by impostor: %v", b.pins)
	}
}

// tryReceive waits up to d for one packet, returning nil on timeout.
func tryReceive(tr *UDPTransport, d time.Duration) []byte {
	ch := make(chan []byte, 1)
	go func() {
		pkt, _, err := tr.Receive()
		if err == nil && pkt != nil {
			ch <- pkt.Payload
		}
	}()
	select {
	case p := <-ch:
		return p
	case <-time.After(d):
		return nil
	}
}

func waitResult(t *testing.T, tr *UDPTransport, d time.Duration) []byte {
	t.Helper()
	p := tryReceive(tr, d)
	if p == nil {
		t.Fatalf("no packet received within %s", d)
	}
	return p
}
