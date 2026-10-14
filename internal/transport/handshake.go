package transport

import (
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/relaymesh/relaymesh/api/proto"
	"github.com/relaymesh/relaymesh/internal/identity"
	"github.com/relaymesh/relaymesh/internal/node"
)

const (
	// flightTimeout is how long an unanswered init is retried on the
	// next Connect/Send tick.
	flightTimeout = 1500 * time.Millisecond
	// maxHandshakeAttempts bounds consecutive failed attempts before a
	// cooldown, so a plaintext or unreachable peer cannot cause an
	// unbounded init storm.
	maxHandshakeAttempts = 6
	handshakeCooldown    = 30 * time.Second
)

// handshakeFlight is our outstanding init, kept so the response can
// be bound back to the exact transcript we signed.
type handshakeFlight struct {
	peer   node.NodeID
	sid    [sidLen]byte
	init   *pb.HandshakeInit
	eph    *ecdh.PrivateKey
	sentAt time.Time
}

// SetSecure enables (encrypt=true, ident non-nil) the authenticated
// encryption mode on the data plane. encrypt without an identity is
// ignored: mutual authentication requires keys.
func (u *UDPTransport) SetSecure(ident *identity.Identity, encrypt bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.ident = ident
	u.encryptMode = encrypt && ident != nil
}

// SecureEnabled reports whether encrypted mode is active.
func (u *UDPTransport) SecureEnabled() bool {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.encryptMode
}

// SecureStats returns encrypted data-plane counters.
func (u *UDPTransport) SecureStats() SecureStats {
	u.mu.RLock()
	sessions := len(u.sessions)
	u.mu.RUnlock()
	return SecureStats{
		Sessions:           sessions,
		HandshakeFailures:  u.handshakeFailures.Load(),
		DecryptFailures:    u.decryptFailures.Load(),
		ReplayRejects:      u.replayRejects.Load(),
		PlaintextRejects:   u.plaintextRejects.Load(),
		SecureRejects:      u.secureRejects.Load(),
		IdentityMismatches: u.identityMismatches.Load(),
	}
}

// DropSession forgets a peer's session, flights, and attempt budget
// (used when discovery loses the peer, so a restart re-handshakes).
func (u *UDPTransport) DropSession(peer node.NodeID) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if s, ok := u.sessions[peer]; ok {
		delete(u.sidIndex, s.sid)
		delete(u.sessions, peer)
	}
	delete(u.flights, peer)
	delete(u.attempts, peer)
	delete(u.cooldowns, peer)
}

// advertisedAddrLocked is where peers must reply: our listen socket.
func (u *UDPTransport) advertisedAddrLocked() string {
	host := u.listenHost
	if host == "" {
		host = u.localIP
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, itoa(u.listenPort))
}

// startHandshake sends a handshake init when no session and no live
// flight exist, honoring the attempt cap and cooldown. Called from
// Connect and from Send whenever a session is missing, so both sides
// converge within one discovery sync tick.
func (u *UDPTransport) startHandshake(peer node.NodeID) {
	u.mu.Lock()
	if !u.encryptMode || u.ident == nil {
		u.mu.Unlock()
		return
	}
	if _, ok := u.sessions[peer]; ok {
		u.mu.Unlock()
		return
	}
	now := time.Now()
	if f, ok := u.flights[peer]; ok {
		if now.Sub(f.sentAt) < flightTimeout {
			u.mu.Unlock()
			return
		}
		delete(u.flights, peer)
	}
	if u.attempts[peer] >= maxHandshakeAttempts {
		if until, ok := u.cooldowns[peer]; ok && now.Before(until) {
			u.mu.Unlock()
			return
		}
		u.attempts[peer] = 0
	}
	conn := u.peers[peer]
	eph, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		u.mu.Unlock()
		return
	}
	var sid [sidLen]byte
	if _, err := rand.Read(sid[:]); err != nil {
		u.mu.Unlock()
		return
	}
	im := buildInit(u.ident, u.nodeID, peer, u.advertisedAddrLocked(), eph, sid, u.psk)
	u.flights[peer] = &handshakeFlight{peer: peer, sid: sid, init: im, eph: eph, sentAt: now}
	u.attempts[peer]++
	attempts := u.attempts[peer]
	u.mu.Unlock()

	frame, err := marshalSecure(kindInit, sid, im)
	if err != nil {
		return
	}
	if conn != nil {
		_, _ = conn.Write(frame)
	}
	if attempts >= maxHandshakeAttempts {
		u.mu.Lock()
		u.cooldowns[peer] = time.Now().Add(handshakeCooldown)
		u.mu.Unlock()
	}
}

// handleInit processes a peer's init as the responder: verify
// (signature, PSK, pin), tie-break simultaneous opens, derive keys,
// and reply to the initiator's advertised listen address.
func (u *UDPTransport) handleInit(frame []byte) {
	var im pb.HandshakeInit
	if err := proto.Unmarshal(securePayload(frame), &im); err != nil {
		u.handshakeFailures.Add(1)
		return
	}
	from := node.NodeID(im.From)

	u.mu.Lock()
	if !u.encryptMode || u.ident == nil {
		u.mu.Unlock()
		u.secureRejects.Add(1)
		return
	}
	if from == u.nodeID {
		u.mu.Unlock()
		return
	}
	sid := frameSID(frame)
	if !sameSID(sid[:], im.SessionId) {
		u.mu.Unlock()
		return
	}
	if err := verifyInit(&im, u.psk, u.nodeID); err != nil {
		u.handshakeFailures.Add(1)
		u.mu.Unlock()
		return
	}
	fp := identity.Fingerprint(ed25519.PublicKey(im.IdentityPub))
	if pin, ok := u.pins[from]; ok && pin != fp {
		u.identityMismatches.Add(1)
		u.mu.Unlock()
		return
	}
	if s, ok := u.sessions[from]; ok && s.sid == sid {
		u.mu.Unlock() // duplicate of a completed handshake
		return
	}
	if f, ok := u.flights[from]; ok {
		if string(u.nodeID) < string(from) {
			_ = f
			u.mu.Unlock() // we win the initiator role; ignore theirs
			return
		}
		delete(u.flights, from) // abandon ours, take the responder role
	}
	if other, ok := u.sidIndex[sid]; ok && other.peer != from {
		u.handshakeFailures.Add(1)
		u.mu.Unlock()
		return
	}

	eph, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		u.mu.Unlock()
		return
	}
	rm := buildResp(u.ident, u.nodeID, &im, eph, u.psk)
	sess, err := deriveSession(u.psk, &im, rm, eph, im.EphemeralPub, false, sid, from, fp)
	if err != nil {
		u.handshakeFailures.Add(1)
		u.mu.Unlock()
		return
	}
	u.installSessionLocked(sess)
	if _, ok := u.pins[from]; !ok {
		u.pins[from] = fp
	}
	dataAddr := im.DataAddr
	conn := u.conn
	u.mu.Unlock()

	frameResp, err := marshalSecure(kindResp, sid, rm)
	if err != nil || conn == nil {
		return
	}
	addr, err := net.ResolveUDPAddr("udp4", dataAddr)
	if err != nil {
		return
	}
	_, _ = conn.WriteToUDP(frameResp, addr)
}

// handleResp completes our flight as the initiator.
func (u *UDPTransport) handleResp(frame []byte) {
	var rm pb.HandshakeResp
	if err := proto.Unmarshal(securePayload(frame), &rm); err != nil {
		u.handshakeFailures.Add(1)
		return
	}
	from := node.NodeID(rm.From)

	u.mu.Lock()
	if !u.encryptMode || u.ident == nil {
		u.mu.Unlock()
		u.secureRejects.Add(1)
		return
	}
	f, ok := u.flights[from]
	if !ok {
		u.mu.Unlock() // late or duplicate response
		return
	}
	if frameSID(frame) != f.sid {
		u.handshakeFailures.Add(1)
		u.mu.Unlock()
		return
	}
	if err := verifyResp(&rm, f.init, u.psk, u.nodeID); err != nil {
		u.handshakeFailures.Add(1)
		delete(u.flights, from)
		u.mu.Unlock()
		return
	}
	fp := identity.Fingerprint(ed25519.PublicKey(rm.IdentityPub))
	if pin, ok := u.pins[from]; ok && pin != fp {
		u.identityMismatches.Add(1)
		delete(u.flights, from)
		u.mu.Unlock()
		return
	}
	sess, err := deriveSession(u.psk, f.init, &rm, f.eph, rm.EphemeralPub, true, f.sid, from, fp)
	if err != nil {
		u.handshakeFailures.Add(1)
		delete(u.flights, from)
		u.mu.Unlock()
		return
	}
	u.installSessionLocked(sess)
	if _, ok := u.pins[from]; !ok {
		u.pins[from] = fp
	}
	u.mu.Unlock()
}

// installSessionLocked replaces any previous session for the peer
// (rekey) and indexes the new session id.
func (u *UDPTransport) installSessionLocked(s *session) {
	if old, ok := u.sessions[s.peer]; ok {
		delete(u.sidIndex, old.sid)
	}
	if clash, ok := u.sidIndex[s.sid]; ok && clash.peer != s.peer {
		delete(u.sidIndex, clash.sid)
	}
	u.sessions[s.peer] = s
	u.sidIndex[s.sid] = s
	delete(u.flights, s.peer)
	delete(u.attempts, s.peer)
	delete(u.cooldowns, s.peer)
}

// handleSecureFrame dispatches a received RMS frame.
func (u *UDPTransport) handleSecureFrame(frame []byte) {
	kind, _, _, ok := parseFrameHeader(frame)
	if !ok {
		return
	}
	u.mu.RLock()
	enc := u.encryptMode
	u.mu.RUnlock()
	if !enc {
		u.secureRejects.Add(1)
		return
	}
	switch kind {
	case kindData:
		u.handleDataFrame(frame)
	case kindInit:
		u.handleInit(frame)
	case kindResp:
		u.handleResp(frame)
	default:
		u.decryptFailures.Add(1)
	}
}

// handleDataFrame authenticates, decrypts, and enqueues one packet.
func (u *UDPTransport) handleDataFrame(frame []byte) {
	u.mu.RLock()
	sess := u.sidIndex[frameSID(frame)]
	psk := u.psk
	u.mu.RUnlock()
	if sess == nil {
		u.decryptFailures.Add(1)
		return
	}
	plain, err := sess.open(frame)
	if err != nil {
		if errors.Is(err, errReplay) {
			u.replayRejects.Add(1)
		} else {
			u.decryptFailures.Add(1)
		}
		return
	}

	var msg pb.RelayPacket
	if err := proto.Unmarshal(plain, &msg); err != nil {
		u.decryptFailures.Add(1)
		return
	}
	if msg.Version != PacketVersion {
		u.decryptFailures.Add(1)
		return
	}
	// Phase-9 PSK tag still applies inside the tunnel (defense in
	// depth): the ciphertext proves the link, the tag the network.
	if len(psk) > 0 && !verifyTag(psk, &msg) {
		u.authFailures.Add(1)
		return
	}
	if node.NodeID(msg.Source) == u.nodeID {
		return
	}

	pkt := &Packet{
		Source:      node.NodeID(msg.Source),
		Destination: node.NodeID(msg.Destination),
		Payload:     msg.Payload,
		Sequence:    msg.Sequence,
		ID:          msg.PacketId,
		TTL:         msg.Ttl,
		Path:        stringsToNodeIDs(msg.Path),
		Priority:    msg.Priority,
	}
	select {
	case u.recvChan <- &udpReceived{pkt: pkt, from: pkt.Source}:
	default:
		u.decryptFailures.Add(1)
	}
}

func itoa(v uint16) string {
	if v == 0 {
		return "0"
	}
	var b [5]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}
