package transport

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/proto"

	pb "github.com/relaymesh/relaymesh/api/proto"
	"github.com/relaymesh/relaymesh/internal/identity"
	"github.com/relaymesh/relaymesh/internal/node"
)

// Secure wire frame (Phase 10). All fields up to and including the
// counter are authenticated as AEAD associated data:
//
//	0..2   magic "RMS"
//	3      frame version
//	4      kind (1=data, 2=handshake init, 3=handshake resp)
//	5..8   session id
//	9..16  counter (big endian; 0 on handshake frames)
//	17..   payload (AES-256-GCM ciphertext for data frames,
//	        plaintext protobuf for handshake frames)
const (
	frameVersion byte = 1
	kindData     byte = 1
	kindInit     byte = 2
	kindResp     byte = 3

	hdrLen         = 17
	sidLen         = 4
	keyLen         = 32
	saltLen        = 4
	keyMaterialLen = keyLen*2 + saltLen*2 // 72
	gcmNonceLen    = 12
	hsNonceLen     = 16
	replayWindow   = 128

	// handshakeVersion is signed into every handshake transcript.
	handshakeVersion uint32 = 1
)

var frameMagic = [3]byte{'R', 'M', 'S'}

var (
	errReplay    = errors.New("replayed or stale counter")
	errDecrypt   = errors.New("AEAD authentication failed")
	errNoSession = errors.New("no session for sid")
)

// SecureStats reports encrypted data-plane health.
type SecureStats struct {
	Sessions           int
	HandshakeFailures  uint64
	DecryptFailures    uint64
	ReplayRejects      uint64
	PlaintextRejects   uint64
	SecureRejects      uint64
	IdentityMismatches uint64
}

func isSecureFrame(b []byte) bool {
	return len(b) >= hdrLen && b[0] == frameMagic[0] && b[1] == frameMagic[1] && b[2] == frameMagic[2]
}

func frameHeader(kind byte, sid [sidLen]byte, ctr uint64) []byte {
	h := make([]byte, hdrLen)
	copy(h, frameMagic[:])
	h[3] = frameVersion
	h[4] = kind
	copy(h[5:9], sid[:])
	binary.BigEndian.PutUint64(h[9:17], ctr)
	return h
}

func parseFrameHeader(b []byte) (kind byte, sid [sidLen]byte, ctr uint64, ok bool) {
	if !isSecureFrame(b) || b[3] != frameVersion {
		return 0, sid, 0, false
	}
	copy(sid[:], b[5:9])
	return b[4], sid, binary.BigEndian.Uint64(b[9:17]), true
}

// session holds pairwise keys for one authenticated link. Nonces are
// direction salts (random per session) plus a per-packet counter, so
// a key never reuses a nonce; a 128-bit sliding receive window drops
// replays while tolerating UDP reordering.
type session struct {
	sid       [sidLen]byte
	peer      node.NodeID
	fp        string // pinned peer identity fingerprint
	initiator bool

	sendKey  [keyLen]byte
	recvKey  [keyLen]byte
	sendSalt [saltLen]byte
	recvSalt [saltLen]byte

	sendCtr atomic.Uint64

	rxMu   sync.Mutex
	maxRx  uint64
	rxWin  [2]uint64
	rxInit bool
}

func gcmFor(key [keyLen]byte) (cipher.AEAD, error) {
	blk, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(blk)
}

func directionNonce(salt [saltLen]byte, ctr uint64) []byte {
	n := make([]byte, gcmNonceLen)
	copy(n, salt[:])
	binary.BigEndian.PutUint64(n[4:], ctr)
	return n
}

// seal encrypts plaintext into a complete secure data frame.
func (s *session) seal(plaintext []byte) ([]byte, error) {
	ctr := s.sendCtr.Add(1)
	hdr := frameHeader(kindData, s.sid, ctr)
	aead, err := gcmFor(s.sendKey)
	if err != nil {
		return nil, err
	}
	ct := aead.Seal(nil, directionNonce(s.sendSalt, ctr), plaintext, hdr)
	return append(hdr, ct...), nil
}

// open authenticates and decrypts a secure data frame, enforcing the
// replay window before touching the key.
func (s *session) open(frame []byte) ([]byte, error) {
	kind, sid, ctr, ok := parseFrameHeader(frame)
	if !ok || kind != kindData || sid != s.sid {
		return nil, errDecrypt
	}
	if !s.acceptRx(ctr) {
		return nil, errReplay
	}
	aead, err := gcmFor(s.recvKey)
	if err != nil {
		return nil, err
	}
	pt, err := aead.Open(nil, directionNonce(s.recvSalt, ctr), frame[hdrLen:], frame[:hdrLen])
	if err != nil {
		return nil, errDecrypt
	}
	return pt, nil
}

// acceptRx implements a WireGuard-style 128-bit sliding window:
// bit offset maxRx-ctr must be inside the window and not yet seen.
func (s *session) acceptRx(ctr uint64) bool {
	s.rxMu.Lock()
	defer s.rxMu.Unlock()
	if !s.rxInit {
		s.rxInit = true
		s.maxRx = ctr
		s.rxWin = [2]uint64{1, 0}
		return true
	}
	if ctr > s.maxRx {
		shift := ctr - s.maxRx
		if shift >= replayWindow {
			s.rxWin = [2]uint64{}
		} else if shift >= 64 {
			s.rxWin[1] = s.rxWin[0] << (shift - 64)
			s.rxWin[0] = 0
		} else {
			s.rxWin[1] = s.rxWin[1]<<shift | s.rxWin[0]>>(64-shift)
			s.rxWin[0] <<= shift
		}
		s.maxRx = ctr
		s.rxWin[0] |= 1
		return true
	}
	off := s.maxRx - ctr
	if off >= replayWindow {
		return false
	}
	word, bit := off/64, off%64
	mask := uint64(1) << bit
	if s.rxWin[word]&mask != 0 {
		return false
	}
	s.rxWin[word] |= mask
	return true
}

// --- handshake transcripts -----------------------------------------
//
// Transcripts are length-prefixed concatenations under distinct
// domain strings, so no field boundary is ambiguous. Signatures and
// PSK proofs cover transcripts only (never signature fields).

func appendField(buf, f []byte) []byte {
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(f)))
	buf = append(buf, l[:]...)
	return append(buf, f...)
}

func appendString(buf []byte, s string) []byte { return appendField(buf, []byte(s)) }

func initTranscript(m *pb.HandshakeInit) []byte {
	buf := []byte("RelayMesh/v1/handshake-init")
	var v [4]byte
	binary.BigEndian.PutUint32(v[:], m.Version)
	buf = appendField(buf, v[:])
	buf = appendString(buf, m.From)
	buf = appendString(buf, m.To)
	buf = appendField(buf, m.IdentityPub)
	buf = appendField(buf, m.EphemeralPub)
	buf = appendField(buf, m.Nonce)
	buf = appendString(buf, m.DataAddr)
	buf = appendField(buf, m.SessionId)
	return buf
}

func respTranscript(im *pb.HandshakeInit, m *pb.HandshakeResp) []byte {
	initHash := sha256.Sum256(initTranscript(im))
	buf := []byte("RelayMesh/v1/handshake-resp")
	var v [4]byte
	binary.BigEndian.PutUint32(v[:], m.Version)
	buf = appendField(buf, v[:])
	buf = appendString(buf, m.From)
	buf = appendString(buf, m.To)
	buf = appendField(buf, initHash[:])
	buf = appendField(buf, m.IdentityPub)
	buf = appendField(buf, m.EphemeralPub)
	buf = appendField(buf, m.Nonce)
	return buf
}

func pskProof(psk, transcript []byte) []byte {
	h := hmac.New(sha256.New, psk)
	h.Write(transcript)
	return h.Sum(nil)
}

func verifyPSK(psk, proof, transcript []byte) bool {
	if len(psk) == 0 {
		return len(proof) == 0
	}
	if len(proof) != sha256.Size {
		return false
	}
	return subtle.ConstantTimeCompare(proof, pskProof(psk, transcript)) == 1
}

// deriveSession performs the X25519 exchange and HKDF derivation.
// initiator selects which direction salt/key pair is send vs recv.
func deriveSession(psk []byte, im *pb.HandshakeInit, resp *pb.HandshakeResp,
	myEph *ecdh.PrivateKey, peerEphPub []byte, initiator bool,
	sid [sidLen]byte, peer node.NodeID, fp string) (*session, error) {

	remote, err := ecdh.X25519().NewPublicKey(peerEphPub)
	if err != nil {
		return nil, fmt.Errorf("peer ephemeral key: %w", err)
	}
	shared, err := myEph.ECDH(remote)
	if err != nil {
		return nil, fmt.Errorf("x25519 exchange: %w", err)
	}

	th := sha256.New()
	th.Write(initTranscript(im))
	th.Write(respTranscript(im, resp))

	ikm := shared
	if len(psk) > 0 {
		ikm = append(append([]byte{}, shared...), psk...)
	}
	material, err := hkdf.Key(sha256.New, ikm, th.Sum(nil), "RelayMesh/v1/session", keyMaterialLen)
	if err != nil {
		return nil, fmt.Errorf("hkdf: %w", err)
	}

	var kI2R, kR2I [keyLen]byte
	var sI2R, sR2I [saltLen]byte
	copy(kI2R[:], material[0:keyLen])
	copy(kR2I[:], material[keyLen:keyLen*2])
	copy(sI2R[:], material[keyLen*2:keyLen*2+saltLen])
	copy(sR2I[:], material[keyLen*2+saltLen:keyMaterialLen])

	s := &session{sid: sid, peer: peer, fp: fp, initiator: initiator}
	if initiator {
		s.sendKey, s.recvKey = kI2R, kR2I
		s.sendSalt, s.recvSalt = sI2R, sR2I
	} else {
		s.sendKey, s.recvKey = kR2I, kI2R
		s.sendSalt, s.recvSalt = sR2I, sI2R
	}
	return s, nil
}

// buildInit creates and signs a handshake init message.
func buildInit(id *identity.Identity, myID, peerID node.NodeID,
	dataAddr string, eph *ecdh.PrivateKey, sid [sidLen]byte, psk []byte) *pb.HandshakeInit {

	nonce := make([]byte, hsNonceLen)
	_, _ = rand.Read(nonce)
	m := &pb.HandshakeInit{
		Version:      handshakeVersion,
		From:         string(myID),
		To:           string(peerID),
		IdentityPub:  id.PublicKey(),
		EphemeralPub: eph.PublicKey().Bytes(),
		Nonce:        nonce,
		DataAddr:     dataAddr,
		SessionId:    sid[:],
	}
	m.Signature = id.Sign(initTranscript(m))
	if len(psk) > 0 {
		m.PskProof = pskProof(psk, initTranscript(m))
	}
	return m
}

// buildResp creates and signs a handshake response bound to im.
func buildResp(id *identity.Identity, myID node.NodeID, im *pb.HandshakeInit,
	eph *ecdh.PrivateKey, psk []byte) *pb.HandshakeResp {

	nonce := make([]byte, hsNonceLen)
	_, _ = rand.Read(nonce)
	m := &pb.HandshakeResp{
		Version:      handshakeVersion,
		From:         string(myID),
		To:           im.From,
		IdentityPub:  id.PublicKey(),
		EphemeralPub: eph.PublicKey().Bytes(),
		Nonce:        nonce,
	}
	m.Signature = id.Sign(respTranscript(im, m))
	if len(psk) > 0 {
		m.PskProof = pskProof(psk, respTranscript(im, m))
	}
	return m
}

// verifyInit checks structural validity, self-signature, and the PSK
// proof (when configured). The name-to-key binding is the caller's
// pin check.
func verifyInit(m *pb.HandshakeInit, psk []byte, expectTo node.NodeID) error {
	if m.Version != handshakeVersion {
		return errors.New("unsupported handshake version")
	}
	if node.NodeID(m.To) != expectTo {
		return errors.New("handshake addressed to another node")
	}
	if len(m.IdentityPub) != ed25519.PublicKeySize || len(m.EphemeralPub) != 32 ||
		len(m.Nonce) != hsNonceLen || len(m.SessionId) != sidLen || m.DataAddr == "" {
		return errors.New("malformed handshake init")
	}
	tr := initTranscript(m)
	if !identity.Verify(ed25519.PublicKey(m.IdentityPub), tr, m.Signature) {
		return errors.New("bad init signature")
	}
	if !verifyPSK(psk, m.PskProof, tr) {
		return errors.New("bad init PSK proof")
	}
	return nil
}

// verifyResp checks the response and its binding to our exact init.
func verifyResp(m *pb.HandshakeResp, im *pb.HandshakeInit, psk []byte, expectTo node.NodeID) error {
	if m.Version != handshakeVersion {
		return errors.New("unsupported handshake version")
	}
	if node.NodeID(m.To) != expectTo || node.NodeID(m.From) != node.NodeID(im.To) {
		return errors.New("handshake response mismatch")
	}
	if len(m.IdentityPub) != ed25519.PublicKeySize || len(m.EphemeralPub) != 32 ||
		len(m.Nonce) != hsNonceLen {
		return errors.New("malformed handshake response")
	}
	tr := respTranscript(im, m)
	if !identity.Verify(ed25519.PublicKey(m.IdentityPub), tr, m.Signature) {
		return errors.New("bad response signature")
	}
	if !verifyPSK(psk, m.PskProof, tr) {
		return errors.New("bad response PSK proof")
	}
	return nil
}

func marshalSecure(kind byte, sid [sidLen]byte, msg proto.Message) ([]byte, error) {
	raw, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return append(frameHeader(kind, sid, 0), raw...), nil
}

func securePayload(frame []byte) []byte { return frame[hdrLen:] }

func frameSID(frame []byte) [sidLen]byte {
	var sid [sidLen]byte
	if len(frame) >= hdrLen {
		copy(sid[:], frame[5:9])
	}
	return sid
}

// sameSID reports whether two session-id byte slices are equal.
func sameSID(a, b []byte) bool { return bytes.Equal(a, b) }
