package identity

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadOrCreatePersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keys", "node.key")

	id1, created, err := LoadOrCreate(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !created {
		t.Fatalf("first load should create")
	}

	id2, created, err := LoadOrCreate(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if created {
		t.Fatalf("second load must not regenerate")
	}
	if Fingerprint(id1.PublicKey()) != Fingerprint(id2.PublicKey()) {
		t.Fatalf("fingerprint changed across reload: %s vs %s",
			Fingerprint(id1.PublicKey()), Fingerprint(id2.PublicKey()))
	}

	// Private seed must never be world-readable.
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("identity file perms = %o, want 600", fi.Mode().Perm())
	}

	// Sign/verify round trip.
	msg := []byte("transcript-bytes")
	sig := id1.Sign(msg)
	if !Verify(id2.PublicKey(), msg, sig) {
		t.Fatalf("signature from reloaded key failed verification")
	}
	msg[0] ^= 0xff
	if Verify(id2.PublicKey(), msg, sig) {
		t.Fatalf("tampered message verified")
	}
}

func TestFingerprintsDistinct(t *testing.T) {
	a, _ := Generate()
	b, _ := Generate()
	if Fingerprint(a.PublicKey()) == Fingerprint(b.PublicKey()) {
		t.Fatalf("distinct keys share a fingerprint")
	}
	if len(Fingerprint(a.PublicKey())) != 32 {
		t.Fatalf("unexpected fingerprint length")
	}
}
