// Package identity provides per-node Ed25519 identities used for
// mutual authentication on the encrypted data plane (Phase 10).
//
// Identity files hold only the private seed and are created with
// 0600 permissions. Fingerprints are SHA-256 digests of the public
// key, used for trust-on-first-use pinning: the first successful
// handshake with a peer records its fingerprint; later mismatches
// are rejected.
package identity

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const fileFormatVersion = 1

// Identity is a node's long-term Ed25519 signing key.
type Identity struct {
	priv ed25519.PrivateKey
	pub  ed25519.PublicKey
	path string
}

type diskFormat struct {
	Version int    `json:"version"`
	Type    string `json:"type"`
	Seed    string `json:"seed"`
}

// Generate creates a fresh in-memory identity (not persisted).
func Generate() (*Identity, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate ed25519 key: %w", err)
	}
	return &Identity{priv: priv, pub: pub}, nil
}

// LoadOrCreate loads the identity at path, creating it (and parent
// directories) when absent. created reports whether a new key was
// generated. The file is written 0600.
func LoadOrCreate(path string) (*Identity, bool, error) {
	data, err := os.ReadFile(path)
	if err == nil {
		var f diskFormat
		if err := json.Unmarshal(data, &f); err != nil {
			return nil, false, fmt.Errorf("parse identity %s: %w", path, err)
		}
		if f.Type != "ed25519" {
			return nil, false, fmt.Errorf("identity %s: unsupported key type %q", path, f.Type)
		}
		seed, err := hex.DecodeString(f.Seed)
		if err != nil || len(seed) != ed25519.SeedSize {
			return nil, false, fmt.Errorf("identity %s: bad seed", path)
		}
		priv := ed25519.NewKeyFromSeed(seed)
		return &Identity{priv: priv, pub: priv.Public().(ed25519.PublicKey), path: path}, false, nil
	}
	if !os.IsNotExist(err) {
		return nil, false, fmt.Errorf("read identity %s: %w", path, err)
	}

	id, err := Generate()
	if err != nil {
		return nil, false, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, false, fmt.Errorf("create identity dir: %w", err)
	}
	out, err := json.MarshalIndent(diskFormat{
		Version: fileFormatVersion,
		Type:    "ed25519",
		Seed:    hex.EncodeToString(id.priv.Seed()),
	}, "", "  ")
	if err != nil {
		return nil, false, err
	}
	if err := os.WriteFile(path, append(out, '\n'), 0o600); err != nil {
		return nil, false, fmt.Errorf("write identity %s: %w", path, err)
	}
	id.path = path
	return id, true, nil
}

// PublicKey returns the 32-byte Ed25519 public key.
func (i *Identity) PublicKey() ed25519.PublicKey { return i.pub }

// Sign signs msg with the node's private key.
func (i *Identity) Sign(msg []byte) []byte { return ed25519.Sign(i.priv, msg) }

// Path reports where the identity is stored ("" when in-memory).
func (i *Identity) Path() string { return i.path }

// Fingerprint returns the SHA-256 fingerprint of a public key as
// lowercase hex (first 16 bytes, 32 characters).
func Fingerprint(pub ed25519.PublicKey) string {
	sum := sha256.Sum256(pub)
	return hex.EncodeToString(sum[:16])
}

// Verify checks an Ed25519 signature in constant length time.
func Verify(pub ed25519.PublicKey, msg, sig []byte) bool {
	if len(pub) != ed25519.PublicKeySize || len(sig) != ed25519.SignatureSize {
		return false
	}
	return ed25519.Verify(pub, msg, sig)
}
