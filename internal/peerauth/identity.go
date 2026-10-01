// Package peerauth authenticates traffic on the public peer port with
// mutual TLS and pinned keys. Every node has one long-lived Ed25519 key;
// its pin (a hash of the public key) is its identity. There is no CA:
// each side checks the other's key against the pin it stored when the two
// were paired, and a key nobody pinned fails the TLS handshake before any
// HTTP is read. Pairing itself runs over the same mTLS, using a temporary
// key carried in a one-time invite string (see Invite).
package peerauth

import (
	"context"
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	"time"
)

// identityKeySetting is the settings row holding this node's private key
// seed. Losing it means re-pairing with every peer and client.
const identityKeySetting = "identity_key"

// Identity is a node's (or an invite's temporary) key pair, as the TLS
// certificate it presents and the pin others know it by.
type Identity struct {
	cert tls.Certificate
	pin  string
}

// LoadOrCreate returns this node's identity, generating and persisting a
// new key on first boot.
func LoadOrCreate(ctx context.Context, db *sql.DB) (*Identity, error) {
	seed := make([]byte, ed25519.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		return nil, fmt.Errorf("generating identity key: %w", err)
	}
	// DO NOTHING keeps an existing key: only a first boot writes one.
	if _, err := db.ExecContext(ctx, `INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO NOTHING`,
		identityKeySetting, base64.StdEncoding.EncodeToString(seed)); err != nil {
		return nil, fmt.Errorf("saving identity key: %w", err)
	}
	var stored string
	if err := db.QueryRowContext(ctx, `SELECT value FROM settings WHERE key = ?`, identityKeySetting).Scan(&stored); err != nil {
		return nil, fmt.Errorf("reading identity key: %w", err)
	}
	seed, err := base64.StdEncoding.DecodeString(stored)
	if err != nil {
		return nil, fmt.Errorf("decoding identity key: %w", err)
	}
	return FromSeed(seed)
}

// FromSeed builds the identity for an Ed25519 private key seed. The
// self-signed certificate is regenerated on every call; only the key
// matters, since peers pin the key, not the certificate.
func FromSeed(seed []byte) (*Identity, error) {
	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("identity key seed must be %d bytes, got %d", ed25519.SeedSize, len(seed))
	}
	key := ed25519.NewKeyFromSeed(seed)
	pin, err := pinOfKey(key.Public())
	if err != nil {
		return nil, err
	}
	// No subject or names: a scanner completing a handshake learns nothing
	// about what is listening. Validity is irrelevant to pinning, so the
	// window just has to cover any plausible clock.
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-24 * time.Hour),
		NotAfter:     time.Now().AddDate(100, 0, 0),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	if err != nil {
		return nil, fmt.Errorf("creating identity certificate: %w", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("parsing identity certificate: %w", err)
	}
	return &Identity{
		cert: tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf},
		pin:  pin,
	}, nil
}

// Pin is this identity's public key pin, as stored by whoever trusts it.
func (id *Identity) Pin() string { return id.pin }

// Certificate is the self-signed certificate this identity presents.
func (id *Identity) Certificate() tls.Certificate { return id.cert }

// PinOf returns cert's public key pin: unpadded base64url of the SHA-256 of
// its SubjectPublicKeyInfo.
func PinOf(cert *x509.Certificate) string {
	sum := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func pinOfKey(pub crypto.PublicKey) (string, error) {
	spki, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return "", fmt.Errorf("encoding public key: %w", err)
	}
	sum := sha256.Sum256(spki)
	return base64.RawURLEncoding.EncodeToString(sum[:]), nil
}

// pinOfSeed is the pin of the key a seed generates, without building a
// certificate.
func pinOfSeed(seed []byte) (string, error) {
	if len(seed) != ed25519.SeedSize {
		return "", errors.New("bad key seed length")
	}
	return pinOfKey(ed25519.NewKeyFromSeed(seed).Public())
}

// ValidPin reports whether s is shaped like a pin (base64url SHA-256).
func ValidPin(s string) bool {
	b, err := base64.RawURLEncoding.DecodeString(s)
	return err == nil && len(b) == sha256.Size
}
