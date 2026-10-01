package peerauth

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// invitePrefix versions the invite string format.
const invitePrefix = "jellysync1:"

// Invite is what a provider hands a consumer to pair with it, as one
// opaque string. It carries a temporary private key: whoever holds the
// string can pair once, so it must travel privately. Seed is the key the
// consumer authenticates the pairing call with; Pin is the provider's own
// key, so the consumer can't be pointed at an impostor.
type Invite struct {
	URL       string `json:"url"`
	NodeID    string `json:"node_id,omitempty"`
	Pin       string `json:"pin"`
	Seed      []byte `json:"seed"`
	ExpiresAt int64  `json:"exp"`
}

// Encode renders the invite as its shareable string.
func (inv Invite) Encode() string {
	b, _ := json.Marshal(inv)
	return invitePrefix + base64.RawURLEncoding.EncodeToString(b)
}

// Expired reports whether the invite is past its expiry. The provider
// enforces this on its own clock; checking it here just fails early.
func (inv Invite) Expired(now time.Time) bool {
	return !now.Before(time.Unix(inv.ExpiresAt, 0))
}

// ParseInvite decodes and validates an invite string. Surrounding
// whitespace is ignored, since it's typically pasted.
func ParseInvite(s string) (Invite, error) {
	s = strings.TrimSpace(s)
	body, ok := strings.CutPrefix(s, invitePrefix)
	if !ok {
		return Invite{}, errors.New("not a jellysync invite")
	}
	raw, err := base64.RawURLEncoding.DecodeString(body)
	if err != nil {
		return Invite{}, errors.New("invite is corrupted (bad encoding)")
	}
	var inv Invite
	if err := json.Unmarshal(raw, &inv); err != nil {
		return Invite{}, errors.New("invite is corrupted (bad content)")
	}
	if inv.URL, err = CanonicalURL(inv.URL); err != nil {
		return Invite{}, fmt.Errorf("invite has a bad address: %w", err)
	}
	if !ValidPin(inv.Pin) {
		return Invite{}, errors.New("invite has a bad key pin")
	}
	if _, err := pinOfSeed(inv.Seed); err != nil {
		return Invite{}, errors.New("invite has a bad key")
	}
	if inv.ExpiresAt <= 0 {
		return Invite{}, errors.New("invite has no expiry")
	}
	return inv, nil
}

// PairRequest is the body of POST /api/v1/pair: the consumer's permanent
// key pin and its node name.
type PairRequest struct {
	Pin    string `json:"pin"`
	NodeID string `json:"node_id"`
}

// PairResponse is the provider's answer to a successful pairing.
type PairResponse struct {
	NodeID  string `json:"node_id"`
	Version string `json:"version,omitempty"`
}

// ErrInviteRejected means the provider refused the invite's key: it was
// already used, cancelled, or expired.
var ErrInviteRejected = errors.New("invite was rejected: already used, cancelled or expired")

// Pair redeems inv against the node that issued it, registering pin (this
// node's permanent key) as an authorized client there. The call is mTLS
// with the invite's temporary key, and the server must present inv.Pin.
func Pair(ctx context.Context, inv Invite, pin, nodeID string) (PairResponse, error) {
	tmp, err := FromSeed(inv.Seed)
	if err != nil {
		return PairResponse{}, err
	}
	transport := NewTransport(tmp, func(string) (string, bool) { return inv.Pin, true })
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 15 * time.Second}

	body, _ := json.Marshal(PairRequest{Pin: pin, NodeID: nodeID})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, inv.URL+"/api/v1/pair", bytes.NewReader(body))
	if err != nil {
		return PairResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		if errors.Is(err, ErrPinMismatch) {
			return PairResponse{}, fmt.Errorf("the node at %s is not the one that created this invite: %w", inv.URL, ErrPinMismatch)
		}
		if isTLSRejection(err) {
			return PairResponse{}, ErrInviteRejected
		}
		return PairResponse{}, fmt.Errorf("contacting %s: %w", inv.URL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusGone {
			return PairResponse{}, ErrInviteRejected
		}
		return PairResponse{}, fmt.Errorf("pairing with %s: status %d: %s", inv.URL, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	var out PairResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return PairResponse{}, fmt.Errorf("decoding pairing response: %w", err)
	}
	return out, nil
}

// isTLSRejection reports whether err is the server refusing our client
// certificate. In TLS 1.3 the client finishes its side of the handshake
// before the server checks the client key, so the refusal arrives as an
// alert on the first read rather than as a handshake error.
func isTLSRejection(err error) bool {
	s := err.Error()
	return strings.Contains(s, "remote error: tls:")
}
