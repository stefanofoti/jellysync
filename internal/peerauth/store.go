package peerauth

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"jellysync/internal/logging"
)

// touchEvery throttles how often a client's last_seen_at is written: it's
// a dashboard hint, not worth a DB write per request.
const touchEvery = time.Minute

var (
	ErrNotFound = errors.New("not found")
	// ErrInviteInvalid means the pending invite a pairing call came in
	// with was used, cancelled or expired in the meantime.
	ErrInviteInvalid = errors.New("invite no longer valid")
	ErrIDTaken       = errors.New("client id already taken")
)

// Client is a node authorized to read this node's catalog and stream from
// it (a consumer that redeemed one of our invites). It is not a peer: this
// node never dials it.
type Client struct {
	ID         string    `json:"id"`
	Pin        string    `json:"pin"`
	Name       string    `json:"name"`
	CreatedAt  time.Time `json:"created_at"`
	LastSeenAt time.Time `json:"last_seen_at,omitzero"`
}

// PendingInvite is an issued, not yet redeemed invite. Only its temporary
// key's pin is kept: the private half exists only in the invite string.
type PendingInvite struct {
	Pin       string    `json:"pin"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Store holds authorized clients and pending invites, in the DB and
// mirrored in memory: they're consulted on every TLS handshake and request
// on the peer port.
type Store struct {
	db  *sql.DB
	now func() time.Time

	mu      sync.RWMutex
	clients map[string]*Client       // by pin
	invites map[string]PendingInvite // by pin
}

// NewStore loads clients and pending invites. now is the clock invite
// expiry is judged by (time.Now outside tests).
func NewStore(ctx context.Context, db *sql.DB, now func() time.Time) (*Store, error) {
	s := &Store{db: db, now: now, clients: make(map[string]*Client), invites: make(map[string]PendingInvite)}

	rows, err := db.QueryContext(ctx, `SELECT id, pin, name, created_at, COALESCE(last_seen_at, 0) FROM clients`)
	if err != nil {
		return nil, fmt.Errorf("loading clients: %w", err)
	}
	for rows.Next() {
		var c Client
		var created, seen int64
		if err := rows.Scan(&c.ID, &c.Pin, &c.Name, &created, &seen); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scanning client: %w", err)
		}
		c.CreatedAt = time.Unix(created, 0)
		if seen > 0 {
			c.LastSeenAt = time.Unix(seen, 0)
		}
		s.clients[c.Pin] = &c
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("loading clients: %w", err)
	}

	rows, err = db.QueryContext(ctx, `SELECT pin, created_at, expires_at FROM invites`)
	if err != nil {
		return nil, fmt.Errorf("loading invites: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var inv PendingInvite
		var created, expires int64
		if err := rows.Scan(&inv.Pin, &created, &expires); err != nil {
			return nil, fmt.Errorf("scanning invite: %w", err)
		}
		inv.CreatedAt, inv.ExpiresAt = time.Unix(created, 0), time.Unix(expires, 0)
		s.invites[inv.Pin] = inv
	}
	return s, rows.Err()
}

// CreateInvite issues a pending invite valid for ttl, returning the
// temporary key's seed (for the invite string only — it isn't stored).
func (s *Store) CreateInvite(ctx context.Context, ttl time.Duration) ([]byte, PendingInvite, error) {
	seed := make([]byte, ed25519.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		return nil, PendingInvite{}, fmt.Errorf("generating invite key: %w", err)
	}
	pin, err := pinOfSeed(seed)
	if err != nil {
		return nil, PendingInvite{}, err
	}
	now := s.now()
	inv := PendingInvite{Pin: pin, CreatedAt: now, ExpiresAt: now.Add(ttl)}

	s.sweepExpired(ctx)
	if _, err := s.db.ExecContext(ctx, `INSERT INTO invites (pin, created_at, expires_at) VALUES (?, ?, ?)`,
		inv.Pin, inv.CreatedAt.Unix(), inv.ExpiresAt.Unix()); err != nil {
		return nil, PendingInvite{}, fmt.Errorf("saving invite: %w", err)
	}
	s.mu.Lock()
	s.invites[pin] = inv
	s.mu.Unlock()
	return seed, inv, nil
}

// Invites lists unexpired pending invites, oldest first.
func (s *Store) Invites(ctx context.Context) []PendingInvite {
	s.sweepExpired(ctx)
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]PendingInvite, 0, len(s.invites))
	for _, inv := range s.invites {
		out = append(out, inv)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}

// CancelInvite withdraws a pending invite.
func (s *Store) CancelInvite(ctx context.Context, pin string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.invites[pin]; !ok {
		return ErrNotFound
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM invites WHERE pin = ?`, pin); err != nil {
		return fmt.Errorf("cancelling invite: %w", err)
	}
	delete(s.invites, pin)
	return nil
}

// PendingInvite reports whether pin is an unexpired pending invite's key.
func (s *Store) PendingInvite(pin string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	inv, ok := s.invites[pin]
	return ok && s.now().Before(inv.ExpiresAt)
}

// sweepExpired drops expired invites. Best-effort: an expired invite is
// already unusable (PendingInvite checks expiry), this just tidies up.
func (s *Store) sweepExpired(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	for pin, inv := range s.invites {
		if now.Before(inv.ExpiresAt) {
			continue
		}
		if _, err := s.db.ExecContext(ctx, `DELETE FROM invites WHERE pin = ?`, pin); err != nil {
			slog.Warn("removing expired invite", logging.Err(err))
			continue
		}
		delete(s.invites, pin)
	}
}

// Redeem consumes the pending invite tmpPin and authorizes pin as a client
// named name, with id. If pin is already a client (re-pairing), it keeps
// its id and only its name is refreshed. Exactly one concurrent Redeem of
// the same invite succeeds.
func (s *Store) Redeem(ctx context.Context, tmpPin, pin, name, id string) (Client, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	inv, ok := s.invites[tmpPin]
	if !ok || !s.now().Before(inv.ExpiresAt) {
		return Client{}, ErrInviteInvalid
	}
	c := Client{ID: id, Pin: pin, Name: name, CreatedAt: s.now()}
	if existing, ok := s.clients[pin]; ok {
		c.ID, c.CreatedAt, c.LastSeenAt = existing.ID, existing.CreatedAt, existing.LastSeenAt
	} else if s.idTakenLocked(id) {
		return Client{}, ErrIDTaken
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Client{}, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM invites WHERE pin = ?`, tmpPin); err != nil {
		return Client{}, fmt.Errorf("consuming invite: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO clients (id, pin, name, created_at) VALUES (?, ?, ?, ?)
		ON CONFLICT(pin) DO UPDATE SET name = excluded.name
	`, c.ID, c.Pin, c.Name, c.CreatedAt.Unix()); err != nil {
		return Client{}, fmt.Errorf("saving client: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Client{}, fmt.Errorf("saving client: %w", err)
	}
	delete(s.invites, tmpPin)
	s.clients[pin] = &c
	return c, nil
}

// Clients lists authorized clients by id.
func (s *Store) Clients() []Client {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Client, 0, len(s.clients))
	for _, c := range s.clients {
		out = append(out, *c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// ClientByPin returns the client authorized with pin.
func (s *Store) ClientByPin(pin string) (Client, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.clients[pin]
	if !ok {
		return Client{}, false
	}
	return *c, true
}

// IDTaken reports whether id belongs to a client. Peers and clients share
// one id namespace, so traffic keys never mix up two different nodes.
func (s *Store) IDTaken(id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.idTakenLocked(id)
}

func (s *Store) idTakenLocked(id string) bool {
	for _, c := range s.clients {
		if c.ID == id {
			return true
		}
	}
	return false
}

// IDForPin returns the id of the client authorized with pin, so a node
// that is both our client and our peer is known by one id.
func (s *Store) IDForPin(pin string) (string, bool) {
	c, ok := s.ClientByPin(pin)
	return c.ID, ok
}

// Revoke removes a client. Its next handshake fails, and requests on any
// connection it still holds are refused (see Authenticate).
func (s *Store) Revoke(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var pin string
	for p, c := range s.clients {
		if c.ID == id {
			pin = p
		}
	}
	if pin == "" {
		return ErrNotFound
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM clients WHERE pin = ?`, pin); err != nil {
		return fmt.Errorf("revoking client: %w", err)
	}
	delete(s.clients, pin)
	return nil
}

// Touch records that the client with pin was just seen, at most once per
// touchEvery.
func (s *Store) Touch(pin string) {
	now := s.now()
	s.mu.Lock()
	c, ok := s.clients[pin]
	if !ok || now.Sub(c.LastSeenAt) < touchEvery {
		s.mu.Unlock()
		return
	}
	c.LastSeenAt = now
	id := c.ID
	s.mu.Unlock()
	if _, err := s.db.Exec(`UPDATE clients SET last_seen_at = ? WHERE pin = ?`, now.Unix(), pin); err != nil {
		slog.Warn("recording client last seen", "client", id, logging.Err(err))
	}
}
