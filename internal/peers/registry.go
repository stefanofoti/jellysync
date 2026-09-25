// Package peers tracks the configured peer nodes and their reachability.
package peers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"jellysync/internal/config"
)

type State string

const (
	StateOnline   State = "ONLINE"
	StateDegraded State = "DEGRADED"
	StateOffline  State = "OFFLINE"
)

const (
	heartbeatInterval   = 15 * time.Second
	heartbeatJitter     = 0.2 // +/- 20%
	heartbeatTimeout    = 5 * time.Second
	degradedAfterMisses = 1
	offlineAfterMisses  = 3
)

type Peer struct {
	ID      string
	URL     string
	State   State
	Name    string // last-known /health "node_id" reported by this peer; display label only, "" until first successful probe
	Version string // last-known /health "version" reported by this peer; "" until first successful probe

	missed int
}

type Registry struct {
	db      *sql.DB
	http    *http.Client
	baseCtx context.Context

	mu      sync.RWMutex
	peers   map[string]*Peer
	cancels map[string]context.CancelFunc
}

// NewRegistry upserts the configured peers into the peers table (so they
// exist on first boot), then loads the in-memory set from *every* row in
// the table — not just the configured ones. This means a peer added at
// runtime (e.g. via the web UI) persists across restarts even if it's
// never added to the PEERS environment variable; the DB, not PEERS, is the
// source of truth once the node has booted once.
func NewRegistry(ctx context.Context, db *sql.DB, configured []config.Peer) (*Registry, error) {
	r := &Registry{
		db:      db,
		http:    &http.Client{Timeout: heartbeatTimeout},
		baseCtx: ctx,
		peers:   make(map[string]*Peer),
		cancels: make(map[string]context.CancelFunc),
	}

	for _, p := range configured {
		_, err := db.ExecContext(ctx, `
			INSERT INTO peers (id, url, state) VALUES (?, ?, ?)
			ON CONFLICT(id) DO UPDATE SET url = excluded.url
		`, p.ID, p.URL, StateOnline)
		if err != nil {
			return nil, fmt.Errorf("seeding peer %s: %w", p.ID, err)
		}
	}

	rows, err := db.QueryContext(ctx, `SELECT id, url, state, version, name FROM peers`)
	if err != nil {
		return nil, fmt.Errorf("loading peers: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var p Peer
		var state string
		if err := rows.Scan(&p.ID, &p.URL, &state, &p.Version, &p.Name); err != nil {
			return nil, fmt.Errorf("scanning peer: %w", err)
		}
		p.State = State(state)
		r.peers[p.ID] = &p
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("loading peers: %w", err)
	}

	return r, nil
}

// Probe checks that url's /health endpoint answers 200 within
// heartbeatTimeout, and returns the "node_id" (display name) and "version"
// it reports (both empty if the peer is running a build old enough not to
// include them — that's not an error, just unknown). Used both as the
// add-peer handshake and by the heartbeat loop, so "reachable" means the
// same thing in both places.
func (r *Registry) Probe(ctx context.Context, url string) (name string, version string, err error) {
	reqCtx, cancel := context.WithTimeout(ctx, heartbeatTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url+"/health", nil)
	if err != nil {
		return "", "", fmt.Errorf("building request: %w", err)
	}
	resp, err := r.http.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("unreachable: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("unhealthy: status %d", resp.StatusCode)
	}

	var body struct {
		NodeID  string `json:"node_id"`
		Version string `json:"version"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return body.NodeID, body.Version, nil
}

// AddPeer inserts (or updates) a peer and starts heartbeating it
// immediately. Callers wanting a reachability handshake first should call
// Probe themselves — AddPeer itself doesn't check, so it stays usable for
// seeding known-good peers (e.g. from config) without a network round trip.
// name and version are whatever Probe returned (or "" if the caller has
// none yet); the next heartbeat corrects both either way.
func (r *Registry) AddPeer(ctx context.Context, id, url, name, version string) error {
	url = strings.TrimRight(url, "/")
	if _, err := r.db.ExecContext(ctx, `
		INSERT INTO peers (id, url, state, name, version) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET url = excluded.url, name = excluded.name, version = excluded.version
	`, id, url, StateOnline, name, version); err != nil {
		return fmt.Errorf("adding peer %s: %w", id, err)
	}

	r.mu.Lock()
	r.peers[id] = &Peer{ID: id, URL: url, State: StateOnline, Name: name, Version: version}
	r.mu.Unlock()

	r.startHeartbeat(id)
	return nil
}

// RemovePeer stops heartbeating a peer and deletes it. Any catalog items
// it was elected primary for are cleaned up on the next sync cycle
// automatically (Sync stops fetching from it, so Reconcile sees no more
// offer and removes the corresponding .strm) — no extra work needed here.
func (r *Registry) RemovePeer(ctx context.Context, id string) error {
	r.mu.Lock()
	if cancel, ok := r.cancels[id]; ok {
		cancel()
		delete(r.cancels, id)
	}
	delete(r.peers, id)
	r.mu.Unlock()

	if _, err := r.db.ExecContext(ctx, `DELETE FROM peers WHERE id = ?`, id); err != nil {
		return fmt.Errorf("removing peer %s: %w", id, err)
	}
	return nil
}

func (r *Registry) List() []Peer {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Peer, 0, len(r.peers))
	for _, p := range r.peers {
		out = append(out, *p)
	}
	return out
}

func (r *Registry) Get(id string) (Peer, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.peers[id]
	if !ok {
		return Peer{}, false
	}
	return *p, true
}

// RunHeartbeat starts one heartbeat goroutine for every peer currently
// loaded. Peers added later via AddPeer start their own independently;
// this only covers what was loaded at construction time.
func (r *Registry) RunHeartbeat(ctx context.Context) {
	for _, id := range r.peerIDs() {
		r.startHeartbeat(id)
	}
}

// startHeartbeat begins (or, if already running, no-ops) the heartbeat
// loop for one peer, on a context this Registry can cancel independently
// via RemovePeer.
func (r *Registry) startHeartbeat(id string) {
	r.mu.Lock()
	if _, running := r.cancels[id]; running {
		r.mu.Unlock()
		return
	}
	hbCtx, cancel := context.WithCancel(r.baseCtx)
	r.cancels[id] = cancel
	r.mu.Unlock()

	go r.heartbeatLoop(hbCtx, id)
}

func (r *Registry) peerIDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.peers))
	for id := range r.peers {
		ids = append(ids, id)
	}
	return ids
}

func (r *Registry) heartbeatLoop(ctx context.Context, id string) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(jittered(heartbeatInterval)):
			r.checkOnce(ctx, id)
		}
	}
}

func jittered(base time.Duration) time.Duration {
	delta := float64(base) * heartbeatJitter
	offset := (rand.Float64()*2 - 1) * delta
	return base + time.Duration(offset)
}

func (r *Registry) checkOnce(ctx context.Context, id string) {
	r.mu.RLock()
	p, ok := r.peers[id]
	url := ""
	if ok {
		url = p.URL
	}
	r.mu.RUnlock()
	if !ok {
		return
	}

	name, version, probeErr := r.Probe(ctx, url)
	healthy := probeErr == nil

	r.mu.Lock()
	p, ok = r.peers[id]
	if !ok {
		r.mu.Unlock()
		return
	}
	prevState := p.State
	if healthy {
		p.missed = 0
		p.State = StateOnline
		if version != "" {
			p.Version = version
		}
		if name != "" {
			p.Name = name
		}
	} else {
		p.missed++
		switch {
		case p.missed >= offlineAfterMisses:
			p.State = StateOffline
		case p.missed >= degradedAfterMisses:
			p.State = StateDegraded
		}
	}
	newState := p.State
	newVersion := p.Version
	newName := p.Name
	r.mu.Unlock()

	if newState != prevState {
		log.Printf("peer %s: %s -> %s", id, prevState, newState)
	}

	var execErr error
	if healthy {
		_, execErr = r.db.ExecContext(ctx, `
			UPDATE peers SET state = ?, last_seen_at = ?, version = ?, name = ? WHERE id = ?
		`, string(newState), time.Now().Unix(), newVersion, newName, id)
	} else {
		_, execErr = r.db.ExecContext(ctx, `
			UPDATE peers SET state = ? WHERE id = ?
		`, string(newState), id)
	}
	if execErr != nil {
		log.Printf("peer %s: failed to persist state: %v", id, execErr)
	}
}
