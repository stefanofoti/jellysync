// Package peers tracks the configured peer nodes and their reachability.
package peers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"jellysync/internal/config"
	"jellysync/internal/logging"
	"jellysync/internal/peerauth"
)

type State string

const (
	StateOnline   State = "ONLINE"
	StateDegraded State = "DEGRADED"
	StateOffline  State = "OFFLINE"
	// StateDisabled marks a peer whose transport this node has turned off
	// (HTTP_PEERS / MTLS_PEERS): it isn't probed, pulled or streamed from,
	// but stays registered with its catalog mirror, so turning the
	// transport back on resumes where it left off. Never persisted.
	StateDisabled State = "DISABLED"
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
	IP      string // last-known IP address of URL's host; display label and caller-attribution fallback only, "" until resolved
	// Pin is the peer's key pin when it was paired by invite (reached
	// with mTLS on its peer port); "" for a legacy peer added by URL.
	Pin string

	missed int
}

type Registry struct {
	db        *sql.DB
	http      *http.Client
	baseCtx   context.Context
	identity  *peerauth.Identity
	transport *http.Transport
	others    OtherIDs
	// httpOff/mtlsOff turn off legacy (by URL) and paired (mTLS) peers
	// respectively; the zero value has both on. See SetTransports.
	httpOff, mtlsOff bool

	mu      sync.RWMutex
	peers   map[string]*Peer
	cancels map[string]context.CancelFunc
}

// NewRegistry upserts the configured peers into the peers table (so they
// exist on first boot), then loads the in-memory set from *every* row in
// the table — not just the configured ones. This means a peer added at
// runtime (e.g. via the web UI) persists across restarts even if it's
// never added to the PEERS environment variable; the DB, not PEERS, is the
// source of truth once the node has booted once. id is this node's own
// key, presented to paired peers; with a nil id only legacy peers can be
// reached.
func NewRegistry(ctx context.Context, db *sql.DB, configured []config.Peer, id *peerauth.Identity) (*Registry, error) {
	r := &Registry{
		db:       db,
		baseCtx:  ctx,
		identity: id,
		peers:    make(map[string]*Peer),
		cancels:  make(map[string]context.CancelFunc),
	}
	r.transport = peerauth.NewTransport(id, r.PinForAddr)
	r.http = &http.Client{Timeout: heartbeatTimeout, Transport: r.transport}

	for _, p := range configured {
		_, err := db.ExecContext(ctx, `
			INSERT INTO peers (id, url, state) VALUES (?, ?, ?)
			ON CONFLICT(id) DO UPDATE SET url = excluded.url
		`, p.ID, p.URL, StateOnline)
		if err != nil {
			return nil, fmt.Errorf("seeding peer %s: %w", p.ID, err)
		}
	}

	if err := migrateUUIDIDs(ctx, db); err != nil {
		return nil, err
	}

	rows, err := db.QueryContext(ctx, `SELECT id, url, state, version, name, pin FROM peers`)
	if err != nil {
		return nil, fmt.Errorf("loading peers: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var p Peer
		var state string
		if err := rows.Scan(&p.ID, &p.URL, &state, &p.Version, &p.Name, &p.Pin); err != nil {
			return nil, fmt.Errorf("scanning peer: %w", err)
		}
		p.State = State(state)
		p.IP = literalIP(p.URL)
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
func (r *Registry) AddPeer(ctx context.Context, id, url, name, version, pin string) error {
	url = strings.TrimRight(url, "/")
	if _, err := r.db.ExecContext(ctx, `
		INSERT INTO peers (id, url, state, name, version, pin) VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET url = excluded.url, name = excluded.name, version = excluded.version, pin = excluded.pin
	`, id, url, StateOnline, name, version, pin); err != nil {
		return fmt.Errorf("adding peer %s: %w", id, err)
	}

	r.mu.Lock()
	p := &Peer{ID: id, URL: url, State: StateOnline, Name: name, Version: version, IP: resolveIP(ctx, url), Pin: pin}
	if !r.enabledLocked(p) {
		p.State = StateDisabled
	}
	r.peers[id] = p
	r.mu.Unlock()
	slog.Info("peer added", "peer", id, "url", url, "name", name, "version", version, "mtls", pin != "")

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
	slog.Info("peer removed; its items are withdrawn at the next sync", "peer", id)
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

// ResolveCaller maps an incoming peer request to this node's registry id
// for that peer. nodeID is the caller's self-reported NODE_ID (which is the
// Name we learned from its /health, not the id we assigned it), remoteIP is
// the request's source address. Name wins; the IP match covers callers too
// old to send their node id. Returns "" if neither matches a known peer.
func (r *Registry) ResolveCaller(nodeID, remoteIP string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if nodeID != "" {
		for id, p := range r.peers {
			if p.Name == nodeID || id == nodeID {
				return id
			}
		}
	}
	if remoteIP != "" {
		for id, p := range r.peers {
			if p.IP != "" && p.IP == remoteIP {
				return id
			}
		}
	}
	return ""
}

// Transport is the transport every request to a peer must use: it dials
// paired peers with mTLS, checking each one's pinned key, and legacy ones
// as before.
func (r *Registry) Transport() *http.Transport { return r.transport }

// Identity is this node's own key, or nil if it has none.
func (r *Registry) Identity() *peerauth.Identity { return r.identity }

// PinForAddr returns the key pin of the paired peer at host:port addr.
func (r *Registry) PinForAddr(addr string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, p := range r.peers {
		if p.Pin == "" {
			continue
		}
		if a, ok := peerauth.AddrOf(p.URL); ok && a == addr {
			return p.Pin, true
		}
	}
	return "", false
}

// PeerByPin returns the peer whose key is pin.
func (r *Registry) PeerByPin(pin string) (Peer, bool) {
	if pin == "" {
		return Peer{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, p := range r.peers {
		if p.Pin == pin {
			return *p, true
		}
	}
	return Peer{}, false
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

// SetTransports turns legacy (http) and paired (mTLS) peers on or off.
// Peers of a turned-off kind become DISABLED; call before RunHeartbeat.
func (r *Registry) SetTransports(httpPeers, mtlsPeers bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.httpOff, r.mtlsOff = !httpPeers, !mtlsPeers
	for _, p := range r.peers {
		if !r.enabledLocked(p) {
			p.State = StateDisabled
		}
	}
}

// HTTPPeers and MTLSPeers report whether legacy and paired peers are on.
func (r *Registry) HTTPPeers() bool { r.mu.RLock(); defer r.mu.RUnlock(); return !r.httpOff }
func (r *Registry) MTLSPeers() bool { r.mu.RLock(); defer r.mu.RUnlock(); return !r.mtlsOff }

func (r *Registry) enabledLocked(p *Peer) bool {
	if p.Pin != "" {
		return !r.mtlsOff
	}
	return !r.httpOff
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
	if p, ok := r.peers[id]; ok && p.State == StateDisabled {
		r.mu.Unlock()
		return
	}
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
	disabled := ok && p.State == StateDisabled
	r.mu.RUnlock()
	if !ok || disabled {
		return
	}

	name, version, probeErr := r.Probe(ctx, url)
	healthy := probeErr == nil
	ip := resolveIP(ctx, url)

	r.mu.Lock()
	p, ok = r.peers[id]
	if !ok {
		r.mu.Unlock()
		return
	}
	prevState := p.State
	if ip != "" {
		p.IP = ip
	}
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

	switch {
	case newState == prevState:
		if !healthy {
			slog.Debug("peer health probe failed", "peer", id, "url", url, "state", newState, logging.Err(probeErr))
		}
	case newState == StateOnline:
		slog.Info("peer is online", "peer", id, "was", prevState, "name", newName, "version", newVersion, "ip", ip)
	default:
		slog.Warn("peer is unreachable", "peer", id, "url", url, "was", prevState, "now", newState, logging.Err(probeErr))
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
		slog.Error("persisting peer state", "peer", id, logging.Err(execErr))
	}
}

// literalIP returns rawURL's host if it's already an IP address, else "".
// Needs no network round trip, so it's safe to call at load time.
func literalIP(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	if ip := net.ParseIP(u.Hostname()); ip != nil {
		return ip.String()
	}
	return ""
}

// resolveIP returns the IP address rawURL's host resolves to, preferring
// IPv4, or "" if it can't be resolved within heartbeatTimeout.
func resolveIP(ctx context.Context, rawURL string) string {
	if ip := literalIP(rawURL); ip != "" {
		return ip
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Hostname() == "" {
		return ""
	}
	lookupCtx, cancel := context.WithTimeout(ctx, heartbeatTimeout)
	defer cancel()
	addrs, err := net.DefaultResolver.LookupIPAddr(lookupCtx, u.Hostname())
	if err != nil || len(addrs) == 0 {
		return ""
	}
	for _, a := range addrs {
		if v4 := a.IP.To4(); v4 != nil {
			return v4.String()
		}
	}
	return addrs[0].IP.String()
}
