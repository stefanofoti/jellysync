package peerauth

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Caller is who a request on the peer port comes from, by the key it
// authenticated with. One key can be both our client (it redeemed our
// invite) and our peer (we redeemed its invite).
type Caller struct {
	Pin      string
	ClientID string // set if the key is an authorized client
	PeerID   string // set if the key is one of our registry peers
	Pairing  bool   // set if the key is a pending invite's temporary key
}

// ID is the id to attribute the caller's traffic and log lines to. Clients
// and peers share one id space, and a node that is both gets the same id
// in both tables, so either works; PeerID wins for clarity.
func (c Caller) ID() string {
	if c.PeerID != "" {
		return c.PeerID
	}
	return c.ClientID
}

// Resolver maps a key pin to its caller; ok is false for an unknown key.
type Resolver func(pin string) (c Caller, ok bool)

type callerKey struct{}

// CallerFrom returns the authenticated caller of a peer-port request.
func CallerFrom(ctx context.Context) (Caller, bool) {
	c, ok := ctx.Value(callerKey{}).(Caller)
	return c, ok
}

// Authenticate resolves the caller from the request's client certificate
// and stores it in the request context. The handshake already rejected
// unknown keys; this re-checks on every request so a revocation applies
// to connections that were open before it.
func Authenticate(resolve Resolver, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
			http.Error(w, "client certificate required", http.StatusUnauthorized)
			return
		}
		pin := PinOf(r.TLS.PeerCertificates[0])
		c, ok := resolve(pin)
		if !ok {
			slog.Debug("peer port request from a key no longer authorized", "pin", pin, "path", r.URL.Path, "remote", r.RemoteAddr)
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), callerKey{}, c)))
	})
}

// RequireClient lets through only authorized clients.
func RequireClient(next http.Handler) http.Handler {
	return require(func(c Caller) bool { return c.ClientID != "" }, next)
}

// RequireClientOrPeer lets through clients and peers.
func RequireClientOrPeer(next http.Handler) http.Handler {
	return require(func(c Caller) bool { return c.ClientID != "" || c.PeerID != "" }, next)
}

// RequirePeer lets through only our registry peers: they're the nodes
// whose change notifications we act on.
func RequirePeer(next http.Handler) http.Handler {
	return require(func(c Caller) bool { return c.PeerID != "" }, next)
}

// RequirePairing lets through only a pending invite's temporary key.
func RequirePairing(next http.Handler) http.Handler {
	return require(func(c Caller) bool { return c.Pairing }, next)
}

func require(allowed func(Caller) bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, ok := CallerFrom(r.Context())
		if !ok || !allowed(c) {
			slog.Debug("peer port request not allowed for this caller", "caller", c.ID(), "pairing", c.Pairing, "path", r.URL.Path)
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Limiter allows one event per key every interval.
type Limiter struct {
	every time.Duration
	now   func() time.Time

	mu   sync.Mutex
	last map[string]time.Time
}

func NewLimiter(every time.Duration, now func() time.Time) *Limiter {
	return &Limiter{every: every, now: now, last: make(map[string]time.Time)}
}

// Allow reports whether key may act now, and if so starts its next window.
func (l *Limiter) Allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if last, ok := l.last[key]; ok && now.Sub(last) < l.every {
		return false
	}
	l.last[key] = now
	return true
}
