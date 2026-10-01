package peerauth

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"database/sql"

	"jellysync/internal/db"
)

func openDB(t *testing.T) *testDB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "jellysync.db")
	store, err := db.Open(path)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return &testDB{DB: store, path: path}
}

type testDB struct {
	*sql.DB
	path string
}

// clock is a settable test clock.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newClock() *clock { return &clock{t: time.Unix(1_800_000_000, 0)} }

func mustIdentity(t *testing.T) *Identity {
	t.Helper()
	seed := make([]byte, 32)
	rand.Read(seed)
	id, err := FromSeed(seed)
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	return id
}

func TestIdentityIsStableAcrossReopen(t *testing.T) {
	d := openDB(t)
	a, err := LoadOrCreate(context.Background(), d.DB)
	if err != nil {
		t.Fatal(err)
	}
	b, err := LoadOrCreate(context.Background(), d.DB)
	if err != nil {
		t.Fatal(err)
	}
	if a.Pin() != b.Pin() {
		t.Fatalf("pin changed across loads: %s vs %s", a.Pin(), b.Pin())
	}
	if !ValidPin(a.Pin()) {
		t.Fatalf("invalid pin %q", a.Pin())
	}
	if PinOf(a.Certificate().Leaf) != a.Pin() {
		t.Fatal("certificate pin does not match identity pin")
	}
	if a.Certificate().Leaf.Subject.String() != "" {
		t.Fatalf("certificate subject should be empty, got %q", a.Certificate().Leaf.Subject)
	}
}

func TestInviteRoundTripAndValidation(t *testing.T) {
	id := mustIdentity(t)
	seed := make([]byte, 32)
	good := Invite{URL: "https://alice.example.org:8443", NodeID: "alice", Pin: id.Pin(), Seed: seed, ExpiresAt: 2_000_000_000}
	got, err := ParseInvite("  " + good.Encode() + "\n")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got.URL != good.URL || got.Pin != good.Pin || got.NodeID != "alice" || got.ExpiresAt != good.ExpiresAt {
		t.Fatalf("round trip mismatch: %+v", got)
	}

	noPort := good
	noPort.URL = "https://alice.example.org"
	if got, err := ParseInvite(noPort.Encode()); err != nil || got.URL != "https://alice.example.org:443" {
		t.Fatalf("default port: %v %q", err, got.URL)
	}

	bad := map[string]string{
		"prefix":    "jellysync2:" + strings.TrimPrefix(good.Encode(), invitePrefix),
		"encoding":  invitePrefix + "!!!",
		"content":   invitePrefix + "bm90IGpzb24",
		"http url":  with(good, func(i *Invite) { i.URL = "http://alice:8098" }).Encode(),
		"path":      with(good, func(i *Invite) { i.URL = "https://alice:8443/x" }).Encode(),
		"pin":       with(good, func(i *Invite) { i.Pin = "short" }).Encode(),
		"seed":      with(good, func(i *Invite) { i.Seed = []byte{1, 2, 3} }).Encode(),
		"no expiry": with(good, func(i *Invite) { i.ExpiresAt = 0 }).Encode(),
	}
	for name, s := range bad {
		if _, err := ParseInvite(s); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	if !good.Expired(time.Unix(2_000_000_000, 0)) || good.Expired(time.Unix(1_999_999_999, 0)) {
		t.Error("Expired boundary wrong")
	}
}

func with(inv Invite, f func(*Invite)) Invite { f(&inv); return inv }

func TestRedeemIsSingleUseAndExpires(t *testing.T) {
	d := openDB(t)
	clk := newClock()
	s, err := NewStore(context.Background(), d.DB, clk.Now)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, inv, err := s.CreateInvite(ctx, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if !s.PendingInvite(inv.Pin) {
		t.Fatal("new invite should be pending")
	}

	// Concurrent redeems of the same invite: exactly one wins.
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pin := mustIdentity(t).Pin()
			if _, err := s.Redeem(ctx, inv.Pin, pin, "bea", "bea"+string(rune('a'+i))); err == nil {
				wins.Add(1)
			} else if !errors.Is(err, ErrInviteInvalid) {
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("redeem winners = %d, want 1", wins.Load())
	}
	if s.PendingInvite(inv.Pin) || len(s.Invites(ctx)) != 0 {
		t.Fatal("redeemed invite still pending")
	}

	// Expiry.
	_, inv2, _ := s.CreateInvite(ctx, time.Hour)
	clk.Add(time.Hour)
	if s.PendingInvite(inv2.Pin) {
		t.Fatal("expired invite still pending")
	}
	if _, err := s.Redeem(ctx, inv2.Pin, mustIdentity(t).Pin(), "carol", "carol"); !errors.Is(err, ErrInviteInvalid) {
		t.Fatalf("redeem expired: %v", err)
	}
	if len(s.Invites(ctx)) != 0 {
		t.Fatal("expired invite not swept")
	}

	// Persistence: reload sees the one client.
	s2, err := NewStore(ctx, d.DB, clk.Now)
	if err != nil {
		t.Fatal(err)
	}
	if len(s2.Clients()) != 1 {
		t.Fatalf("clients after reload = %d", len(s2.Clients()))
	}
}

func TestRedeemSamePinKeepsID(t *testing.T) {
	d := openDB(t)
	clk := newClock()
	ctx := context.Background()
	s, _ := NewStore(ctx, d.DB, clk.Now)
	pin := mustIdentity(t).Pin()
	_, a, _ := s.CreateInvite(ctx, time.Hour)
	if _, err := s.Redeem(ctx, a.Pin, pin, "bea", "bea"); err != nil {
		t.Fatal(err)
	}
	_, b, _ := s.CreateInvite(ctx, time.Hour)
	c, err := s.Redeem(ctx, b.Pin, pin, "beatrice", "beatrice")
	if err != nil {
		t.Fatal(err)
	}
	if c.ID != "bea" || c.Name != "beatrice" || len(s.Clients()) != 1 {
		t.Fatalf("re-pair: %+v, clients %d", c, len(s.Clients()))
	}
	_, x, _ := s.CreateInvite(ctx, time.Hour)
	if _, err := s.Redeem(ctx, x.Pin, mustIdentity(t).Pin(), "other", "bea"); !errors.Is(err, ErrIDTaken) {
		t.Fatalf("id collision: %v", err)
	}
}

func TestLimiter(t *testing.T) {
	clk := newClock()
	l := NewLimiter(10*time.Minute, clk.Now)
	if !l.Allow("a") || l.Allow("a") || !l.Allow("b") {
		t.Fatal("first window wrong")
	}
	clk.Add(10 * time.Minute)
	if !l.Allow("a") {
		t.Fatal("window did not reset")
	}
}

// pinnedServer starts a TLS server with id's peer TLS config; known
// decides which client keys pass the handshake, resolve which pass each
// request.
func pinnedServer(t *testing.T, id *Identity, known func(string) bool, resolve Resolver) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	hits := &atomic.Int32{}
	h := Authenticate(resolve, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		c, _ := CallerFrom(r.Context())
		io.WriteString(w, c.ID())
	}))
	srv := httptest.NewUnstartedServer(h)
	srv.TLS = id.ServerTLSConfig(known)
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv, hits
}

func TestTransportPinning(t *testing.T) {
	server, client, stranger := mustIdentity(t), mustIdentity(t), mustIdentity(t)
	var revoked atomic.Bool
	resolve := func(pin string) (Caller, bool) {
		if pin == client.Pin() && !revoked.Load() {
			return Caller{Pin: pin, ClientID: "bea"}, true
		}
		return Caller{}, false
	}
	srv, hits := pinnedServer(t, server, func(p string) bool { _, ok := resolve(p); return ok }, resolve)
	addr := strings.TrimPrefix(srv.URL, "https://")

	get := func(id *Identity, pin string) (string, error) {
		tr := NewTransport(id, func(a string) (string, bool) { return pin, a == addr })
		defer tr.CloseIdleConnections()
		resp, err := (&http.Client{Transport: tr}).Get(srv.URL + "/x")
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			return "", errors.New(resp.Status)
		}
		return string(b), nil
	}

	if got, err := get(client, server.Pin()); err != nil || got != "bea" {
		t.Fatalf("pinned client: %q %v", got, err)
	}
	if _, err := get(client, stranger.Pin()); !errors.Is(err, ErrPinMismatch) {
		t.Fatalf("wrong server pin: %v", err)
	}
	before := hits.Load()
	if _, err := get(stranger, server.Pin()); err == nil {
		t.Fatal("unknown client key accepted")
	}
	// No client certificate at all.
	plain := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	if _, err := plain.Get(srv.URL + "/x"); err == nil {
		t.Fatal("client without certificate accepted")
	}
	// Plain HTTP on the TLS port: Go answers 400 itself, never the handler.
	if resp, err := http.Get(strings.Replace(srv.URL, "https", "http", 1) + "/x"); err == nil {
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("plain HTTP: status %d", resp.StatusCode)
		}
	}
	if hits.Load() != before {
		t.Fatal("handler reached by an unauthenticated caller")
	}
	// An address with no pin is a legacy https:// peer: ordinary CA
	// verification, which a self-signed key fails.
	var verifyErr *tls.CertificateVerificationError
	if _, err := (&http.Client{Transport: NewTransport(client, func(string) (string, bool) { return "", false })}).Get(srv.URL); !errors.As(err, &verifyErr) {
		t.Fatalf("no pin: %v", err)
	}

	// Revocation applies to an already-open connection.
	tr := NewTransport(client, func(string) (string, bool) { return server.Pin(), true })
	c := &http.Client{Transport: tr}
	resp, err := c.Get(srv.URL + "/x")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	revoked.Store(true)
	resp, err = c.Get(srv.URL + "/x")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("revoked on open connection: status %d", resp.StatusCode)
	}
}

func TestCanonicalURL(t *testing.T) {
	cases := map[string]string{
		"https://a.example:8443":  "https://a.example:8443",
		"https://a.example:8443/": "https://a.example:8443",
		"https://a.example":       "https://a.example:443",
		"https://A.Example:1":     "https://a.example:1",
		"https://[::1]:9":         "https://[::1]:9",
	}
	for in, want := range cases {
		if got, err := CanonicalURL(in); err != nil || got != want {
			t.Errorf("%s: got %q %v want %q", in, got, err, want)
		}
	}
	for _, in := range []string{"http://a:1", "https://u:p@a:1", "https://a:1/p", "https://a:1?x=1", "nope"} {
		if _, err := CanonicalURL(in); err == nil {
			t.Errorf("%s: expected error", in)
		}
	}
}
