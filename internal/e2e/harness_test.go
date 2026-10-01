// Package e2e runs whole jellysync nodes in one process — dashboard port,
// mTLS peer port, DB, sync loop steps and .strm output — against fake
// Jellyfins, to test pairing and peer traffic end to end.
package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"jellysync/internal/app"
	"jellysync/internal/config"
	"jellysync/internal/db"
	"jellysync/internal/peerauth"
)

// fakeJellyfin serves a mutable movie library: the /Items listing, the
// library roots, and each movie's bytes (Range-capable) for /Download.
type fakeJellyfin struct {
	mu     sync.Mutex
	movies map[string]string // tmdb id -> name
}

func (f *fakeJellyfin) set(movies map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.movies = movies
}

func movieBytes(tmdbID string) []byte {
	return bytes.Repeat([]byte("movie-"+tmdbID+"|"), 20000)
}

func (f *fakeJellyfin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case r.URL.Path == "/Items":
		items := make([]map[string]any, 0, len(f.movies))
		for id, name := range f.movies {
			items = append(items, map[string]any{
				"Id": "jf-" + id, "Name": name, "Type": "Movie", "Path": "/media/movies/" + id + ".mkv",
				"LocationType": "FileSystem", "ProviderIds": map[string]string{"Tmdb": id},
			})
		}
		json.NewEncoder(w).Encode(map[string]any{"Items": items})
	case r.URL.Path == "/Library/VirtualFolders":
		json.NewEncoder(w).Encode([]map[string]any{{"Locations": []string{"/media/movies"}}})
	case strings.HasPrefix(r.URL.Path, "/Items/jf-") && strings.HasSuffix(r.URL.Path, "/Download"):
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/Items/jf-"), "/Download")
		if _, ok := f.movies[id]; !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "video/x-matroska")
		http.ServeContent(w, r, id+".mkv", time.Time{}, bytes.NewReader(movieBytes(id)))
	default:
		http.NotFound(w, r)
	}
}

// clock is a settable clock shared by a test's nodes.
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

// node is one running jellysync instance.
type node struct {
	t      *testing.T
	name   string
	clk    *clock
	jf     *fakeJellyfin
	dbPath string
	outDir string
	jfURL  string

	db   *sql.DB
	app  *app.App
	dash *httptest.Server
	peer *httptest.Server // nil without a peer port

	dashAddr, peerAddr string
	peerHits           atomic.Int32 // requests that reached the peer port's handler

	// httpOff/mtlsOff set HTTP_PEERS=false / MTLS_PEERS=false; change
	// them before restart to flip a node's transports.
	httpOff, mtlsOff bool
}

type nodeOpt struct {
	noPeerPort bool // no listener and no PUBLIC_URL: a consumer-only node
	httpOff    bool // HTTP_PEERS=false
	mtlsOff    bool // MTLS_PEERS=false (the production default)
}

func newNode(t *testing.T, name string, clk *clock, movies map[string]string, opts ...nodeOpt) *node {
	t.Helper()
	var o nodeOpt
	if len(opts) > 0 {
		o = opts[0]
	}
	dir := t.TempDir()
	n := &node{t: t, name: name, clk: clk, jf: &fakeJellyfin{movies: movies},
		dbPath: filepath.Join(dir, "jellysync.db"), outDir: filepath.Join(dir, "synced"),
		httpOff: o.httpOff, mtlsOff: o.mtlsOff}
	jfSrv := httptest.NewServer(n.jf)
	t.Cleanup(jfSrv.Close)
	n.dashAddr = freeAddr(t)
	if !o.noPeerPort {
		n.peerAddr = freeAddr(t)
	}
	n.start(jfSrv.URL)
	t.Cleanup(n.stop)
	return n
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func listen(t *testing.T, addr string) net.Listener {
	t.Helper()
	var err error
	for i := 0; i < 50; i++ {
		var ln net.Listener
		if ln, err = net.Listen("tcp", addr); err == nil {
			return ln
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("listen %s: %v", addr, err)
	return nil
}

func (n *node) start(jfURL string) {
	t := n.t
	t.Helper()
	n.jfURL = jfURL
	cfg := &config.Config{
		NodeID:           n.name,
		ListenAddr:       n.dashAddr,
		DBPath:           n.dbPath,
		Jellyfin:         config.Jellyfin{URL: jfURL, APIKey: "key"},
		Strm:             config.Strm{OutputDir: n.outDir, BaseURL: "http://" + n.dashAddr},
		HTTPPeers:        !n.httpOff,
		MTLSPeers:        !n.mtlsOff,
		StreamBufferKB:   64,
		JellyfinTimeout:  5 * time.Second,
		PeerFetchTimeout: 5 * time.Second,
	}
	if n.peerAddr != "" && !n.mtlsOff {
		cfg.PeerListenAddr = n.peerAddr
		cfg.PublicURL = "https://" + n.peerAddr
	}
	store, err := db.Open(n.dbPath)
	if err != nil {
		t.Fatalf("%s: open db: %v", n.name, err)
	}
	n.db = store
	n.app, err = app.New(context.Background(), cfg, store, app.Options{Version: "test", Now: n.clk.Now})
	if err != nil {
		t.Fatalf("%s: app: %v", n.name, err)
	}

	n.dash = httptest.NewUnstartedServer(n.app.DashboardHandler())
	n.dash.Listener.Close()
	n.dash.Listener = listen(t, n.dashAddr)
	n.dash.Start()

	if n.peerAddr != "" && !n.mtlsOff {
		h := n.app.PeerHandler()
		n.peer = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n.peerHits.Add(1)
			h.ServeHTTP(w, r)
		}))
		n.peer.Listener.Close()
		n.peer.Listener = listen(t, n.peerAddr)
		n.peer.TLS = n.app.PeerTLSConfig()
		n.peer.StartTLS()
	}
}

func (n *node) stop() {
	if n.dash != nil {
		n.dash.CloseClientConnections()
		n.dash.Close()
		n.dash = nil
	}
	if n.peer != nil {
		n.peer.CloseClientConnections()
		n.peer.Close()
		n.peer = nil
	}
	if n.app != nil {
		n.app.Registry().Transport().CloseIdleConnections()
	}
	if n.db != nil {
		n.db.Close()
		n.db = nil
	}
}

// restart stops the node and starts it again on the same DB and addresses.
func (n *node) restart() {
	n.stop()
	n.start(n.jfURL)
}

func (n *node) publicURL() string { return "https://" + n.peerAddr }

// api calls the node's dashboard port.
func (n *node) api(method, path string, body any) (int, []byte) {
	n.t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, n.dash.URL+path, rd)
	if err != nil {
		n.t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		n.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func (n *node) invite() string {
	n.t.Helper()
	status, body := n.api(http.MethodPost, "/api/v1/invites", nil)
	if status != http.StatusCreated {
		n.t.Fatalf("%s: create invite: %d %s", n.name, status, body)
	}
	var out struct {
		Invite string `json:"invite"`
	}
	json.Unmarshal(body, &out)
	return out.Invite
}

type peerView struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	URL  string `json:"url"`
	MTLS bool   `json:"mtls"`
}

func (n *node) redeem(invite string) (int, string, peerView) {
	n.t.Helper()
	status, body := n.api(http.MethodPost, "/api/v1/peers/redeem", map[string]string{"invite": invite})
	var p peerView
	if status == http.StatusOK {
		json.Unmarshal(body, &p)
	}
	return status, strings.TrimSpace(string(body)), p
}

func (n *node) mustRedeem(invite string) peerView {
	n.t.Helper()
	status, body, p := n.redeem(invite)
	if status != http.StatusOK {
		n.t.Fatalf("%s: redeem: %d %s", n.name, status, body)
	}
	return p
}

func (n *node) sync(refreshLocal bool) {
	n.t.Helper()
	if err := n.app.SyncNow(context.Background(), refreshLocal); err != nil {
		n.t.Fatalf("%s: sync: %v", n.name, err)
	}
}

// strmFiles maps each .strm file (relative to the output dir) to its URL.
func (n *node) strmFiles() map[string]string {
	n.t.Helper()
	out := map[string]string{}
	filepath.Walk(n.outDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".strm") {
			return nil
		}
		b, _ := os.ReadFile(path)
		rel, _ := filepath.Rel(n.outDir, path)
		out[rel] = strings.TrimSpace(string(b))
		return nil
	})
	return out
}

func (n *node) strmFor(peerID, tmdbID string) string {
	n.t.Helper()
	for rel, u := range n.strmFiles() {
		if strings.HasPrefix(rel, filepath.Join("movies", peerID)+string(filepath.Separator)) && strings.HasSuffix(u, "/jf-"+tmdbID) {
			return u
		}
	}
	n.t.Fatalf("%s: no .strm for %s from %s in %v", n.name, tmdbID, peerID, n.strmFiles())
	return ""
}

func (n *node) items() []string {
	n.t.Helper()
	rows, err := n.db.Query(`SELECT global_id, local, COALESCE(primary_peer_id, '') FROM catalog_items`)
	if err != nil {
		n.t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id, peer string
		var local bool
		rows.Scan(&id, &local, &peer)
		if local {
			peer = "local"
		}
		out = append(out, id+"="+peer)
	}
	sort.Strings(out)
	return out
}

func (n *node) mirrorSize(peerID string) int {
	var c int
	n.db.QueryRow(`SELECT COUNT(*) FROM peer_catalog WHERE peer_id = ?`, peerID).Scan(&c)
	return c
}

// keyClient is an HTTP client presenting id's key to target's peer port
// (pinned to target's key), as an arbitrary node would.
func keyClient(id *peerauth.Identity, target *node) *http.Client {
	pin := target.app.Identity().Pin()
	addr := target.peerAddr
	return &http.Client{Transport: peerauth.NewTransport(id, func(a string) (string, bool) { return pin, a == addr }), Timeout: 10 * time.Second}
}

func do(t *testing.T, c *http.Client, method, url string, body any) (int, []byte, error) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, url, rd)
	resp, err := c.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b, nil
}

func assertItems(t *testing.T, got []string, want ...string) {
	t.Helper()
	sort.Strings(want)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("catalog_items = %v, want %v", got, want)
	}
}

// eventually polls cond until it holds or a few seconds pass. Traffic is
// recorded after a stream's last byte is written, so a client can finish
// reading before the counter moves.
func eventually(t *testing.T, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
	return true
}

func newClock() *clock { return &clock{t: time.Now()} }

func randomIdentity(t *testing.T) *peerauth.Identity {
	t.Helper()
	seed := make([]byte, 32)
	if _, err := rand.Read(seed); err != nil {
		t.Fatal(err)
	}
	id, err := peerauth.FromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
