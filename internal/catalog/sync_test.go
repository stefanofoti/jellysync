package catalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"jellysync/internal/config"
	"jellysync/internal/db"
	"jellysync/internal/jellyfin"
	"jellysync/internal/peers"
)

// fakeJellyfin serves a mutable /Items listing of movies.
type fakeJellyfin struct {
	mu     sync.Mutex
	movies map[string]string // tmdb id -> name
	dirs   map[string]string // tmdb id -> folder of its file; default /media
	calls  atomic.Int32
}

func (f *fakeJellyfin) set(movies map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.movies = movies
}

func (f *fakeJellyfin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.calls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	items := make([]map[string]any, 0, len(f.movies))
	for id, name := range f.movies {
		dir := "/media"
		if d, ok := f.dirs[id]; ok {
			dir = d
		}
		items = append(items, map[string]any{
			"Id": "jf-" + id, "Name": name, "Type": "Movie", "Path": dir + "/" + id + ".mkv",
			"LocationType": "FileSystem", "ProviderIds": map[string]string{"Tmdb": id},
		})
	}
	json.NewEncoder(w).Encode(map[string]any{"Items": items})
}

// node is one jellysync instance: its DB, fake Jellyfin, index, and the
// peer-facing catalog endpoints.
type node struct {
	db      *sql.DB
	jf      *fakeJellyfin
	ix      *Index
	srv     *httptest.Server
	changes atomic.Int32 // change-feed requests served
	legacy  bool         // serve only the pre-change-feed endpoint
}

func newNode(t *testing.T, movies map[string]string) *node {
	t.Helper()
	store, err := db.Open(filepath.Join(t.TempDir(), "jellysync.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	n := &node{db: store, jf: &fakeJellyfin{movies: movies}}
	jfSrv := httptest.NewServer(n.jf)
	t.Cleanup(jfSrv.Close)

	n.ix, err = NewIndex(context.Background(), store, jellyfin.New(jfSrv.URL, "key", "test", 5*time.Second), "/strm")
	if err != nil {
		t.Fatalf("new index: %v", err)
	}

	changes := ChangesHandler(n.ix)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/catalog", Handler(n.ix))
	mux.HandleFunc("GET /api/v1/catalog/changes", func(w http.ResponseWriter, r *http.Request) {
		if n.legacy {
			http.NotFound(w, r)
			return
		}
		n.changes.Add(1)
		changes(w, r)
	})
	n.srv = httptest.NewServer(mux)
	t.Cleanup(n.srv.Close)
	return n
}

func (n *node) registry(t *testing.T, ps ...config.Peer) *peers.Registry {
	t.Helper()
	r, err := peers.NewRegistry(context.Background(), n.db, ps, nil)
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	return r
}

// items returns catalog_items as "global_id=owner" strings, owner being
// "local" or the elected peer id.
func (n *node) items(t *testing.T) []string {
	t.Helper()
	rows, err := n.db.Query(`SELECT global_id, local, COALESCE(primary_peer_id, '') FROM catalog_items`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id, peer string
		var local bool
		if err := rows.Scan(&id, &local, &peer); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if local {
			peer = "local"
		}
		out = append(out, id+"="+peer)
	}
	sort.Strings(out)
	return out
}

func (n *node) mirrorSize(t *testing.T, peerID string) int {
	t.Helper()
	var c int
	if err := n.db.QueryRow(`SELECT COUNT(*) FROM peer_catalog WHERE peer_id = ?`, peerID).Scan(&c); err != nil {
		t.Fatalf("count: %v", err)
	}
	return c
}

func mustSync(t *testing.T, n *node, reg *peers.Registry, refreshLocal bool) SyncStats {
	t.Helper()
	st, err := Sync(context.Background(), n.db, n.ix, reg, refreshLocal, 5*time.Second, "test-node")
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	for _, p := range st.Peers {
		if p.Err != nil {
			t.Fatalf("pulling peer %s: %v", p.Peer, p.Err)
		}
	}
	return st
}

func assertItems(t *testing.T, got []string, want ...string) {
	t.Helper()
	sort.Strings(want)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("catalog_items = %v, want %v", got, want)
	}
}

func movies(n int, from int) map[string]string {
	m := make(map[string]string, n)
	for i := from; i < from+n; i++ {
		m[fmt.Sprint(i)] = fmt.Sprintf("Movie %d", i)
	}
	return m
}

func TestSyncPullsOnlyChanges(t *testing.T) {
	old := pullPageSize
	pullPageSize = 3
	t.Cleanup(func() { pullPageSize = old })

	a := newNode(t, movies(7, 1))
	b := newNode(t, map[string]string{"1": "Movie 1"})
	reg := b.registry(t, config.Peer{ID: "a", URL: a.srv.URL})

	// Initial sync: 7 items in pages of 3 -> 3 requests.
	mustSync(t, b, reg, true)
	assertItems(t, b.items(t),
		"tmdb:1=local", "tmdb:2=a", "tmdb:3=a", "tmdb:4=a", "tmdb:5=a", "tmdb:6=a", "tmdb:7=a")
	if got := a.changes.Load(); got != 3 {
		t.Errorf("initial sync made %d change-feed requests, want 3", got)
	}

	// Nothing changed: one request, and A's Jellyfin isn't touched at all.
	a.changes.Store(0)
	jfCalls := a.jf.calls.Load()
	mustSync(t, b, reg, true)
	if got := a.changes.Load(); got != 1 {
		t.Errorf("no-op sync made %d change-feed requests, want 1", got)
	}
	if a.jf.calls.Load() != jfCalls {
		t.Errorf("serving a peer's sync listed the peer's own Jellyfin")
	}

	// A drops 2 and 3, gains 8; B picks up exactly that.
	a.jf.set(map[string]string{"1": "Movie 1", "4": "Movie 4", "5": "Movie 5", "6": "Movie 6", "7": "Movie 7", "8": "Movie 8"})
	if st, err := a.ix.Refresh(context.Background()); err != nil || st.Added != 1 || st.Removed != 2 || st.Updated != 0 {
		t.Fatalf("refresh: %+v, err %v; want 1 added, 2 removed", st, err)
	}
	st := mustSync(t, b, reg, false)
	assertItems(t, b.items(t),
		"tmdb:1=local", "tmdb:4=a", "tmdb:5=a", "tmdb:6=a", "tmdb:7=a", "tmdb:8=a")
	if p := st.Peers[0]; p.Upserts != 1 || p.Deletes != 2 || p.Resync || p.Legacy {
		t.Errorf("delta pull stats = %+v, want 1 upsert, 2 deletes", p)
	}
}

func TestSyncResyncsOnEpochChangeWithoutDroppingItems(t *testing.T) {
	a := newNode(t, movies(3, 1))
	b := newNode(t, nil)
	reg := b.registry(t, config.Peer{ID: "a", URL: a.srv.URL})
	mustSync(t, b, reg, true)

	// Simulate A's database being recreated: new epoch, fresh revs, and
	// one item gone. B must converge without leftover mirror rows.
	a.jf.set(movies(2, 2))
	if _, err := a.db.Exec(`DELETE FROM local_catalog; DELETE FROM settings`); err != nil {
		t.Fatal(err)
	}
	// NewIndex seeds the new epoch; all other index state lives in the DB.
	if _, err := NewIndex(context.Background(), a.db, a.ix.jf, "/strm"); err != nil {
		t.Fatal(err)
	}

	mustSync(t, b, reg, true)
	assertItems(t, b.items(t), "tmdb:2=a", "tmdb:3=a")
	if got := b.mirrorSize(t, "a"); got != 2 {
		t.Errorf("mirror has %d rows after resync, want 2", got)
	}
}

func TestRequestResyncRedownloadsWholeCatalog(t *testing.T) {
	a := newNode(t, movies(3, 1))
	b := newNode(t, nil)
	reg := b.registry(t, config.Peer{ID: "a", URL: a.srv.URL})
	mustSync(t, b, reg, true)

	// A stray mirror row the delta feed would never correct: only a full
	// resync drops it.
	if _, err := b.db.Exec(`INSERT INTO peer_catalog (peer_id, global_id, entry, gen)
		SELECT peer_id, 'tmdb:99', entry, gen FROM peer_catalog WHERE peer_id = 'a' LIMIT 1`); err != nil {
		t.Fatal(err)
	}
	if err := RequestResync(context.Background(), b.db, "a"); err != nil {
		t.Fatal(err)
	}
	st := mustSync(t, b, reg, false)
	if p := st.Peers[0]; !p.Resync || p.Upserts != 3 || p.Swept != 1 {
		t.Errorf("requested resync stats = %+v, want resync with 3 upserts and 1 swept", p)
	}
	assertItems(t, b.items(t), "tmdb:1=a", "tmdb:2=a", "tmdb:3=a")
	if got := b.mirrorSize(t, "a"); got != 3 {
		t.Errorf("mirror has %d rows after resync, want 3", got)
	}

	// The request is consumed: the next pull is a delta again.
	st = mustSync(t, b, reg, false)
	if p := st.Peers[0]; p.Resync || p.Upserts != 0 {
		t.Errorf("pull after resync = %+v, want an empty delta", p)
	}
}

func TestSyncFallsBackToLegacyPeer(t *testing.T) {
	a := newNode(t, movies(3, 1))
	a.legacy = true
	b := newNode(t, nil)
	reg := b.registry(t, config.Peer{ID: "a", URL: a.srv.URL})

	mustSync(t, b, reg, true)
	assertItems(t, b.items(t), "tmdb:1=a", "tmdb:2=a", "tmdb:3=a")

	a.jf.set(movies(1, 3))
	if _, err := a.ix.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	mustSync(t, b, reg, true)
	assertItems(t, b.items(t), "tmdb:3=a")
}

func TestSyncElectsSmallestPeerAndDropsRemovedPeer(t *testing.T) {
	a := newNode(t, movies(2, 1))
	c := newNode(t, movies(2, 2))
	b := newNode(t, nil)
	reg := b.registry(t, config.Peer{ID: "c", URL: c.srv.URL}, config.Peer{ID: "a", URL: a.srv.URL})

	mustSync(t, b, reg, true)
	assertItems(t, b.items(t), "tmdb:1=a", "tmdb:2=a", "tmdb:3=c")

	if err := reg.RemovePeer(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
	mustSync(t, b, reg, true)
	// tmdb:1 had no .strm yet, so its cleared row is deleted outright.
	assertItems(t, b.items(t), "tmdb:2=c", "tmdb:3=c")
	if got := b.mirrorSize(t, "a"); got != 0 {
		t.Errorf("removed peer's mirror still has %d rows", got)
	}
}

func TestChangesFeed(t *testing.T) {
	a := newNode(t, movies(3, 1))
	ctx := context.Background()
	if _, err := a.ix.Refresh(ctx); err != nil {
		t.Fatal(err)
	}

	f, err := a.ix.Changes(ctx, "", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !f.Reset || f.More || len(f.Changes) != 3 || f.Next != 3 || f.Head != 3 {
		t.Fatalf("first page = %+v", f)
	}

	a.jf.set(movies(2, 2))
	if _, err := a.ix.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	f, err = a.ix.Changes(ctx, f.Epoch, f.Next, 10)
	if err != nil {
		t.Fatal(err)
	}
	if f.Reset || len(f.Changes) != 1 || !f.Changes[0].Deleted || f.Changes[0].GlobalID != "tmdb:1" {
		t.Fatalf("delta page = %+v", f)
	}

	// A cursor ahead of head (e.g. from before a DB wipe with same epoch)
	// is unusable and restarts the feed.
	f, err = a.ix.Changes(ctx, f.Epoch, 99, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !f.Reset || len(f.Changes) != 2 {
		t.Fatalf("out-of-range page = %+v", f)
	}
}

func TestLastSyncTimeOnlyMovesOnSuccess(t *testing.T) {
	a := newNode(t, movies(2, 1))
	b := newNode(t, nil)
	reg := b.registry(t, config.Peer{ID: "a", URL: a.srv.URL})
	ctx := context.Background()

	if got, err := LastSyncTimes(ctx, b.db); err != nil || len(got) != 0 {
		t.Fatalf("before any sync: %v, %v; want none", got, err)
	}

	mustSync(t, b, reg, true)
	got, err := LastSyncTimes(ctx, b.db)
	if err != nil || got["a"].IsZero() {
		t.Fatalf("after sync: %v, %v; want a timestamp for peer a", got, err)
	}
	first := got["a"]

	// Pretend it was long ago, then fail a pull: the timestamp must stay.
	if _, err := b.db.Exec(`UPDATE peer_sync_state SET synced_at = 1000`); err != nil {
		t.Fatal(err)
	}
	a.srv.Close()
	st, err := Sync(ctx, b.db, b.ix, reg, false, time.Second, "test-node")
	if err != nil || st.Peers[0].Err == nil {
		t.Fatalf("expected the pull to fail, got stats %+v, err %v", st.Peers, err)
	}
	got, _ = LastSyncTimes(ctx, b.db)
	if got["a"].Unix() != 1000 {
		t.Errorf("failed pull moved last sync time to %v (first success was %v)", got["a"], first)
	}
}

// TestSyncKeepsUnreachablePeerItemsUntilWithdrawAfter checks that a peer
// that's DEGRADED or OFFLINE keeps its items elected (from its last-known
// mirror, without being pulled) until it's been unreachable for longer than
// peers.WithdrawAfter.
func TestSyncKeepsUnreachablePeerItemsUntilWithdrawAfter(t *testing.T) {
	a := newNode(t, movies(2, 1))
	b := newNode(t, nil)
	cfg := config.Peer{ID: "a", URL: a.srv.URL}

	mustSync(t, b, b.registry(t, cfg), true)
	assertItems(t, b.items(t), "tmdb:1=a", "tmdb:2=a")

	setPeer := func(state peers.State, lastSeen time.Time) *peers.Registry {
		t.Helper()
		if _, err := b.db.Exec(`UPDATE peers SET state = ?, last_seen_at = ? WHERE id = 'a'`, state, lastSeen.Unix()); err != nil {
			t.Fatal(err)
		}
		return b.registry(t, cfg)
	}

	pulls := a.changes.Load()
	st := mustSync(t, b, setPeer(peers.StateOffline, time.Now().Add(-time.Minute)), false)
	assertItems(t, b.items(t), "tmdb:1=a", "tmdb:2=a")
	if len(st.Peers) != 0 || a.changes.Load() != pulls {
		t.Errorf("offline peer was pulled")
	}

	mustSync(t, b, setPeer(peers.StateOffline, time.Now().Add(-2*peers.WithdrawAfter)), false)
	assertItems(t, b.items(t))
}
