package e2e

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"jellysync/internal/peers"
)

func assertNoStrmFrom(t *testing.T, n *node, peerID string) {
	t.Helper()
	for rel := range n.strmFiles() {
		if strings.HasPrefix(rel, "movies/"+peerID+"/") {
			t.Fatalf("%s: .strm from disabled peer %s still present: %s", n.name, peerID, rel)
		}
	}
}

func peerState(t *testing.T, n *node, id string) peers.State {
	t.Helper()
	p, ok := n.app.Registry().Get(id)
	if !ok {
		t.Fatalf("%s: no peer %s", n.name, id)
	}
	return p.State
}

// The production default (HTTP_PEERS on, MTLS_PEERS off) behaves exactly
// like earlier versions: legacy peers work, nothing about pairing does.
func TestDefaultTransportsAreLegacyOnly(t *testing.T) {
	clk := newClock()
	alice := newNode(t, "alice", clk, map[string]string{"100": "Alpha"}, nodeOpt{mtlsOff: true})
	bea := newNode(t, "beatrice", clk, nil, nodeOpt{mtlsOff: true})

	if status, _ := alice.api(http.MethodPost, "/api/v1/invites", nil); status != http.StatusConflict {
		t.Fatalf("create invite with MTLS_PEERS off: %d", status)
	}
	if status, _, _ := bea.redeem("jellysync1:whatever"); status != http.StatusConflict {
		t.Fatalf("redeem with MTLS_PEERS off: %d", status)
	}
	_, body := alice.api(http.MethodGet, "/api/v1/peerport", nil)
	var info map[string]any
	json.Unmarshal(body, &info)
	if info["http_peers"] != true || info["mtls_peers"] != false || info["invites_enabled"] != false {
		t.Fatalf("peerport info = %s", body)
	}

	if status, body := bea.api(http.MethodPost, "/api/v1/peers", map[string]string{"url": alice.dash.URL}); status != http.StatusNoContent {
		t.Fatalf("add legacy peer: %d %s", status, body)
	}
	bea.sync(true)
	assertItems(t, bea.items(), "tmdb:100=alice")
	resp, err := http.Get(bea.strmFor("alice", "100"))
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(got) != string(movieBytes("100")) {
		t.Fatalf("legacy stream: %d bytes", len(got))
	}
}

// HTTP_PEERS=false drops the legacy peer API from the dashboard port and
// sidelines legacy peers, without losing them: turning it back on resumes
// them with no resync.
func TestHTTPPeersOff(t *testing.T) {
	clk := newClock()
	alice := newNode(t, "alice", clk, map[string]string{"100": "Alpha"})
	carol := newNode(t, "carol", clk, map[string]string{"300": "Carol only"}, nodeOpt{noPeerPort: true})
	bea := newNode(t, "beatrice", clk, nil, nodeOpt{noPeerPort: true})

	bea.mustRedeem(alice.invite())
	if status, body := bea.api(http.MethodPost, "/api/v1/peers", map[string]string{"url": carol.dash.URL}); status != http.StatusNoContent {
		t.Fatalf("add legacy peer: %d %s", status, body)
	}
	bea.sync(true)
	assertItems(t, bea.items(), "tmdb:100=alice", "tmdb:300=carol")
	var gen int64
	bea.db.QueryRow(`SELECT gen FROM peer_sync_state WHERE peer_id = 'carol'`).Scan(&gen)

	bea.httpOff = true
	bea.restart()
	if s := peerState(t, bea, "carol"); s != peers.StateDisabled {
		t.Fatalf("legacy peer state = %s", s)
	}
	if s := peerState(t, bea, "alice"); s == peers.StateDisabled {
		t.Fatal("mTLS peer disabled by HTTP_PEERS=false")
	}
	// Beatrice's own legacy peer API is gone; dashboard and local playback stay.
	for _, path := range []string{"/api/v1/catalog", "/api/v1/catalog/changes"} {
		if status, _ := bea.api(http.MethodGet, path, nil); status != http.StatusNotFound {
			t.Errorf("GET %s with HTTP_PEERS off: %d", path, status)
		}
	}
	if status, _ := bea.api(http.MethodPost, "/api/v1/catalog/notify", nil); status != http.StatusNotFound {
		t.Errorf("notify with HTTP_PEERS off: %d", status)
	}
	if status, _ := bea.api(http.MethodGet, "/api/v1/peers", nil); status != http.StatusOK {
		t.Error("dashboard API gone with HTTP_PEERS off")
	}
	if status, _ := bea.api(http.MethodPost, "/api/v1/peers", map[string]string{"url": carol.dash.URL}); status != http.StatusConflict {
		t.Errorf("add by URL with HTTP_PEERS off: %d", status)
	}
	if status, _ := bea.api(http.MethodPost, "/api/v1/sync/trigger/carol", nil); status != http.StatusConflict {
		t.Errorf("sync trigger for a disabled peer: %d", status)
	}
	if status, _ := bea.api(http.MethodGet, "/api/v1/proxy/stream/carol/jf-300", nil); status != http.StatusServiceUnavailable {
		t.Errorf("stream from a disabled peer: %d", status)
	}

	// Carol's items are withdrawn like an offline peer's (pointer file
	// removed this cycle, row the next), her mirror kept; Alice's still flow.
	bea.sync(true)
	assertNoStrmFrom(t, bea, "carol")
	bea.sync(true)
	assertItems(t, bea.items(), "tmdb:100=alice")
	if bea.mirrorSize("carol") != 1 {
		t.Fatal("disabled peer's mirror dropped")
	}
	resp, err := http.Get(bea.strmFor("alice", "100"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("mTLS stream with HTTP_PEERS off: %d", resp.StatusCode)
	}

	bea.httpOff = false
	bea.restart()
	bea.sync(true)
	assertItems(t, bea.items(), "tmdb:100=alice", "tmdb:300=carol")
	var gen2 int64
	bea.db.QueryRow(`SELECT gen FROM peer_sync_state WHERE peer_id = 'carol'`).Scan(&gen2)
	if gen2 != gen {
		t.Fatalf("re-enabling legacy peers resynced carol: gen %d -> %d", gen, gen2)
	}
}

// MTLS_PEERS=false sidelines paired peers and closes the peer port, while
// legacy peers carry on.
func TestMTLSPeersOff(t *testing.T) {
	clk := newClock()
	alice := newNode(t, "alice", clk, map[string]string{"100": "Alpha"})
	carol := newNode(t, "carol", clk, map[string]string{"300": "Carol only"}, nodeOpt{noPeerPort: true})
	bea := newNode(t, "beatrice", clk, nil, nodeOpt{noPeerPort: true})
	bea.mustRedeem(alice.invite())
	if status, _ := bea.api(http.MethodPost, "/api/v1/peers", map[string]string{"url": carol.dash.URL}); status != http.StatusNoContent {
		t.Fatal("add legacy peer")
	}
	bea.sync(true)

	bea.mtlsOff = true
	bea.restart()
	if s := peerState(t, bea, "alice"); s != peers.StateDisabled {
		t.Fatalf("paired peer state = %s", s)
	}
	bea.sync(true)
	assertNoStrmFrom(t, bea, "alice")
	bea.sync(true)
	assertItems(t, bea.items(), "tmdb:300=carol")
	if bea.mirrorSize("alice") != 1 {
		t.Fatal("disabled peer's mirror dropped")
	}

	// Alice turns MTLS_PEERS off too: her peer port closes.
	alice.mtlsOff = true
	alice.restart()
	if _, _, err := do(t, keyClient(bea.app.Identity(), alice), http.MethodGet, alice.publicURL()+"/health", nil); err == nil {
		t.Fatal("peer port still answering with MTLS_PEERS off")
	}
	if status, body := alice.api(http.MethodPost, "/api/v1/invites", nil); status != http.StatusConflict || !strings.Contains(string(body), "MTLS_PEERS") {
		t.Fatalf("create invite with MTLS_PEERS off: %d %s", status, body)
	}

	// Both back on: the pairing still holds.
	alice.mtlsOff, bea.mtlsOff = false, false
	alice.restart()
	bea.restart()
	bea.sync(true)
	assertItems(t, bea.items(), "tmdb:100=alice", "tmdb:300=carol")
}
