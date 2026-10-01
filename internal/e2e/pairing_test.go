package e2e

import (
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"jellysync/internal/peerauth"
)

// Beatrice pairs with Alice by invite, syncs her library over the peer
// port, and plays a movie through the .strm file it wrote.
func TestPairSyncAndStream(t *testing.T) {
	clk := newClock()
	alice := newNode(t, "alice", clk, map[string]string{"100": "Alpha", "101": "Beta"})
	bea := newNode(t, "beatrice", clk, map[string]string{"200": "Gamma"}, nodeOpt{noPeerPort: true})

	p := bea.mustRedeem(alice.invite())
	if p.ID != "alice" || !p.MTLS || p.URL != alice.publicURL() || p.Name != "alice" {
		t.Fatalf("paired peer = %+v", p)
	}

	bea.sync(true)
	assertItems(t, bea.items(), "tmdb:100=alice", "tmdb:101=alice", "tmdb:200=local")

	// Play: Jellyfin on Beatrice's side fetches the URL in the .strm file.
	url := bea.strmFor("alice", "100")
	if !strings.HasPrefix(url, bea.dash.URL+"/api/v1/proxy/stream/alice/") {
		t.Fatalf(".strm URL %q does not point at beatrice's own node", url)
	}
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(got) != string(movieBytes("100")) {
		t.Fatalf("stream: status %d, %d bytes (want %d)", resp.StatusCode, len(got), len(movieBytes("100")))
	}

	// Seeking: a Range request is passed through.
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Range", "bytes=10-19")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	got, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent || string(got) != string(movieBytes("100")[10:20]) {
		t.Fatalf("range: status %d body %q", resp.StatusCode, got)
	}

	// Traffic is attributed by key on both sides.
	want := int64(len(movieBytes("100")) + 10)
	if !eventually(t, func() bool { return alice.app.Collector().Snapshot()["beatrice"].TotalOut == want }) {
		t.Errorf("alice out to beatrice = %d, want %d", alice.app.Collector().Snapshot()["beatrice"].TotalOut, want)
	}
	if !eventually(t, func() bool { return bea.app.Collector().Snapshot()["alice"].TotalIn == want }) {
		t.Errorf("beatrice in from alice = %d, want %d", bea.app.Collector().Snapshot()["alice"].TotalIn, want)
	}
	if c, ok := alice.app.Clients().ClientByPin(bea.app.Identity().Pin()); !ok || c.ID != "beatrice" || c.LastSeenAt.IsZero() {
		t.Errorf("alice's client record = %+v %v", c, ok)
	}

	// The legacy dashboard port is unchanged: still unauthenticated.
	if status, _ := alice.api(http.MethodGet, "/api/v1/catalog/changes", nil); status != http.StatusOK {
		t.Errorf("legacy change feed on the dashboard port: %d", status)
	}
}

// Pairing is one-way: Alice authorizes Beatrice as a client, never adds
// her as a peer, and never dials her.
func TestPairingIsOneWay(t *testing.T) {
	clk := newClock()
	alice := newNode(t, "alice", clk, map[string]string{"100": "Alpha"})
	bea := newNode(t, "beatrice", clk, nil, nodeOpt{noPeerPort: true})
	bea.mustRedeem(alice.invite())
	bea.sync(true)

	if peers := alice.app.Registry().List(); len(peers) != 0 {
		t.Fatalf("alice has peers after being redeemed: %+v", peers)
	}
	if cl := alice.app.Clients().Clients(); len(cl) != 1 || cl[0].ID != "beatrice" || cl[0].Name != "beatrice" {
		t.Fatalf("alice's clients = %+v", cl)
	}
	status, body := alice.api(http.MethodGet, "/api/v1/clients", nil)
	if status != http.StatusOK || !strings.Contains(string(body), `"id":"beatrice"`) {
		t.Fatalf("GET /api/v1/clients = %d %s", status, body)
	}

	// Beatrice's library changes: Alice is told nothing she'd act on
	// (Beatrice is her client, not her peer).
	bea.jf.set(map[string]string{"900": "Bea's"})
	bea.sync(true)
	if _, ok := alice.app.Queue().Take(); ok {
		t.Fatal("a client's notification queued a sync on the node it reads from")
	}

	// Alice's library changes: nobody to notify, Beatrice catches up on
	// her own next sync.
	alice.jf.set(map[string]string{"100": "Alpha", "102": "Delta"})
	alice.sync(true)
	bea.sync(false)
	assertItems(t, bea.items(), "tmdb:100=alice", "tmdb:102=alice", "tmdb:900=local")
}

// Without a pinned key nothing reaches the peer port's handlers.
func TestUnauthenticatedCallersNeverReachHandlers(t *testing.T) {
	clk := newClock()
	alice := newNode(t, "alice", clk, map[string]string{"100": "Alpha"})
	url := alice.publicURL() + "/health"

	// No client certificate.
	noCert := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}, Timeout: 5 * time.Second}
	if _, _, err := do(t, noCert, http.MethodGet, url, nil); err == nil {
		t.Error("request without a client certificate succeeded")
	}
	// A self-signed key nobody pinned.
	if _, _, err := do(t, keyClient(randomIdentity(t), alice), http.MethodGet, url, nil); err == nil {
		t.Error("request with an unknown key succeeded")
	}
	// Plain HTTP on the TLS port.
	if resp, err := http.Get(strings.Replace(url, "https://", "http://", 1)); err == nil {
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("plain HTTP on the peer port: %d", resp.StatusCode)
		}
	}
	if hits := alice.peerHits.Load(); hits != 0 {
		t.Fatalf("peer port handler reached %d times by unauthenticated callers", hits)
	}
}

// An invite pointed at another node fails the pin check: Beatrice never
// sends anything to the impostor, and nothing is stored.
func TestRedeemAgainstImpostorFails(t *testing.T) {
	clk := newClock()
	alice := newNode(t, "alice", clk, nil)
	mallory := newNode(t, "mallory", clk, nil)
	bea := newNode(t, "beatrice", clk, nil, nodeOpt{noPeerPort: true})

	inv, err := peerauth.ParseInvite(alice.invite())
	if err != nil {
		t.Fatal(err)
	}
	inv.URL = mallory.publicURL()
	status, body, _ := bea.redeem(inv.Encode())
	if status != http.StatusUnprocessableEntity || !strings.Contains(body, "not the one that created this invite") {
		t.Fatalf("redeem against impostor: %d %s", status, body)
	}
	if mallory.peerHits.Load() != 0 {
		t.Fatal("impostor received a request")
	}
	if len(bea.app.Registry().List()) != 0 {
		t.Fatal("peer stored after a failed redeem")
	}
	// The real invite is untouched and still works.
	inv.URL = alice.publicURL()
	bea.mustRedeem(inv.Encode())
}

func TestInviteIsSingleUse(t *testing.T) {
	clk := newClock()
	alice := newNode(t, "alice", clk, nil)
	bea := newNode(t, "beatrice", clk, nil, nodeOpt{noPeerPort: true})
	carol := newNode(t, "carol", clk, nil, nodeOpt{noPeerPort: true})

	inv := alice.invite()
	bea.mustRedeem(inv)
	if status, body, _ := bea.redeem(inv); status != http.StatusUnprocessableEntity {
		t.Fatalf("second redeem by the same node: %d %s", status, body)
	}
	if status, body, _ := carol.redeem(inv); status != http.StatusUnprocessableEntity || !strings.Contains(body, "rejected") {
		t.Fatalf("redeem of a used invite by another node: %d %s", status, body)
	}
	if len(carol.app.Registry().List()) != 0 || len(alice.app.Clients().Clients()) != 1 {
		t.Fatal("used invite authorized a second node")
	}
}

func TestInviteExpires(t *testing.T) {
	clk := newClock()
	alice := newNode(t, "alice", clk, nil)
	bea := newNode(t, "beatrice", clk, nil, nodeOpt{noPeerPort: true})

	inv := alice.invite()
	clk.Add(25 * time.Hour) // only the issuer's clock decides
	if status, body, _ := bea.redeem(inv); status != http.StatusUnprocessableEntity {
		t.Fatalf("redeem of an expired invite: %d %s", status, body)
	}
	if status, body := alice.api(http.MethodGet, "/api/v1/invites", nil); status != http.StatusOK || strings.TrimSpace(string(body)) != "[]" {
		t.Fatalf("expired invite still listed: %d %s", status, body)
	}
	if len(alice.app.Clients().Clients()) != 0 {
		t.Fatal("expired invite authorized a client")
	}
}

func TestCancelledInviteIsRejected(t *testing.T) {
	clk := newClock()
	alice := newNode(t, "alice", clk, nil)
	bea := newNode(t, "beatrice", clk, nil, nodeOpt{noPeerPort: true})

	inv, _ := peerauth.ParseInvite(alice.invite())
	tmp, _ := peerauth.FromSeed(inv.Seed)
	if status, _ := alice.api(http.MethodDelete, "/api/v1/invites/"+tmp.Pin(), nil); status != http.StatusNoContent {
		t.Fatalf("cancel: %d", status)
	}
	if status, body, _ := bea.redeem(inv.Encode()); status != http.StatusUnprocessableEntity {
		t.Fatalf("redeem of a cancelled invite: %d %s", status, body)
	}
}

// An invite's temporary key can only pair: nothing else on the peer port.
func TestInviteKeyCanOnlyPair(t *testing.T) {
	clk := newClock()
	alice := newNode(t, "alice", clk, map[string]string{"100": "Alpha"})
	inv, _ := peerauth.ParseInvite(alice.invite())
	tmp, _ := peerauth.FromSeed(inv.Seed)
	c := keyClient(tmp, alice)
	base := alice.publicURL()

	for _, path := range []string{"/health", "/api/v1/catalog", "/api/v1/catalog/changes", "/api/v1/proxy/stream/local/jf-100", "/api/v1/sync/status"} {
		status, _, err := do(t, c, http.MethodGet, base+path, nil)
		if err != nil || status != http.StatusForbidden {
			t.Errorf("GET %s with an invite key: %d %v", path, status, err)
		}
	}
	if status, _, _ := do(t, c, http.MethodPost, base+"/api/v1/catalog/notify", nil); status != http.StatusForbidden {
		t.Errorf("notify with an invite key: %d", status)
	}

	me := randomIdentity(t)
	status, body, err := do(t, c, http.MethodPost, base+"/api/v1/pair", peerauth.PairRequest{Pin: me.Pin(), NodeID: "dave"})
	if err != nil || status != http.StatusOK {
		t.Fatalf("pair: %d %s %v", status, body, err)
	}
	var res peerauth.PairResponse
	json.Unmarshal(body, &res)
	if res.NodeID != "alice" || res.Version != "test" {
		t.Fatalf("pair response = %+v", res)
	}
	// The invite key is spent, even on the connection it used.
	if status, _, err := do(t, c, http.MethodPost, base+"/api/v1/pair", peerauth.PairRequest{Pin: randomIdentity(t).Pin(), NodeID: "eve"}); err == nil && status == http.StatusOK {
		t.Fatal("invite key paired twice")
	}
	// The new permanent key works.
	if status, _, err := do(t, keyClient(me, alice), http.MethodGet, base+"/api/v1/catalog/changes", nil); err != nil || status != http.StatusOK {
		t.Fatalf("paired key reading the feed: %d %v", status, err)
	}
}

// A client can read and stream, and nothing else: no dashboard, no admin,
// no relaying toward other peers, no notifications.
func TestClientScope(t *testing.T) {
	clk := newClock()
	alice := newNode(t, "alice", clk, map[string]string{"100": "Alpha"})
	bea := newNode(t, "beatrice", clk, nil, nodeOpt{noPeerPort: true})
	bea.mustRedeem(alice.invite())
	c := keyClient(bea.app.Identity(), alice)
	base := alice.publicURL()
	// A node Alice pulls from: a client must not reach it through her.
	if err := alice.app.Registry().AddPeer(t.Context(), "zed", "http://127.0.0.1:1", "zed", "", ""); err != nil {
		t.Fatal(err)
	}

	allowed := []string{"/health", "/api/v1/catalog", "/api/v1/catalog/changes", "/api/v1/proxy/stream/local/jf-100", "/api/v1/sync/status"}
	for _, path := range allowed {
		if status, body, err := do(t, c, http.MethodGet, base+path, nil); err != nil || status != http.StatusOK {
			t.Errorf("GET %s: %d %s %v", path, status, body, err)
		}
	}
	notRouted := []struct{ method, path string }{
		{http.MethodGet, "/"},
		{http.MethodGet, "/api/v1/peers"},
		{http.MethodPost, "/api/v1/peers"},
		{http.MethodPost, "/api/v1/peers/redeem"},
		{http.MethodGet, "/api/v1/settings"},
		{http.MethodPut, "/api/v1/settings"},
		{http.MethodDelete, "/api/v1/traffic"},
		{http.MethodGet, "/api/v1/traffic"},
		{http.MethodGet, "/metrics"},
		{http.MethodPost, "/api/v1/invites"},
		{http.MethodGet, "/api/v1/clients"},
		{http.MethodDelete, "/api/v1/clients/beatrice"},
		{http.MethodGet, "/api/v1/items"},
		{http.MethodGet, "/api/v1/proxy/stream/zed/jf-1"},
		{http.MethodPost, "/api/v1/sync/trigger/zed"},
	}
	for _, r := range notRouted {
		status, _, err := do(t, c, r.method, base+r.path, nil)
		if err != nil || (status != http.StatusNotFound && status != http.StatusMethodNotAllowed) {
			t.Errorf("%s %s: %d %v, want not routed", r.method, r.path, status, err)
		}
	}
	if status, _, _ := do(t, c, http.MethodPost, base+"/api/v1/catalog/notify", nil); status != http.StatusForbidden {
		t.Errorf("notify from a client: %d", status)
	}
	if status, _, _ := do(t, c, http.MethodPost, base+"/api/v1/pair", peerauth.PairRequest{Pin: randomIdentity(t).Pin()}); status != http.StatusForbidden {
		t.Errorf("pair from a client: %d", status)
	}

	// Sync status only shows Alice's own run, not who she pulls from.
	alice.sync(true)
	_, body, _ := do(t, c, http.MethodGet, base+"/api/v1/sync/status", nil)
	var st map[string]any
	json.Unmarshal(body, &st)
	if _, ok := st["local"]; !ok || len(st) != 1 {
		t.Errorf("client sees sync status %s", body)
	}
}

// Revoking a client cuts it off at once, even on an open connection, and
// its own node keeps its last known copy of the catalog.
func TestRevokeClient(t *testing.T) {
	clk := newClock()
	alice := newNode(t, "alice", clk, map[string]string{"100": "Alpha"})
	bea := newNode(t, "beatrice", clk, nil, nodeOpt{noPeerPort: true})
	bea.mustRedeem(alice.invite())
	bea.sync(true)

	// Keep a connection open with Beatrice's own transport.
	c := &http.Client{Transport: bea.app.Registry().Transport()}
	if status, _, err := do(t, c, http.MethodGet, alice.publicURL()+"/health", nil); err != nil || status != http.StatusOK {
		t.Fatalf("health before revoke: %d %v", status, err)
	}
	if status, _ := alice.api(http.MethodDelete, "/api/v1/clients/beatrice", nil); status != http.StatusNoContent {
		t.Fatalf("revoke: %d", status)
	}
	if status, _, err := do(t, c, http.MethodGet, alice.publicURL()+"/health", nil); err != nil || status != http.StatusForbidden {
		t.Fatalf("open connection after revoke: %d %v", status, err)
	}
	bea.app.Registry().Transport().CloseIdleConnections()
	if _, _, err := do(t, c, http.MethodGet, alice.publicURL()+"/health", nil); err == nil {
		t.Fatal("new connection after revoke succeeded")
	}

	// Beatrice's pull now fails; she keeps what she had.
	bea.sync(false)
	assertItems(t, bea.items(), "tmdb:100=alice")
	if bea.mirrorSize("alice") != 1 {
		t.Fatal("mirror dropped after a failed pull")
	}

	// A fresh invite re-pairs the same key under the same ids.
	p := bea.mustRedeem(alice.invite())
	if p.ID != "alice" || len(bea.app.Registry().List()) != 1 {
		t.Fatalf("re-pair: %+v", p)
	}
	if cl := alice.app.Clients().Clients(); len(cl) != 1 || cl[0].ID != "beatrice" {
		t.Fatalf("clients after re-pair: %+v", cl)
	}
}

// A client may make Alice re-read her library, at most once per window.
func TestForceRefreshIsRateLimited(t *testing.T) {
	clk := newClock()
	alice := newNode(t, "alice", clk, nil)
	bea := newNode(t, "beatrice", clk, nil, nodeOpt{noPeerPort: true})
	bea.mustRedeem(alice.invite())
	c := keyClient(bea.app.Identity(), alice)
	url := alice.publicURL() + "/api/v1/sync/trigger/local"

	if status, _, _ := do(t, c, http.MethodPost, url, nil); status != http.StatusAccepted {
		t.Fatalf("first refresh: %d", status)
	}
	if req, ok := alice.app.Queue().Take(); !ok || !req.RefreshLocal {
		t.Fatalf("queued sync = %+v %v", req, ok)
	}
	if status, _, _ := do(t, c, http.MethodPost, url, nil); status != http.StatusTooManyRequests {
		t.Fatalf("second refresh: %d", status)
	}
	if _, ok := alice.app.Queue().Take(); ok {
		t.Fatal("rate-limited refresh still queued a sync")
	}
	clk.Add(10 * time.Minute)
	if status, _, _ := do(t, c, http.MethodPost, url, nil); status != http.StatusAccepted {
		t.Fatalf("refresh after the window: %d", status)
	}
}

// Legacy http:// peers and mTLS peers sync side by side.
func TestLegacyAndPairedPeersCoexist(t *testing.T) {
	clk := newClock()
	alice := newNode(t, "alice", clk, map[string]string{"100": "Alpha", "300": "Shared"})
	carol := newNode(t, "carol", clk, map[string]string{"300": "Shared", "301": "Carol only"}, nodeOpt{noPeerPort: true})
	bea := newNode(t, "beatrice", clk, nil, nodeOpt{noPeerPort: true})

	bea.mustRedeem(alice.invite())
	if status, body := bea.api(http.MethodPost, "/api/v1/peers", map[string]string{"url": carol.dash.URL}); status != http.StatusNoContent {
		t.Fatalf("add legacy peer: %d %s", status, body)
	}
	bea.sync(true)
	// Smallest peer id wins a shared item, regardless of transport.
	assertItems(t, bea.items(), "tmdb:100=alice", "tmdb:300=alice", "tmdb:301=carol")

	for _, tc := range []struct{ peer, id string }{{"alice", "100"}, {"carol", "301"}} {
		resp, err := http.Get(bea.strmFor(tc.peer, tc.id))
		if err != nil {
			t.Fatal(err)
		}
		got, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(got) != string(movieBytes(tc.id)) {
			t.Errorf("stream %s from %s: %d bytes", tc.id, tc.peer, len(got))
		}
	}

	// Alice's peer port added by URL is a legacy https:// peer: plain
	// CA-verified TLS, which her self-signed key (and missing client
	// certificate) fails. Only an invite gets in.
	if status, _ := carol.api(http.MethodPost, "/api/v1/peers", map[string]string{"url": alice.publicURL()}); status == http.StatusNoContent {
		t.Fatal("peer port reachable without pairing")
	}
}

// Redeeming an invite from a node already added by URL upgrades that peer
// in place: same id, same .strm paths, same catalog cursor, no resync.
func TestRedeemUpgradesLegacyPeerInPlace(t *testing.T) {
	clk := newClock()
	alice := newNode(t, "alice", clk, map[string]string{"100": "Alpha", "101": "Beta"})
	bea := newNode(t, "beatrice", clk, nil, nodeOpt{noPeerPort: true})

	if status, body := bea.api(http.MethodPost, "/api/v1/peers", map[string]string{"url": alice.dash.URL}); status != http.StatusNoContent {
		t.Fatalf("add legacy peer: %d %s", status, body)
	}
	bea.sync(true)
	before := bea.strmFiles()
	var epoch string
	var rev, gen int64
	bea.db.QueryRow(`SELECT epoch, rev, gen FROM peer_sync_state WHERE peer_id = 'alice'`).Scan(&epoch, &rev, &gen)
	if len(before) != 2 || epoch == "" {
		t.Fatalf("legacy sync: files %v epoch %q", before, epoch)
	}

	p := bea.mustRedeem(alice.invite())
	if p.ID != "alice" || !p.MTLS || p.URL != alice.publicURL() || len(bea.app.Registry().List()) != 1 {
		t.Fatalf("upgraded peer = %+v (peers %d)", p, len(bea.app.Registry().List()))
	}

	alice.jf.set(map[string]string{"100": "Alpha", "101": "Beta", "102": "Gamma"})
	alice.sync(true)
	hits := alice.peerHits.Load()
	bea.sync(false)
	if alice.peerHits.Load() == hits {
		t.Fatal("sync after upgrade did not use the peer port")
	}
	var gen2 int64
	var epoch2 string
	bea.db.QueryRow(`SELECT epoch, gen FROM peer_sync_state WHERE peer_id = 'alice'`).Scan(&epoch2, &gen2)
	if epoch2 != epoch || gen2 != gen {
		t.Fatalf("cursor reset by the upgrade: epoch %q->%q gen %d->%d", epoch, epoch2, gen, gen2)
	}
	after := bea.strmFiles()
	for rel, u := range before {
		if after[rel] != u {
			t.Errorf(".strm %s changed: %q -> %q", rel, u, after[rel])
		}
	}
	if len(after) != 3 {
		t.Fatalf(".strm files after upgrade = %v", after)
	}
}

// Two invites, one each way: each node is the other's client and peer,
// under one id, and change notifications flow over the peer port.
func TestMutualPairing(t *testing.T) {
	clk := newClock()
	alice := newNode(t, "alice", clk, map[string]string{"100": "Alpha"})
	bea := newNode(t, "beatrice", clk, map[string]string{"200": "Gamma"})

	bea.mustRedeem(alice.invite())
	if p := alice.mustRedeem(bea.invite()); p.ID != "beatrice" {
		t.Fatalf("alice's peer id for beatrice = %q", p.ID)
	}
	if cl := alice.app.Clients().Clients(); len(cl) != 1 || cl[0].ID != "beatrice" {
		t.Fatalf("alice's clients = %+v", cl)
	}

	alice.sync(true)
	bea.sync(true)
	assertItems(t, alice.items(), "tmdb:100=local", "tmdb:200=beatrice")
	assertItems(t, bea.items(), "tmdb:100=alice", "tmdb:200=local")

	// Alice's library changes: she notifies Beatrice over the peer port.
	for bea.app.Queue() != nil {
		if _, ok := bea.app.Queue().Take(); !ok {
			break
		}
	}
	alice.jf.set(map[string]string{"100": "Alpha", "101": "Beta"})
	alice.sync(true)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if req, ok := bea.app.Queue().Take(); ok {
			if req.RefreshLocal || req.Reason != "peer-notify" {
				t.Fatalf("notified sync = %+v", req)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("beatrice was not notified")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Traffic both ways lands under one id on Alice's side.
	for _, u := range []string{bea.strmFor("alice", "100"), alice.strmFor("beatrice", "200")} {
		resp, err := http.Get(u)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	ok := eventually(t, func() bool {
		snap := alice.app.Collector().Snapshot()
		return len(snap) == 1 && snap["beatrice"].TotalIn > 0 && snap["beatrice"].TotalOut > 0
	})
	if !ok {
		t.Fatalf("alice's traffic = %+v", alice.app.Collector().Snapshot())
	}
}

// Keys, clients and pairings survive a restart.
func TestPairingSurvivesRestart(t *testing.T) {
	clk := newClock()
	alice := newNode(t, "alice", clk, map[string]string{"100": "Alpha"})
	bea := newNode(t, "beatrice", clk, nil, nodeOpt{noPeerPort: true})
	bea.mustRedeem(alice.invite())
	alicePin, beaPin := alice.app.Identity().Pin(), bea.app.Identity().Pin()

	alice.restart()
	bea.restart()
	if alice.app.Identity().Pin() != alicePin || bea.app.Identity().Pin() != beaPin {
		t.Fatal("identity changed across restart")
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
		t.Fatalf("stream after restart: %d bytes", len(got))
	}
}

func TestInvitesNeedPublicURL(t *testing.T) {
	bea := newNode(t, "beatrice", newClock(), nil, nodeOpt{noPeerPort: true})
	if status, _ := bea.api(http.MethodPost, "/api/v1/invites", nil); status != http.StatusConflict {
		t.Fatalf("invite without PUBLIC_URL: %d", status)
	}
}
