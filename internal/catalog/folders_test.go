package catalog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"jellysync/internal/config"
	"jellysync/internal/peers"
)

func TestUnderFolder(t *testing.T) {
	cases := []struct {
		path, folder string
		want         bool
	}{
		{"/Data/Media/Remote/film.mkv", "/Data/Media/Remote", true},
		{"/Data/Media/Remote/sub/film.mkv", "/Data/Media/Remote", true},
		{"/Data/Media/Remote", "/Data/Media/Remote", true},
		{"/Data/Media/Remote2/film.mkv", "/Data/Media/Remote", false},
		{"/Data/Media/Remote/film.mkv", "/Data/Media/Rem", false},
		{"/Data/Media/remote/film.mkv", "/Data/Media/Remote", false}, // case-sensitive
		{"/Data/Media/film.mkv", "/Data/Media/Remote", false},
		{`D:\Media\Remote\film.mkv`, "D:/Media/Remote", true},
		{"/anything.mkv", "/", true},
		{"", "/Data", false},
	}
	for _, c := range cases {
		if got := underFolder(c.path, c.folder); got != c.want {
			t.Errorf("underFolder(%q, %q) = %v, want %v", c.path, c.folder, got, c.want)
		}
	}
}

func TestNormalizeFolder(t *testing.T) {
	cases := map[string]string{
		" /Data/Media/Remote/ ": "/Data/Media/Remote",
		"/Data//":               "/Data",
		`D:\Media\`:             "D:/Media",
		"/":                     "/",
	}
	for in, want := range cases {
		if got := normalizeFolder(in); got != want {
			t.Errorf("normalizeFolder(%q) = %q, want %q", in, got, want)
		}
	}
}

// hiddenFolderItems returns catalog_items as "global_id=owner[:folder]"
// strings, like node.items plus a marker for rows hidden by folder.
func (n *node) hiddenFolderItems(t *testing.T) []string {
	t.Helper()
	rows, err := n.db.Query(`SELECT global_id, local, COALESCE(primary_peer_id, ''), hidden_folder, path FROM catalog_items`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id, peer, path string
		var local, folder bool
		if err := rows.Scan(&id, &local, &peer, &folder, &path); err != nil {
			t.Fatal(err)
		}
		if local {
			peer = "local"
		}
		s := id + "=" + peer
		if folder {
			s += ":folder"
		}
		if path == "" {
			s += ":nopath"
		}
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

func mustExclude(t *testing.T, n *node, peerID string, folders ...string) {
	t.Helper()
	if _, err := SetExcludedFolders(context.Background(), n.db, peerID, folders); err != nil {
		t.Fatal(err)
	}
}

// TestSyncFolderExclusions covers the election with excluded folders: an
// excluded offer gives way to another peer's, an item only offered from
// under excluded folders is listed as hidden by folder, a local item is
// never excluded, and removing the exclusion restores the item.
func TestSyncFolderExclusions(t *testing.T) {
	a := newNode(t, movies(3, 1)) // tmdb:1..3
	a.jf.dirs = map[string]string{"1": "/media/Remote", "2": "/media/Remote/sub", "3": "/media/Remote"}
	c := newNode(t, movies(1, 2)) // tmdb:2, outside any excluded folder
	b := newNode(t, map[string]string{"3": "Movie 3"})
	reg := b.registry(t, config.Peer{ID: "a", URL: a.srv.URL}, config.Peer{ID: "c", URL: c.srv.URL})

	mustSync(t, b, reg, true)
	assertItems(t, b.hiddenFolderItems(t), "tmdb:1=a", "tmdb:2=a", "tmdb:3=local")

	mustExclude(t, b, "a", "/media/Remote")
	mustSync(t, b, reg, false)
	// tmdb:1: only a offers it, so it stays listed but hidden by folder;
	// tmdb:2: c's offer wins over a's excluded one; tmdb:3: local wins.
	assertItems(t, b.hiddenFolderItems(t), "tmdb:1=a:folder", "tmdb:2=c", "tmdb:3=local")

	mustExclude(t, b, "a")
	mustSync(t, b, reg, false)
	assertItems(t, b.hiddenFolderItems(t), "tmdb:1=a", "tmdb:2=a", "tmdb:3=local")
}

func TestSyncCarriesPeerPath(t *testing.T) {
	a := newNode(t, movies(1, 1))
	a.jf.dirs = map[string]string{"1": "/Data/Media/folder"}
	b := newNode(t, nil)
	mustSync(t, b, b.registry(t, config.Peer{ID: "a", URL: a.srv.URL}), true)
	var path string
	if err := b.db.QueryRow(`SELECT path FROM catalog_items WHERE global_id = 'tmdb:1'`).Scan(&path); err != nil {
		t.Fatal(err)
	}
	if path != "/Data/Media/folder/1.mkv" {
		t.Errorf("path = %q, want the peer's /Data/Media/folder/1.mkv", path)
	}
}

func TestPruneRemovedPeerDropsExclusions(t *testing.T) {
	a := newNode(t, movies(1, 1))
	b := newNode(t, nil)
	reg := b.registry(t, config.Peer{ID: "a", URL: a.srv.URL})
	mustExclude(t, b, "a", "/media")
	if err := reg.RemovePeer(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
	mustSync(t, b, reg, true)
	all, err := excludedFolders(context.Background(), b.db)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 0 {
		t.Errorf("removed peer's exclusions survived: %v", all)
	}
}

func TestExcludedFoldersHandler(t *testing.T) {
	b := newNode(t, nil)
	changed := 0
	h := ExcludedFoldersHandler(b.db, func(id string) bool { return id == "a" }, func() { changed++ })
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/peers/{peerID}/excluded-folders", h)
	mux.HandleFunc("PUT /api/v1/peers/{peerID}/excluded-folders", h)

	do := func(method, peer, body string) (int, string) {
		req := httptest.NewRequest(method, "/api/v1/peers/"+peer+"/excluded-folders", strings.NewReader(body))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec.Code, strings.TrimSpace(rec.Body.String())
	}

	if code, body := do("GET", "a", ""); code != 200 || body != `{"folders":[]}` {
		t.Errorf("GET empty: %d %s", code, body)
	}
	if code, body := do("PUT", "a", `{"folders":["/Data/Media/Remote/", "/Data/Media/Remote", " /B "]}`); code != 200 || body != `{"folders":["/B","/Data/Media/Remote"]}` {
		t.Errorf("PUT: %d %s", code, body)
	}
	if changed != 1 {
		t.Errorf("changed called %d times, want 1", changed)
	}
	if code, body := do("GET", "a", ""); code != 200 || body != `{"folders":["/B","/Data/Media/Remote"]}` {
		t.Errorf("GET after PUT: %d %s", code, body)
	}
	if code, _ := do("PUT", "a", `{"folders":["relative/dir"]}`); code != 400 {
		t.Errorf("PUT relative: %d, want 400", code)
	}
	if code, _ := do("PUT", "a", `{"folders":[""]}`); code != 400 {
		t.Errorf("PUT empty: %d, want 400", code)
	}
	if code, _ := do("GET", "nobody", ""); code != 404 {
		t.Errorf("GET unknown peer: %d, want 404", code)
	}
	if changed != 1 {
		t.Errorf("rejected PUTs called changed")
	}
}

func getItemInfo(t *testing.T, n *node, reg *peers.Registry, globalID string) (int, itemInfo) {
	t.Helper()
	rec := httptest.NewRecorder()
	ItemInfoHandler(n.db, reg.List)(rec, httptest.NewRequest(http.MethodGet, "/api/v1/items/info?global_id="+globalID, nil))
	var info itemInfo
	if rec.Code == http.StatusOK {
		if err := json.NewDecoder(rec.Body).Decode(&info); err != nil {
			t.Fatal(err)
		}
	}
	return rec.Code, info
}

func offerSummary(info itemInfo) []string {
	out := make([]string, 0, len(info.Offers))
	for _, o := range info.Offers {
		s := o.Source + ":" + o.Path
		if o.Elected {
			s += ":elected"
		}
		if o.ExcludedFolder {
			s += ":excluded"
		}
		out = append(out, s)
	}
	return out
}

// TestItemInfoListsEveryOffer: the info endpoint lists every copy, elected
// first, with folder exclusions and media details.
func TestItemInfoListsEveryOffer(t *testing.T) {
	a := newNode(t, movies(2, 1)) // tmdb:1, tmdb:2
	a.jf.dirs = map[string]string{"1": "/a/Remote", "2": "/a/Remote"}
	c := newNode(t, movies(1, 2)) // tmdb:2
	c.jf.dirs = map[string]string{"2": "/c"}
	b := newNode(t, map[string]string{"1": "Movie 1"})
	reg := b.registry(t, config.Peer{ID: "a", URL: a.srv.URL}, config.Peer{ID: "c", URL: c.srv.URL})
	mustExclude(t, b, "a", "/a/Remote")
	mustSync(t, b, reg, true)

	// Local wins over a's copy.
	code, info := getItemInfo(t, b, reg, "tmdb:1")
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if got, want := strings.Join(offerSummary(info), " "), "local:/media/1.mkv:elected a:/a/Remote/1.mkv:excluded"; got != want {
		t.Errorf("tmdb:1 offers = %s, want %s", got, want)
	}
	if info.TmdbID != "1" || !info.Item.Local {
		t.Errorf("tmdb:1 info = %+v, want local with tmdb id 1", info)
	}

	// a's copy is excluded, so c's wins even though a sorts first.
	_, info = getItemInfo(t, b, reg, "tmdb:2")
	if got, want := strings.Join(offerSummary(info), " "), "c:/c/2.mkv:elected a:/a/Remote/2.mkv:excluded"; got != want {
		t.Errorf("tmdb:2 offers = %s, want %s", got, want)
	}
	if o := info.Offers[0]; o.PeerState != string(peers.StateOnline) || !o.Offering {
		t.Errorf("c's offer = %+v, want an online, offering peer", o)
	}

	if code, _ := getItemInfo(t, b, reg, "tmdb:99"); code != http.StatusNotFound {
		t.Errorf("unknown item: status %d, want 404", code)
	}
}
