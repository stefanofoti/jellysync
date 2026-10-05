package strm

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"jellysync/internal/config"
	"jellysync/internal/db"
)

func newStore(t *testing.T) (*sql.DB, config.Strm) {
	t.Helper()
	dir := t.TempDir()
	store, err := db.Open(filepath.Join(dir, "jellysync.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store, config.Strm{OutputDir: filepath.Join(dir, "out"), BaseURL: "http://jellysync:8080"}
}

func mustExec(t *testing.T, store *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := store.Exec(query, args...); err != nil {
		t.Fatal(err)
	}
}

func mustReconcile(t *testing.T, store *sql.DB, cfg config.Strm) Stats {
	t.Helper()
	st, err := Reconcile(context.Background(), store, cfg)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return st
}

func strmPath(t *testing.T, store *sql.DB, globalID string) string {
	t.Helper()
	var p sql.NullString
	if err := store.QueryRow(`SELECT strm_path FROM catalog_items WHERE global_id = ?`, globalID).Scan(&p); err != nil {
		t.Fatal(err)
	}
	return p.String
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// readOnly makes dir unwritable (so nothing in it can be removed) until
// the returned func is called.
func readOnly(t *testing.T, dir string) func() {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	restore := func() { os.Chmod(dir, 0o755) }
	t.Cleanup(restore)
	return restore
}

const movieRow = `INSERT INTO catalog_items (global_id, name, media_type, primary_peer_id, primary_item_id, updated_at)
	VALUES (?, ?, 'movie', ?, ?, 0)`

func TestReconcileRetriesFailedRemoval(t *testing.T) {
	store, cfg := newStore(t)
	mustExec(t, store, movieRow, "tmdb:1", "Movie", "a", "jf-1")
	mustReconcile(t, store, cfg)
	path := strmPath(t, store, "tmdb:1")

	mustExec(t, store, `UPDATE catalog_items SET hidden = 1`)
	restore := readOnly(t, filepath.Dir(path))
	if st := mustReconcile(t, store, cfg); st.Failed != 1 || st.Removed != 0 {
		t.Fatalf("reconcile with unremovable file: %+v; want 1 failed", st)
	}
	if strmPath(t, store, "tmdb:1") != path || !exists(path) {
		t.Fatal("failed removal forgot the file's path")
	}

	restore()
	if st := mustReconcile(t, store, cfg); st.Removed != 1 || st.Failed != 0 {
		t.Fatalf("retry: %+v; want 1 removed", st)
	}
	if strmPath(t, store, "tmdb:1") != "" || exists(path) {
		t.Fatal("retry didn't remove the file")
	}
}

func TestReconcileMoveKeepsOldPathUntilRemoved(t *testing.T) {
	store, cfg := newStore(t)
	mustExec(t, store, movieRow, "tmdb:1", "Movie", "a", "jf-1")
	mustReconcile(t, store, cfg)
	oldPath := strmPath(t, store, "tmdb:1")

	mustExec(t, store, `UPDATE catalog_items SET primary_peer_id = 'b'`)
	restore := readOnly(t, filepath.Dir(oldPath))
	if st := mustReconcile(t, store, cfg); st.Written != 1 || st.Failed != 1 || st.Moved != 0 {
		t.Fatalf("move with unremovable old file: %+v; want 1 written, 1 failed", st)
	}
	if strmPath(t, store, "tmdb:1") != oldPath {
		t.Fatal("failed move forgot the old path")
	}
	newPath := filepath.Join(cfg.OutputDir, "movies", "b", "Movie [tmdbid-1]", "Movie [tmdbid-1].strm")
	if !exists(newPath) || !exists(oldPath) {
		t.Fatal("expected both files on disk until the old one can be removed")
	}

	restore()
	if st := mustReconcile(t, store, cfg); st.Moved != 1 || st.Failed != 0 {
		t.Fatalf("retry: %+v; want 1 moved", st)
	}
	if strmPath(t, store, "tmdb:1") != newPath || exists(oldPath) {
		t.Fatal("retry didn't finish the move")
	}
}

func TestReconcileSweepsUntrackedFiles(t *testing.T) {
	store, cfg := newStore(t)
	mustExec(t, store, movieRow, "tmdb:1", "Movie", "a", "jf-1")
	mustReconcile(t, store, cfg)
	tracked := strmPath(t, store, "tmdb:1")

	orphanDir := filepath.Join(cfg.OutputDir, "series", "gone", "Show [id-1]", "Season 01")
	orphan := filepath.Join(orphanDir, "Show - S01E01 - Pilot [id-2].strm")
	temp := filepath.Join(filepath.Dir(tracked), tempPrefix+"123.tmp")
	artwork := filepath.Join(filepath.Dir(tracked), "poster.jpg")
	if err := os.MkdirAll(orphanDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{orphan, temp, artwork} {
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	if st := mustReconcile(t, store, cfg); st.Swept != 2 || st.Unchanged != 1 {
		t.Fatalf("reconcile: %+v; want 2 swept, 1 unchanged", st)
	}
	if exists(orphan) || exists(temp) {
		t.Error("untracked .strm or temp file survived the sweep")
	}
	if !exists(tracked) || !exists(artwork) {
		t.Error("sweep removed a tracked .strm or a non-.strm file")
	}
	if exists(filepath.Join(cfg.OutputDir, "series", "gone")) {
		t.Error("sweep left the orphan's empty folders behind")
	}
	if !exists(filepath.Join(cfg.OutputDir, "series")) {
		t.Error("sweep removed the series root")
	}
}

func TestReconcileSkipsPathCollision(t *testing.T) {
	store, cfg := newStore(t)
	// Both fall back to the same truncated hash suffix.
	mustExec(t, store, movieRow, "hash:abcdefgh1", "Movie", "a", "jf-1")
	mustExec(t, store, movieRow, "hash:abcdefgh2", "Movie", "a", "jf-2")
	for range 2 {
		if st := mustReconcile(t, store, cfg); st.Failed != 1 || st.Written+st.Unchanged != 1 {
			t.Fatalf("reconcile: %+v; want 1 kept, 1 failed", st)
		}
	}
	if strmPath(t, store, "hash:abcdefgh1") == "" || strmPath(t, store, "hash:abcdefgh2") != "" {
		t.Error("the first item (by global_id) should own the path")
	}
}

func TestWriteIfChangedLeavesNoTempFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a", "a.strm")
	for _, content := range []string{"one", "two"} {
		if _, err := writeIfChanged(path, content); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := os.ReadFile(path)
	entries, _ := os.ReadDir(filepath.Dir(path))
	if string(got) != "two" || len(entries) != 1 {
		t.Fatalf("content %q, %d entries; want \"two\", 1", got, len(entries))
	}
	if info, _ := os.Stat(path); info.Mode().Perm() != 0o644 {
		t.Errorf("mode %v, want 0644", info.Mode().Perm())
	}
}

// TestRemoveStrmOutsideOutputDir covers a path recorded before OUTPUT_DIR
// changed: the file goes, but no directory outside outputDir is touched.
func TestRemoveStrmOutsideOutputDir(t *testing.T) {
	base := t.TempDir()
	oldDir := filepath.Join(base, "old", "movies", "a", "Movie")
	path := filepath.Join(oldDir, "Movie.strm")
	if err := os.MkdirAll(oldDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := removeStrm(path, filepath.Join(base, "new")); err != nil {
		t.Fatal(err)
	}
	if exists(path) {
		t.Error("file not removed")
	}
	if !exists(oldDir) {
		t.Error("removed a directory outside outputDir")
	}
}

func TestPathForLongNames(t *testing.T) {
	long := strings.Repeat("長い名前", 40) // 480 bytes
	for _, r := range []row{
		{globalID: "tmdb:1", name: long, mediaType: "movie", peerID: "a"},
		{globalID: "tvdb:2", name: long, mediaType: "episode", peerID: "a", seriesGlobalID: "tvdb:3", seriesName: long, seasonNumber: 1, episodeNumber: 2},
		{globalID: "tvdb:2", name: "Pilot", mediaType: "episode", peerID: "a", seriesGlobalID: "tvdb:3", seriesName: long, seasonNumber: 1, episodeNumber: 2},
		{globalID: "tmdb:" + strings.Repeat("9", 400), name: "Movie", mediaType: "movie", peerID: "a"},
	} {
		p := pathFor("/out", r)
		for _, elem := range strings.Split(p, string(filepath.Separator)) {
			if len(elem) > maxNameBytes || !utf8.ValidString(elem) {
				t.Errorf("path element %q: %d bytes (max %d), valid UTF-8 %v", elem, len(elem), maxNameBytes, utf8.ValidString(elem))
			}
		}
		if !strings.HasSuffix(p, ".strm") {
			t.Errorf("path %q lost its .strm extension", p)
		}
	}
	short := pathFor("/out", row{globalID: "tvdb:2", name: "Pilot", mediaType: "episode", peerID: "a", seriesGlobalID: "tvdb:3", seriesName: long, seasonNumber: 1, episodeNumber: 2})
	if !strings.Contains(filepath.Base(short), " - Pilot [tvdbid-2].strm") {
		t.Errorf("a short episode name should survive whole: %q", short)
	}
}

func TestReconcileRemovesFolderHiddenItemStrm(t *testing.T) {
	store, cfg := newStore(t)
	mustExec(t, store, movieRow, "tmdb:1", "Movie", "a", "jf-1")
	mustReconcile(t, store, cfg)
	path := strmPath(t, store, "tmdb:1")

	mustExec(t, store, `UPDATE catalog_items SET hidden_folder = 1`)
	if st := mustReconcile(t, store, cfg); st.Removed != 1 {
		t.Fatalf("reconcile after folder exclusion: %+v; want 1 removed", st)
	}
	if exists(path) || strmPath(t, store, "tmdb:1") != "" {
		t.Error(".strm of a folder-hidden item still there")
	}
}
