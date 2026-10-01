package strm

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"jellysync/internal/config"
	"jellysync/internal/db"
)

func TestReconcileRemovesHiddenItemStrm(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := db.Open(filepath.Join(dir, "jellysync.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.Exec(`
		INSERT INTO catalog_items (global_id, name, media_type, primary_peer_id, primary_item_id, updated_at)
		VALUES ('tmdb:1', 'Movie', 'movie', 'a', 'jf-1', 0)
	`); err != nil {
		t.Fatal(err)
	}
	cfg := config.Strm{OutputDir: filepath.Join(dir, "out"), BaseURL: "http://jellysync:8080"}

	if st, err := Reconcile(ctx, store, cfg); err != nil || st.Written != 1 {
		t.Fatalf("first reconcile: %+v, %v; want 1 written", st, err)
	}
	var path string
	if err := store.QueryRow(`SELECT strm_path FROM catalog_items`).Scan(&path); err != nil {
		t.Fatal(err)
	}

	if _, err := store.Exec(`UPDATE catalog_items SET hidden = 1`); err != nil {
		t.Fatal(err)
	}
	if st, err := Reconcile(ctx, store, cfg); err != nil || st.Removed != 1 {
		t.Fatalf("reconcile after hiding: %+v, %v; want 1 removed", st, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf(".strm of hidden item still on disk: %v", err)
	}
}
