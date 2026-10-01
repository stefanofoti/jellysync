package metrics

import (
	"context"
	"path/filepath"
	"testing"

	"jellysync/internal/db"
)

func TestReset(t *testing.T) {
	ctx := context.Background()
	store, err := db.Open(filepath.Join(t.TempDir(), "jellysync.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	c, err := NewCollector(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	c.RecordBytes("peer-a", In, 100)
	c.RecordBytes("peer-a", Out, 50)
	c.sample()
	c.flush(ctx)

	if err := c.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	if snap := c.Snapshot(); len(snap) != 0 {
		t.Errorf("snapshot after reset = %v, want empty", snap)
	}
	var rows int
	if err := store.QueryRow(`SELECT COUNT(*) FROM peer_traffic`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Errorf("peer_traffic rows after reset = %d, want 0", rows)
	}

	c.RecordBytes("peer-a", In, 7)
	if got := c.Snapshot()["peer-a"].TotalIn; got != 7 {
		t.Errorf("total in after reset = %d, want 7", got)
	}
}
