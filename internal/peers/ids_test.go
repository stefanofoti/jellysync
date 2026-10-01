package peers

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/google/uuid"

	"jellysync/internal/db"
)

func TestIDFor(t *testing.T) {
	taken := map[string]bool{"node-a": true, "node-a_01": true, "node-b": true}
	has := func(id string) bool { return taken[id] }

	cases := map[string]string{
		"node-c":       "node-c",
		"node-a":       "node-a_02",
		"Node-B":       "node-b_01",
		"  Node C Two": "node-c-two",
		"local":        "local_01",
		"unknown":      "unknown_01",
		"../etc":       "etc",
	}
	for name, want := range cases {
		if got := idFor(name, has); got != want {
			t.Errorf("idFor(%q) = %q, want %q", name, got, want)
		}
	}
	for _, name := range []string{"", "///"} {
		if _, err := uuid.Parse(idFor(name, has)); err != nil {
			t.Errorf("idFor(%q) should fall back to a UUID", name)
		}
	}
}

func TestMigrateUUIDIDs(t *testing.T) {
	store, err := db.Open(filepath.Join(t.TempDir(), "jellysync.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	a, b, c := uuid.NewString(), uuid.NewString(), uuid.NewString()
	for _, q := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO peers (id, url, name) VALUES (?, 'http://a', 'node-a')`, []any{a}},
		{`INSERT INTO peers (id, url, name) VALUES ('node-b', 'http://b', 'node-b')`, nil},
		{`INSERT INTO peers (id, url, name) VALUES (?, 'http://c', 'node-a')`, []any{b}},
		{`INSERT INTO peers (id, url, name) VALUES (?, 'http://d', '')`, []any{c}},
		{`INSERT INTO peer_catalog (peer_id, global_id, entry, gen) VALUES (?, 'g1', '{}', 0)`, []any{b}},
		{`INSERT INTO peer_sync_state (peer_id) VALUES (?)`, []any{b}},
		{`INSERT INTO peer_traffic (peer_id, direction, bytes) VALUES (?, 'in', 5)`, []any{b}},
		// Leftovers already keyed by the name b is about to take: legacy
		// outgoing traffic (summed) and a removed peer's stale mirror (dropped).
		{`INSERT INTO peer_traffic (peer_id, direction, bytes) VALUES ('node-a_01', 'in', 3), ('node-a_01', 'out', 4)`, nil},
		{`INSERT INTO peer_catalog (peer_id, global_id, entry, gen) VALUES ('node-a_01', 'stale', '{}', 0)`, nil},
		{`INSERT INTO peer_sync_state (peer_id) VALUES ('node-a_01')`, nil},
		{`INSERT INTO catalog_items (global_id, media_type, primary_peer_id, updated_at) VALUES ('g1', 'movie', ?, 0)`, []any{b}},
	} {
		if _, err := store.Exec(q.sql, q.args...); err != nil {
			t.Fatalf("%s: %v", q.sql, err)
		}
	}

	if err := migrateUUIDIDs(context.Background(), store); err != nil {
		t.Fatal(err)
	}

	rows, err := store.Query(`SELECT id FROM peers ORDER BY rowid`)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for rows.Next() {
		var id string
		rows.Scan(&id)
		ids = append(ids, id)
	}
	rows.Close()
	want := []string{"node-a", "node-b", "node-a_01", c}
	if len(ids) != len(want) {
		t.Fatalf("ids = %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("ids = %v, want %v", ids, want)
		}
	}

	var in, out int64
	store.QueryRow(`SELECT bytes FROM peer_traffic WHERE peer_id = 'node-a_01' AND direction = 'in'`).Scan(&in)
	store.QueryRow(`SELECT bytes FROM peer_traffic WHERE peer_id = 'node-a_01' AND direction = 'out'`).Scan(&out)
	if in != 8 || out != 4 {
		t.Errorf("traffic in/out = %d/%d, want 8/4", in, out)
	}

	for _, q := range []string{
		`SELECT peer_id FROM peer_catalog`,
		`SELECT peer_id FROM peer_sync_state`,
		`SELECT primary_peer_id FROM catalog_items`,
	} {
		var got string
		if err := store.QueryRow(q).Scan(&got); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if got != "node-a_01" {
			t.Errorf("%s = %q, want node-a_01", q, got)
		}
	}
}
