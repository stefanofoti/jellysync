// Package db opens the node's local SQLite store.
package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS peers (
	id           TEXT PRIMARY KEY,
	url          TEXT NOT NULL,
	state        TEXT NOT NULL DEFAULT 'ONLINE',
	last_seen_at INTEGER
);

CREATE TABLE IF NOT EXISTS catalog_items (
	global_id        TEXT PRIMARY KEY,
	name             TEXT NOT NULL DEFAULT '',
	media_type       TEXT NOT NULL,
	local            INTEGER NOT NULL DEFAULT 0,
	local_item_id    TEXT,
	primary_peer_id  TEXT,
	primary_item_id  TEXT,
	strm_path        TEXT,
	series_global_id TEXT NOT NULL DEFAULT '',
	series_name      TEXT NOT NULL DEFAULT '',
	season_number    INTEGER NOT NULL DEFAULT 0,
	episode_number   INTEGER NOT NULL DEFAULT 0,
	updated_at       INTEGER NOT NULL
);
`

// seriesColumns are added to catalog_items via ALTER TABLE for databases
// created before series support existed: CREATE TABLE IF NOT EXISTS above
// is a no-op against an already-existing table, and SQLite has no
// "ADD COLUMN IF NOT EXISTS", so each is added individually guarded by a
// PRAGMA table_info check.
var seriesColumns = []string{
	"series_global_id TEXT NOT NULL DEFAULT ''",
	"series_name TEXT NOT NULL DEFAULT ''",
	"season_number INTEGER NOT NULL DEFAULT 0",
	"episode_number INTEGER NOT NULL DEFAULT 0",
}

// Open creates the parent directory if needed, opens the SQLite file at
// path, and applies the schema (idempotent: CREATE TABLE IF NOT EXISTS).
func Open(path string) (*sql.DB, error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("creating db dir %s: %w", dir, err)
		}
	}

	// WAL plus a busy_timeout: the heartbeat loop, catalog sync and the API
	// handlers all hit this DB from separate goroutines, and SQLite's
	// default rollback journal fails a writer immediately (SQLITE_BUSY)
	// instead of waiting when another writer holds the lock.
	dsn := "file:" + path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(wal)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening db %s: %w", path, err)
	}

	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("applying schema to %s: %w", path, err)
	}

	if err := addMissingColumns(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrating schema: %w", err)
	}

	return db, nil
}

// addMissingColumns adds any catalog_items column from seriesColumns that
// isn't already present, for databases created before series support
// existed.
func addMissingColumns(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(catalog_items)`)
	if err != nil {
		return err
	}
	existing := make(map[string]bool)
	for rows.Next() {
		var cid int
		var name, colType string
		var notNull, pk int
		var dfltValue sql.NullString
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dfltValue, &pk); err != nil {
			rows.Close()
			return err
		}
		existing[name] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, col := range seriesColumns {
		colName, _, _ := strings.Cut(col, " ")
		if existing[colName] {
			continue
		}
		if _, err := db.Exec(`ALTER TABLE catalog_items ADD COLUMN ` + col); err != nil {
			return fmt.Errorf("adding column %s: %w", colName, err)
		}
	}
	return nil
}
