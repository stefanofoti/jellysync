package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"

	"jellysync/internal/jellyfin"
)

// Settings keys backing the local change feed. They live in the settings
// table rather than being derived from local_catalog (e.g. MAX(rev)) so
// they stay monotonic even after old tombstones are garbage-collected.
const (
	epochKey     = "catalog_epoch"      // random id of this feed's history; a new one forces every peer to resync
	headKey      = "catalog_head"       // last rev handed out
	minRevKey    = "catalog_min_rev"    // highest rev dropped by tombstone GC; cursors below it must resync
	indexedAtKey = "catalog_indexed_at" // unix time of the last successful Refresh; absent until the first one
)

// tombstoneTTL is how long a removal stays in the feed. A peer that hasn't
// pulled for longer than this gets a full resync instead of a delta, which
// is always correct, just more expensive.
const tombstoneTTL = 30 * 24 * time.Hour

// Index is this node's own catalog as served to peers: a snapshot of the
// local Jellyfin library (via LocalEntries) persisted in local_catalog with
// a per-row revision, so a peer can ask "what changed since rev N" instead
// of making this node list its whole Jellyfin library on every request.
//
// The snapshot is only as fresh as the last Refresh, which syncrun.RunLocal
// performs every cycle; when it finds changes, peers are nudged to pull
// right away (see NotifyPeers) rather than waiting out their own interval.
type Index struct {
	db      *sql.DB
	jf      *jellyfin.Client
	strmDir string

	// mu serializes Refresh, and is the only writer of local_catalog and
	// the feed's settings keys, so it can read them outside its write
	// transaction without racing another writer.
	mu sync.Mutex
}

// NewIndex returns the local catalog index, creating this node's feed epoch
// on first boot.
func NewIndex(ctx context.Context, db *sql.DB, jf *jellyfin.Client, strmDir string) (*Index, error) {
	if _, err := db.ExecContext(ctx, `INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)`, epochKey, uuid.NewString()); err != nil {
		return nil, fmt.Errorf("initializing catalog epoch: %w", err)
	}
	return &Index{db: db, jf: jf, strmDir: strmDir}, nil
}

// RefreshStats describes what one Refresh found.
type RefreshStats struct {
	Total   int // items currently in the local library
	Added   int
	Updated int
	Removed int
	Purged  int   // expired tombstones garbage-collected
	Head    int64 // feed head after the refresh
	// ListDuration is the Jellyfin listing alone; Duration the whole refresh.
	ListDuration time.Duration
	Duration     time.Duration
}

// Changed is how many items were added, updated or removed.
func (s RefreshStats) Changed() int { return s.Added + s.Updated + s.Removed }

// Refresh re-reads the local Jellyfin library and records every difference
// from the previous snapshot as a new revision, all in one transaction.
func (ix *Index) Refresh(ctx context.Context) (RefreshStats, error) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	return ix.refreshLocked(ctx)
}

// EnsureIndexed runs a Refresh only if none has ever succeeded, so the
// feed can be served on a brand-new node before its first sync cycle.
func (ix *Index) EnsureIndexed(ctx context.Context) error {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if _, ok, err := getSetting(ctx, ix.db, indexedAtKey); err != nil || ok {
		return err
	}
	slog.Info("building initial local catalog index")
	_, err := ix.refreshLocked(ctx)
	return err
}

type indexedRow struct {
	hash    string
	deleted bool
}

func (ix *Index) refreshLocked(ctx context.Context) (RefreshStats, error) {
	started := time.Now()
	var st RefreshStats
	entries, err := LocalEntries(ctx, ix.jf, ix.strmDir)
	if err != nil {
		return st, err
	}
	st.ListDuration = time.Since(started)
	// The same item can appear more than once (e.g. in two libraries); the
	// last one wins, as it always has.
	current := make(map[string]Entry, len(entries))
	for _, e := range entries {
		current[e.GlobalID] = e
	}

	if dups := len(entries) - len(current); dups > 0 {
		slog.Debug("local library lists some items more than once; keeping one each", "duplicates", dups)
	}
	st.Total = len(current)

	existing := make(map[string]indexedRow)
	rows, err := ix.db.QueryContext(ctx, `SELECT global_id, hash, deleted FROM local_catalog`)
	if err != nil {
		return st, err
	}
	for rows.Next() {
		var id string
		var r indexedRow
		if err := rows.Scan(&id, &r.hash, &r.deleted); err != nil {
			rows.Close()
			return st, err
		}
		existing[id] = r
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return st, err
	}

	head, err := getIntSetting(ctx, ix.db, headKey)
	if err != nil {
		return st, err
	}

	ids := make([]string, 0, len(current))
	for id := range current {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	now := time.Now()
	err = withTx(ctx, ix.db, func(tx *sql.Tx) error {
		// Write first: a transaction that reads before its first write can
		// fail to upgrade to a writer (SQLITE_BUSY, which busy_timeout
		// doesn't retry) if another connection committed in between.
		if err := setSetting(ctx, tx, indexedAtKey, strconv.FormatInt(now.Unix(), 10)); err != nil {
			return err
		}
		for _, id := range ids {
			raw, err := json.Marshal(current[id])
			if err != nil {
				return err
			}
			hash := contentHash(raw)
			old, ok := existing[id]
			switch {
			case ok && !old.deleted && old.hash == hash:
				continue
			case ok && !old.deleted:
				st.Updated++
			default:
				st.Added++
			}
			head++
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO local_catalog (global_id, entry, hash, rev, deleted, updated_at)
				VALUES (?, ?, ?, ?, 0, ?)
				ON CONFLICT(global_id) DO UPDATE SET
					entry = excluded.entry, hash = excluded.hash, rev = excluded.rev,
					deleted = 0, updated_at = excluded.updated_at
			`, id, string(raw), hash, head, now.Unix()); err != nil {
				return fmt.Errorf("indexing %s: %w", id, err)
			}
		}

		removed := make([]string, 0)
		for id, old := range existing {
			if _, ok := current[id]; !ok && !old.deleted {
				removed = append(removed, id)
			}
		}
		sort.Strings(removed)
		for _, id := range removed {
			head++
			st.Removed++
			if _, err := tx.ExecContext(ctx, `
				UPDATE local_catalog SET deleted = 1, rev = ?, updated_at = ? WHERE global_id = ?
			`, head, now.Unix(), id); err != nil {
				return fmt.Errorf("tombstoning %s: %w", id, err)
			}
		}

		if st.Purged, err = gcTombstones(ctx, tx, now); err != nil {
			return err
		}
		return setSetting(ctx, tx, headKey, strconv.FormatInt(head, 10))
	})
	if err != nil {
		return RefreshStats{}, err
	}
	st.Head = head
	st.Duration = time.Since(started)
	slog.Debug("local catalog index refreshed",
		"items", st.Total, "added", st.Added, "updated", st.Updated, "removed", st.Removed,
		"tombstones_purged", st.Purged, "head", st.Head,
		"jellyfin_listing", st.ListDuration.Round(time.Millisecond), "duration", st.Duration.Round(time.Millisecond))
	return st, nil
}

// gcTombstones drops removals older than tombstoneTTL and raises the
// feed's min rev past them, so a peer whose cursor predates what was
// dropped is told to resync rather than silently missing a removal.
func gcTombstones(ctx context.Context, tx *sql.Tx, now time.Time) (int, error) {
	cutoff := now.Add(-tombstoneTTL).Unix()
	var maxRev sql.NullInt64
	if err := tx.QueryRowContext(ctx, `SELECT MAX(rev) FROM local_catalog WHERE deleted = 1 AND updated_at < ?`, cutoff).Scan(&maxRev); err != nil {
		return 0, err
	}
	if !maxRev.Valid {
		return 0, nil
	}
	res, err := tx.ExecContext(ctx, `DELETE FROM local_catalog WHERE deleted = 1 AND rev <= ?`, maxRev.Int64)
	if err != nil {
		return 0, err
	}
	purged, _ := res.RowsAffected()
	return int(purged), setSetting(ctx, tx, minRevKey, strconv.FormatInt(maxRev.Int64, 10))
}

// Change is one entry of the change feed: either the item's current wire
// entry, or a tombstone (Deleted, Entry nil) for an item no longer offered.
type Change struct {
	Rev      int64  `json:"rev"`
	GlobalID string `json:"global_id"`
	Deleted  bool   `json:"deleted,omitempty"`
	Entry    *Entry `json:"entry,omitempty"`
}

// Feed is one page of GET /api/v1/catalog/changes.
type Feed struct {
	// Epoch identifies the history Rev values belong to. A client whose
	// stored epoch differs must discard what it mirrored and resync.
	Epoch string `json:"epoch"`
	// Head is the newest rev at the time of this page.
	Head int64 `json:"head"`
	// Next is the cursor to pass as `since` for the following page (or the
	// next sync, once More is false).
	Next int64 `json:"next"`
	More bool  `json:"more"`
	// Reset means the requested cursor was unusable (unknown epoch, older
	// than the oldest retained tombstone, or ahead of Head) and this page
	// restarts the feed from the beginning.
	Reset   bool     `json:"reset,omitempty"`
	Changes []Change `json:"changes"`

	// resetReason says why Reset was set, for logging on the serving side.
	resetReason string
}

// Changes returns up to limit changes after rev since, oldest first. All
// reads happen in one transaction so the page and Head are consistent with
// each other even while a Refresh commits concurrently.
func (ix *Index) Changes(ctx context.Context, epoch string, since int64, limit int) (Feed, error) {
	var feed Feed
	err := withTx(ctx, ix.db, func(tx *sql.Tx) error {
		var err error
		if feed.Epoch, _, err = getSetting(ctx, tx, epochKey); err != nil {
			return err
		}
		if feed.Head, err = getIntSetting(ctx, tx, headKey); err != nil {
			return err
		}
		minRev, err := getIntSetting(ctx, tx, minRevKey)
		if err != nil {
			return err
		}
		switch {
		case epoch == "":
			feed.resetReason = "first sync"
		case epoch != feed.Epoch:
			feed.resetReason = "epoch changed"
		case since < 0 || since > feed.Head:
			feed.resetReason = "cursor out of range"
		case since > 0 && since < minRev:
			feed.resetReason = "cursor older than retained tombstones"
		}
		if feed.resetReason != "" {
			feed.Reset = true
			since = 0
		}

		// From scratch, tombstones are pointless: the client has nothing
		// to delete yet (or sweeps what it had once the resync completes).
		query := `SELECT rev, global_id, deleted, entry FROM local_catalog WHERE rev > ? ORDER BY rev LIMIT ?`
		if since == 0 {
			query = `SELECT rev, global_id, deleted, entry FROM local_catalog WHERE rev > ? AND deleted = 0 ORDER BY rev LIMIT ?`
		}
		rows, err := tx.QueryContext(ctx, query, since, limit+1)
		if err != nil {
			return err
		}
		defer rows.Close()
		feed.Changes = make([]Change, 0, limit)
		for rows.Next() {
			var c Change
			var raw string
			if err := rows.Scan(&c.Rev, &c.GlobalID, &c.Deleted, &raw); err != nil {
				return err
			}
			if len(feed.Changes) == limit {
				feed.More = true
				break
			}
			if !c.Deleted {
				c.Entry = new(Entry)
				if err := json.Unmarshal([]byte(raw), c.Entry); err != nil {
					return fmt.Errorf("decoding indexed entry %s: %w", c.GlobalID, err)
				}
			}
			feed.Changes = append(feed.Changes, c)
		}
		return rows.Err()
	})
	if err != nil {
		return Feed{}, err
	}
	feed.Next = feed.Head
	if feed.More {
		feed.Next = feed.Changes[len(feed.Changes)-1].Rev
	}
	return feed, nil
}

// Entries returns every item currently in the index, for the legacy
// full-catalog endpoint.
func (ix *Index) Entries(ctx context.Context) ([]Entry, error) {
	rows, err := ix.db.QueryContext(ctx, `SELECT entry FROM local_catalog WHERE deleted = 0 ORDER BY global_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	entries := make([]Entry, 0)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var e Entry
		if err := json.Unmarshal([]byte(raw), &e); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

func contentHash(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:16])
}

type queryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func getSetting(ctx context.Context, q queryer, key string) (string, bool, error) {
	var v string
	err := q.QueryRowContext(ctx, `SELECT value FROM settings WHERE key = ?`, key).Scan(&v)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("reading setting %s: %w", key, err)
	}
	return v, true, nil
}

func getIntSetting(ctx context.Context, q queryer, key string) (int64, error) {
	v, ok, err := getSetting(ctx, q, key)
	if err != nil || !ok {
		return 0, err
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing setting %s=%q: %w", key, v, err)
	}
	return n, nil
}

func setSetting(ctx context.Context, tx *sql.Tx, key, value string) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO settings (key, value) VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value
	`, key, value)
	return err
}

// withTx runs fn in a transaction, committing iff it returns nil. Batching
// a sync's writes this way turns thousands of individually-fsynced
// autocommit statements into one commit.
func withTx(ctx context.Context, db *sql.DB, fn func(*sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}
