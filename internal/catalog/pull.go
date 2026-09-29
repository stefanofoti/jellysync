package catalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"jellysync/internal/peers"
)

// pullPageSize is how many changes are requested per page. Each page is
// its own short request (bounded by the peer fetch timeout) and its own
// transaction, so a large initial sync is resumable instead of one giant
// all-or-nothing call.
var pullPageSize = 2000

// pullConcurrency bounds how many peers are pulled from at once.
const pullConcurrency = 4

// mirrorMu serializes writes to peer_catalog/peer_sync_state across the
// concurrent per-peer pulls: SQLite has a single writer anyway, and taking
// turns here avoids transactions contending for the write lock.
var mirrorMu sync.Mutex

// errFeedUnsupported means the peer predates the change feed.
var errFeedUnsupported = errors.New("peer has no change feed")

type cursor struct {
	epoch  string
	rev    int64
	gen    int64
	resync bool
}

// pullPeers brings every given peer's mirror up to date, concurrently. A
// failing peer is logged and skipped: its mirror just stays as of its last
// successful pull.
func pullPeers(ctx context.Context, db *sql.DB, list []peers.Peer, timeout time.Duration, logf func(string, ...any)) {
	sem := make(chan struct{}, pullConcurrency)
	var wg sync.WaitGroup
	for _, p := range list {
		wg.Add(1)
		sem <- struct{}{}
		go func(p peers.Peer) {
			defer wg.Done()
			defer func() { <-sem }()
			started := time.Now()
			n, err := pullPeer(ctx, db, p, timeout)
			if err != nil {
				logf("catalog sync: peer %s: %v", p.ID, err)
				return
			}
			if n > 0 {
				logf("catalog sync: peer %s: applied %d change(s) in %s", p.ID, n, time.Since(started).Round(time.Millisecond))
			}
		}(p)
	}
	wg.Wait()
}

// pullPeer follows p's change feed from the stored cursor until caught up,
// committing each page together with the advanced cursor, so an interrupted
// pull resumes where it stopped. It returns the number of changes applied.
//
// When the feed resets (first contact, the peer's DB was recreated, or our
// cursor fell behind its tombstone retention), the existing mirror is not
// dropped up front — that would make the peer's items vanish (and their
// .strm files with them) until the resync finished. Instead the resync is
// written under a new generation and rows from older generations are swept
// only once the feed is fully caught up.
func pullPeer(ctx context.Context, db *sql.DB, p peers.Peer, timeout time.Duration) (int, error) {
	cur, err := loadCursor(ctx, db, p.ID)
	if err != nil {
		return 0, err
	}

	applied := 0
	for {
		pageCtx, cancel := context.WithTimeout(ctx, timeout)
		feed, err := fetchChanges(pageCtx, p.URL, cur.epoch, cur.rev, pullPageSize)
		cancel()
		if errors.Is(err, errFeedUnsupported) {
			return pullLegacy(ctx, db, p, cur, timeout)
		}
		if err != nil {
			return applied, err
		}

		if feed.Reset || feed.Epoch != cur.epoch {
			cur.epoch = feed.Epoch
			cur.gen++
			cur.resync = true
		}
		if feed.More && feed.Next <= cur.rev && !feed.Reset {
			return applied, fmt.Errorf("change feed did not advance past rev %d", cur.rev)
		}
		cur.rev = feed.Next
		sweep := !feed.More && cur.resync
		if sweep {
			cur.resync = false
		}

		if err := applyChanges(ctx, db, p.ID, cur, feed.Changes, sweep); err != nil {
			return applied, err
		}
		applied += len(feed.Changes)
		if !feed.More {
			return applied, nil
		}
	}
}

// pullLegacy fetches a pre-change-feed peer's whole catalog and replaces
// its mirror with it, reusing the generation sweep so the swap is atomic.
func pullLegacy(ctx context.Context, db *sql.DB, p peers.Peer, cur cursor, timeout time.Duration) (int, error) {
	fetchCtx, cancel := context.WithTimeout(ctx, timeout)
	entries, err := FetchPeerCatalog(fetchCtx, p.URL)
	cancel()
	if err != nil {
		return 0, err
	}
	changes := make([]Change, len(entries))
	for i := range entries {
		changes[i] = Change{GlobalID: entries[i].GlobalID, Entry: &entries[i]}
	}
	// An empty epoch guarantees a proper resync if the peer later upgrades.
	next := cursor{gen: cur.gen + 1}
	if err := applyChanges(ctx, db, p.ID, next, changes, true); err != nil {
		return 0, err
	}
	return len(changes), nil
}

func fetchChanges(ctx context.Context, peerURL, epoch string, since int64, limit int) (Feed, error) {
	q := url.Values{
		"epoch": {epoch},
		"since": {strconv.FormatInt(since, 10)},
		"limit": {strconv.Itoa(limit)},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, peerURL+"/api/v1/catalog/changes?"+q.Encode(), nil)
	if err != nil {
		return Feed{}, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return Feed{}, fmt.Errorf("fetching catalog changes from %s: %w", peerURL, err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound, http.StatusMethodNotAllowed:
		return Feed{}, errFeedUnsupported
	default:
		return Feed{}, fmt.Errorf("fetching catalog changes from %s: status %d", peerURL, resp.StatusCode)
	}
	var feed Feed
	if err := json.NewDecoder(resp.Body).Decode(&feed); err != nil {
		return Feed{}, fmt.Errorf("decoding catalog changes from %s: %w", peerURL, err)
	}
	return feed, nil
}

func loadCursor(ctx context.Context, db *sql.DB, peerID string) (cursor, error) {
	var c cursor
	err := db.QueryRowContext(ctx, `SELECT epoch, rev, gen, resync FROM peer_sync_state WHERE peer_id = ?`, peerID).
		Scan(&c.epoch, &c.rev, &c.gen, &c.resync)
	if err == sql.ErrNoRows {
		return cursor{}, nil
	}
	if err != nil {
		return cursor{}, fmt.Errorf("loading sync cursor for %s: %w", peerID, err)
	}
	return c, nil
}

// applyChanges writes one page of changes plus the cursor it advances to,
// atomically. With sweep set, it also drops every row not rewritten under
// the current generation, completing a resync.
func applyChanges(ctx context.Context, db *sql.DB, peerID string, cur cursor, changes []Change, sweep bool) error {
	mirrorMu.Lock()
	defer mirrorMu.Unlock()
	return withTx(ctx, db, func(tx *sql.Tx) error {
		// The cursor goes first so the transaction starts as a writer (see
		// Index.refreshLocked).
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO peer_sync_state (peer_id, epoch, rev, gen, resync, synced_at)
			VALUES (?, ?, ?, ?, ?, ?)
			ON CONFLICT(peer_id) DO UPDATE SET
				epoch = excluded.epoch, rev = excluded.rev, gen = excluded.gen,
				resync = excluded.resync, synced_at = excluded.synced_at
		`, peerID, cur.epoch, cur.rev, cur.gen, cur.resync, time.Now().Unix()); err != nil {
			return fmt.Errorf("saving sync cursor: %w", err)
		}
		for _, c := range changes {
			if c.Deleted || c.Entry == nil {
				if _, err := tx.ExecContext(ctx, `DELETE FROM peer_catalog WHERE peer_id = ? AND global_id = ?`, peerID, c.GlobalID); err != nil {
					return err
				}
				continue
			}
			raw, err := json.Marshal(c.Entry)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO peer_catalog (peer_id, global_id, entry, gen) VALUES (?, ?, ?, ?)
				ON CONFLICT(peer_id, global_id) DO UPDATE SET entry = excluded.entry, gen = excluded.gen
			`, peerID, c.Entry.GlobalID, string(raw), cur.gen); err != nil {
				return fmt.Errorf("mirroring %s: %w", c.GlobalID, err)
			}
		}
		if sweep {
			if _, err := tx.ExecContext(ctx, `DELETE FROM peer_catalog WHERE peer_id = ? AND gen < ?`, peerID, cur.gen); err != nil {
				return fmt.Errorf("sweeping stale mirror rows: %w", err)
			}
		}
		return nil
	})
}

// pruneRemovedPeers drops mirrors and cursors of peers no longer in the
// registry.
func pruneRemovedPeers(ctx context.Context, db *sql.DB, known []peers.Peer) error {
	ids := make([]any, 0, len(known))
	placeholders := ""
	for i, p := range known {
		if i > 0 {
			placeholders += ","
		}
		placeholders += "?"
		ids = append(ids, p.ID)
	}
	cond := "1"
	if len(ids) > 0 {
		cond = "peer_id NOT IN (" + placeholders + ")"
	}
	mirrorMu.Lock()
	defer mirrorMu.Unlock()
	return withTx(ctx, db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM peer_sync_state WHERE `+cond, ids...); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `DELETE FROM peer_catalog WHERE `+cond, ids...)
		return err
	})
}
