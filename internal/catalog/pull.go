package catalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"jellysync/internal/logging"
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

// PeerPullStats describes one peer's pull.
type PeerPullStats struct {
	Peer     string
	Upserts  int // items added or changed in the mirror
	Deletes  int // items the peer no longer offers
	Swept    int // stale rows dropped at the end of a resync
	Pages    int
	Resync   bool // the feed was reset and the mirror rebuilt
	Legacy   bool // peer predates the change feed; full catalog fetched
	Head     int64
	Duration time.Duration
	Err      error
}

// Changes is how many mirror rows this pull added, changed or removed.
func (s PeerPullStats) Changes() int { return s.Upserts + s.Deletes + s.Swept }

// pullPeers brings every given peer's mirror up to date, concurrently. A
// failing peer is logged and skipped: its mirror just stays as of its last
// successful pull.
func pullPeers(ctx context.Context, db *sql.DB, list []peers.Peer, timeout time.Duration, selfID string) []PeerPullStats {
	out := make([]PeerPullStats, len(list))
	sem := make(chan struct{}, pullConcurrency)
	var wg sync.WaitGroup
	for i, p := range list {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, p peers.Peer) {
			defer wg.Done()
			defer func() { <-sem }()
			started := time.Now()
			st := PeerPullStats{Peer: p.ID}
			st.Err = pullPeer(ctx, db, p, timeout, selfID, &st)
			st.Duration = time.Since(started)
			out[i] = st
			if st.Err != nil {
				slog.Warn("peer pull failed; keeping its last known catalog",
					"peer", p.ID, "url", p.URL, "applied_before_failure", st.Changes(), "pages", st.Pages,
					"duration", st.Duration.Round(time.Millisecond), logging.Err(st.Err))
				return
			}
			slog.Info("peer pull finished",
				"peer", p.ID, "added_or_changed", st.Upserts, "removed", st.Deletes, "swept", st.Swept,
				"pages", st.Pages, "resync", st.Resync, "legacy", st.Legacy, "head", st.Head,
				"duration", st.Duration.Round(time.Millisecond))
		}(i, p)
	}
	wg.Wait()
	return out
}

// pullPeer follows p's change feed from the stored cursor until caught up,
// committing each page together with the advanced cursor, so an interrupted
// pull resumes where it stopped. Progress is accumulated into st.
//
// When the feed resets (first contact, the peer's DB was recreated, or our
// cursor fell behind its tombstone retention), the existing mirror is not
// dropped up front — that would make the peer's items vanish (and their
// .strm files with them) until the resync finished. Instead the resync is
// written under a new generation and rows from older generations are swept
// only once the feed is fully caught up.
func pullPeer(ctx context.Context, db *sql.DB, p peers.Peer, timeout time.Duration, selfID string, st *PeerPullStats) error {
	cur, err := loadCursor(ctx, db, p.ID)
	if err != nil {
		return err
	}
	slog.Debug("peer pull started", "peer", p.ID, "url", p.URL, "epoch", cur.epoch, "since", cur.rev, "resync_in_progress", cur.resync)

	for {
		pageStarted := time.Now()
		pageCtx, cancel := context.WithTimeout(ctx, timeout)
		feed, err := fetchChanges(pageCtx, p.URL, selfID, cur.epoch, cur.rev, pullPageSize)
		cancel()
		if errors.Is(err, errFeedUnsupported) {
			slog.Info("peer predates the change feed; fetching its full catalog (upgrade it for incremental sync)", "peer", p.ID)
			return pullLegacy(ctx, db, p, cur, timeout, selfID, st)
		}
		if err != nil {
			return err
		}
		st.Pages++
		st.Head = feed.Head

		if feed.Reset || feed.Epoch != cur.epoch {
			reason := "peer catalog history changed (e.g. its database was recreated)"
			if cur.epoch == "" {
				reason = "first sync with this peer"
			}
			slog.Info("resyncing peer catalog from scratch", "peer", p.ID, "reason", reason, "our_since", cur.rev, "peer_head", feed.Head)
			cur.epoch = feed.Epoch
			cur.gen++
			cur.resync = true
			st.Resync = true
		}
		if feed.More && feed.Next <= cur.rev && !feed.Reset {
			return fmt.Errorf("change feed did not advance past rev %d", cur.rev)
		}
		cur.rev = feed.Next
		sweep := !feed.More && cur.resync
		if sweep {
			cur.resync = false
		}

		ups, dels, swept, err := applyChanges(ctx, db, p.ID, cur, feed.Changes, sweep)
		if err != nil {
			return err
		}
		st.Upserts += ups
		st.Deletes += dels
		st.Swept += swept
		slog.Debug("peer catalog page applied",
			"peer", p.ID, "page", st.Pages, "changes", len(feed.Changes), "added_or_changed", ups, "removed", dels,
			"next", feed.Next, "head", feed.Head, "more", feed.More, "duration", time.Since(pageStarted).Round(time.Millisecond))
		if swept > 0 {
			slog.Debug("resync complete, dropped items the peer no longer offers", "peer", p.ID, "swept", swept)
		}
		if !feed.More {
			return nil
		}
	}
}

// pullLegacy fetches a pre-change-feed peer's whole catalog and replaces
// its mirror with it, reusing the generation sweep so the swap is atomic.
func pullLegacy(ctx context.Context, db *sql.DB, p peers.Peer, cur cursor, timeout time.Duration, selfID string, st *PeerPullStats) error {
	st.Legacy = true
	fetchCtx, cancel := context.WithTimeout(ctx, timeout)
	entries, err := FetchPeerCatalog(fetchCtx, p.URL, selfID)
	cancel()
	if err != nil {
		return err
	}
	st.Pages++
	changes := make([]Change, len(entries))
	for i := range entries {
		changes[i] = Change{GlobalID: entries[i].GlobalID, Entry: &entries[i]}
	}
	// An empty epoch guarantees a proper resync if the peer later upgrades.
	next := cursor{gen: cur.gen + 1}
	ups, _, swept, err := applyChanges(ctx, db, p.ID, next, changes, true)
	if err != nil {
		return err
	}
	st.Upserts, st.Swept = ups, swept
	return nil
}

func fetchChanges(ctx context.Context, peerURL, selfID, epoch string, since int64, limit int) (Feed, error) {
	q := url.Values{
		"epoch": {epoch},
		"since": {strconv.FormatInt(since, 10)},
		"limit": {strconv.Itoa(limit)},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, peerURL+"/api/v1/catalog/changes?"+q.Encode(), nil)
	if err != nil {
		return Feed{}, err
	}
	req.Header.Set(peerHeader, selfID)
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
// the current generation, completing a resync. It returns how many rows
// were upserted, deleted and swept.
func applyChanges(ctx context.Context, db *sql.DB, peerID string, cur cursor, changes []Change, sweep bool) (upserts, deletes, swept int, err error) {
	mirrorMu.Lock()
	defer mirrorMu.Unlock()
	err = withTx(ctx, db, func(tx *sql.Tx) error {
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
				deletes++
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
			upserts++
		}
		if sweep {
			res, err := tx.ExecContext(ctx, `DELETE FROM peer_catalog WHERE peer_id = ? AND gen < ?`, peerID, cur.gen)
			if err != nil {
				return fmt.Errorf("sweeping stale mirror rows: %w", err)
			}
			n, _ := res.RowsAffected()
			swept = int(n)
		}
		return nil
	})
	if err != nil {
		return 0, 0, 0, err
	}
	return upserts, deletes, swept, nil
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
		res, err := tx.ExecContext(ctx, `DELETE FROM peer_catalog WHERE `+cond, ids...)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n > 0 {
			slog.Info("dropped catalog mirror of removed peer(s)", "items", n)
		}
		return nil
	})
}
