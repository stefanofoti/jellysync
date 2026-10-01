package catalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"jellysync/internal/logging"
)

// Hiding excludes an item from sync, in both directions: a hidden local
// item isn't offered to peers (the feed serves it as a tombstone), and a
// hidden remote item gets no .strm file. Hidden items stay listed on the
// dashboard so they can be unhidden. A hide is keyed by a movie's
// global_id or by a series key (see seriesKey), which covers the series
// root and all its episodes, including ones added later. Changes apply at
// the next sync, and a key is dropped once nothing offers the item any
// more (see pruneHidden), so an item that comes back later is visible again.

// seriesKey is an episode's series grouping key, the same one
// seriesKeyExpr computes in SQL: series_global_id when present, falling back
// to series_name.
func seriesKey(e Entry) string {
	if e.SeriesGlobalID != "" {
		return e.SeriesGlobalID
	}
	return e.SeriesName
}

// hideKey is the hidden_items key that hides e: its series key for an
// episode, else its own global_id (for a series root, that's the series key
// too).
func hideKey(e Entry) string {
	if e.MediaType == "episode" {
		return seriesKey(e)
	}
	return e.GlobalID
}

func isHidden(hidden map[string]bool, e Entry) bool {
	k := hideKey(e)
	return k != "" && hidden[k]
}

type rowsQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// hiddenSet reads every hidden_items key.
func hiddenSet(ctx context.Context, q rowsQueryer) (map[string]bool, error) {
	rows, err := q.QueryContext(ctx, `SELECT key FROM hidden_items`)
	if err != nil {
		return nil, fmt.Errorf("reading hidden items: %w", err)
	}
	defer rows.Close()
	hidden := make(map[string]bool)
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, fmt.Errorf("reading hidden items: %w", err)
		}
		hidden[k] = true
	}
	return hidden, rows.Err()
}

// pruneHidden drops every hidden key that no longer matches anything in
// offered: the item disappeared from its source (removed from the local
// library, or from every peer that had it), so the hide goes with it.
// offered must cover the mirrors of all known peers, not only ONLINE ones,
// or a peer going offline for a while would lose its hides.
func pruneHidden(ctx context.Context, tx *sql.Tx, hidden, offered map[string]bool) (int, error) {
	n := 0
	for k := range hidden {
		if offered[k] {
			continue
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM hidden_items WHERE key = ?`, k); err != nil {
			return n, fmt.Errorf("dropping hidden item %s: %w", k, err)
		}
		slog.Debug("dropped hide of an item no longer offered", "key", k)
		n++
	}
	return n, nil
}

var errNoSuchItem = errors.New("no movie or series with that key")

// SetHidden hides or unhides the movie or series identified by key: a
// movie's global_id, or a series key as listed by GET /api/v1/items?type=series.
// It only records the choice; the next sync applies it.
func SetHidden(ctx context.Context, db *sql.DB, key string, hidden bool) error {
	if !hidden {
		_, err := db.ExecContext(ctx, `DELETE FROM hidden_items WHERE key = ?`, key)
		return err
	}
	var found bool
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM catalog_items
			WHERE (media_type = 'movie' AND global_id = ?)
			   OR (media_type = 'episode' AND `+seriesKeyExpr+` = ?)
		)
	`, key, key).Scan(&found); err != nil {
		return err
	}
	if !found {
		return errNoSuchItem
	}
	_, err := db.ExecContext(ctx, `INSERT OR IGNORE INTO hidden_items (key, created_at) VALUES (?, ?)`, key, time.Now().Unix())
	return err
}

type hideRequest struct {
	Key    string `json:"key"`
	Hidden bool   `json:"hidden"`
}

// HideHandler serves PUT /api/v1/items/hidden with {"key": ..., "hidden":
// true|false}: hides or unhides a movie (key = global_id) or a whole series
// (key = series_key). Applied at the next sync.
func HideHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req hideRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Key == "" {
			http.Error(w, "body must be {\"key\": string, \"hidden\": bool}", http.StatusBadRequest)
			return
		}
		err := SetHidden(r.Context(), db, req.Key, req.Hidden)
		switch {
		case errors.Is(err, errNoSuchItem):
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		case err != nil:
			slog.Error("changing item visibility", "key", req.Key, logging.Err(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		slog.Info("item visibility changed, applied at next sync", "key", req.Key, "hidden", req.Hidden)
		w.WriteHeader(http.StatusNoContent)
	}
}
