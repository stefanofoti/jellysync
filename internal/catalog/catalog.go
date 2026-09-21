// Package catalog exchanges item metadata between nodes and decides, for
// each globally-identified item, whether it's served locally or by a
// remote peer.
package catalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"jellysync/internal/jellyfin"
	"jellysync/internal/peers"
)

const syncInterval = 60 * time.Second
const fetchTimeout = 15 * time.Second

var httpClient = &http.Client{Timeout: fetchTimeout}

// Entry is the wire format exchanged over GET /api/v1/catalog.
type Entry struct {
	GlobalID  string `json:"global_id"`
	ItemID    string `json:"item_id"`
	Name      string `json:"name"`
	Year      int    `json:"year"`
	MediaType string `json:"media_type"`
	TmdbID    string `json:"tmdb_id,omitempty"`
	TvdbID    string `json:"tvdb_id,omitempty"`
	ImdbID    string `json:"imdb_id,omitempty"`

	// Episode-only grouping fields, carried across peers so a
	// remote-elected episode groups under its series the same way a local
	// one does. Zero-valued for movies and series roots.
	SeriesGlobalID string `json:"series_global_id,omitempty"`
	SeriesName     string `json:"series_name,omitempty"`
	SeasonNumber   int    `json:"season_number,omitempty"`
	EpisodeNumber  int    `json:"episode_number,omitempty"`
}

type catalogResponse struct {
	Items []Entry `json:"items"`
}

// LocalEntries builds the wire-format catalog for this node's own library,
// excluding any item whose file lives under strmDir. Those are jellysync's
// own generated .strm redirects, not real local media — Jellyfin can't
// tell the two apart in its /Items listing (both just look like a
// FileSystem-located item), so without this exclusion a node would "find"
// its own remote-elected item on the next sync, mark it local, and delete
// the very .strm it just wrote, in an endless loop.
func LocalEntries(ctx context.Context, jf *jellyfin.Client, strmDir string) ([]Entry, error) {
	items, err := jf.ListItems(ctx)
	if err != nil {
		return nil, err
	}
	cleanStrmDir := filepath.Clean(strmDir) + string(filepath.Separator)

	entries := make([]Entry, 0, len(items))
	for _, it := range items {
		if strmDir != "" && strings.HasPrefix(filepath.Clean(it.Path)+string(filepath.Separator), cleanStrmDir) {
			continue
		}
		entries = append(entries, Entry{
			GlobalID:       it.GlobalID(),
			ItemID:         it.ItemID,
			Name:           it.Name,
			Year:           it.Year,
			MediaType:      it.MediaType,
			TmdbID:         it.TmdbID,
			TvdbID:         it.TvdbID,
			ImdbID:         it.ImdbID,
			SeriesGlobalID: it.SeriesGlobalID,
			SeriesName:     it.SeriesName,
			SeasonNumber:   it.SeasonNumber,
			EpisodeNumber:  it.EpisodeNumber,
		})
	}
	return entries, nil
}

// Handler serves GET /api/v1/catalog with this node's local catalog.
func Handler(jf *jellyfin.Client, strmDir string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()

		entries, err := LocalEntries(ctx, jf, strmDir)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(catalogResponse{Items: entries})
	}
}

// FetchPeerCatalog fetches a peer node's catalog over HTTP.
func FetchPeerCatalog(ctx context.Context, peerURL string) ([]Entry, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, peerURL+"/api/v1/catalog", nil)
	if err != nil {
		return nil, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching catalog from %s: %w", peerURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetching catalog from %s: status %d", peerURL, resp.StatusCode)
	}

	var parsed catalogResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decoding catalog from %s: %w", peerURL, err)
	}
	return parsed.Items, nil
}

// Sync fetches the local catalog and every ONLINE peer's catalog, applies
// the dedup rule, and persists the result to catalog_items:
//   - if an item exists locally, local always wins;
//   - otherwise the peer with the lexicographically smallest ID among those
//     offering the item is elected primary. This is a pure function of peer
//     IDs (no latency/timing involved), which avoids two nodes racing to a
//     different answer for the same item.
func Sync(ctx context.Context, db *sql.DB, jf *jellyfin.Client, registry *peers.Registry, strmDir string) error {
	local, err := LocalEntries(ctx, jf, strmDir)
	if err != nil {
		return fmt.Errorf("local catalog: %w", err)
	}
	localByGlobalID := make(map[string]Entry, len(local))
	for _, e := range local {
		localByGlobalID[e.GlobalID] = e
	}

	// global_id -> peer_id -> full offered entry. Keeping the whole Entry
	// (not just the item ID) means name/media_type below can be read from
	// whichever peer actually gets elected primary, instead of whichever
	// peer happened to be processed last in this (unordered) map iteration.
	remoteOffers := make(map[string]map[string]Entry)

	for _, p := range registry.List() {
		if p.State != peers.StateOnline {
			continue
		}
		entries, err := FetchPeerCatalog(ctx, p.URL)
		if err != nil {
			// One unreachable peer shouldn't abort the whole sync.
			log.Printf("catalog sync: peer %s: %v", p.ID, err)
			continue
		}
		for _, e := range entries {
			if remoteOffers[e.GlobalID] == nil {
				remoteOffers[e.GlobalID] = make(map[string]Entry)
			}
			remoteOffers[e.GlobalID][p.ID] = e
		}
	}

	now := time.Now().Unix()

	for globalID, entry := range localByGlobalID {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO catalog_items (global_id, name, media_type, local, local_item_id, primary_peer_id, primary_item_id, series_global_id, series_name, season_number, episode_number, updated_at)
			VALUES (?, ?, ?, 1, ?, NULL, NULL, ?, ?, ?, ?, ?)
			ON CONFLICT(global_id) DO UPDATE SET
				name             = excluded.name,
				media_type       = excluded.media_type,
				local            = 1,
				local_item_id    = excluded.local_item_id,
				primary_peer_id  = NULL,
				primary_item_id  = NULL,
				series_global_id = excluded.series_global_id,
				series_name      = excluded.series_name,
				season_number    = excluded.season_number,
				episode_number   = excluded.episode_number,
				updated_at       = excluded.updated_at
		`, globalID, entry.Name, entry.MediaType, entry.ItemID,
			entry.SeriesGlobalID, entry.SeriesName, entry.SeasonNumber, entry.EpisodeNumber, now); err != nil {
			return fmt.Errorf("upserting local catalog item %s: %w", globalID, err)
		}
	}

	for globalID, offers := range remoteOffers {
		if _, isLocal := localByGlobalID[globalID]; isLocal {
			continue // rule 1: local always wins
		}
		primaryPeerID := electPrimary(offers)
		primary := offers[primaryPeerID]

		// No "WHERE local = 0" guard here: the isLocal check above already
		// guarantees, from this cycle's fresh data, that the item isn't
		// local right now — so it's always correct to force local=0. A
		// guard keyed on the row's *previous* local value would block
		// exactly the local-to-remote transition (item removed locally,
		// still offered by a peer) that this branch exists to handle.
		if _, err := db.ExecContext(ctx, `
			INSERT INTO catalog_items (global_id, name, media_type, local, local_item_id, primary_peer_id, primary_item_id, series_global_id, series_name, season_number, episode_number, updated_at)
			VALUES (?, ?, ?, 0, NULL, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(global_id) DO UPDATE SET
				name             = excluded.name,
				media_type       = excluded.media_type,
				local            = 0,
				local_item_id    = NULL,
				primary_peer_id  = excluded.primary_peer_id,
				primary_item_id  = excluded.primary_item_id,
				series_global_id = excluded.series_global_id,
				series_name      = excluded.series_name,
				season_number    = excluded.season_number,
				episode_number   = excluded.episode_number,
				updated_at       = excluded.updated_at
		`, globalID, primary.Name, primary.MediaType, primaryPeerID, primary.ItemID,
			primary.SeriesGlobalID, primary.SeriesName, primary.SeasonNumber, primary.EpisodeNumber, now); err != nil {
			return fmt.Errorf("upserting remote catalog item %s: %w", globalID, err)
		}
	}

	// Rows still marked local from a prior cycle whose global_id this
	// cycle's scan no longer produces. This happens whenever the
	// fallback title/season/episode hash an item resolves to changes
	// (e.g. a join that momentarily failed to attach series/season/
	// episode data) — the item gets upserted under a new global_id above,
	// and without this the old row would linger forever, still local=1,
	// still carrying whatever partial data it was first written with.
	// Local items never have a strm_path to clean up, so it's safe to
	// delete them outright rather than clear-and-reconcile like the
	// remote path below.
	if err := deleteStaleLocalItems(ctx, db, localByGlobalID); err != nil {
		return fmt.Errorf("deleting stale local catalog items: %w", err)
	}

	// Items that are neither local nor offered by any current peer
	// (typically: the peer that used to offer them was removed). Clear
	// the election rather than deleting the row outright, so Reconcile
	// (which runs right after Sync, reading this same table) sees
	// peer_id="" and removes the orphaned .strm this same cycle instead
	// of it lingering with a stale, now-unreachable primary_peer_id.
	if err := clearOrphanedElections(ctx, db, localByGlobalID, remoteOffers); err != nil {
		return fmt.Errorf("clearing orphaned catalog items: %w", err)
	}
	// Rows that are fully inert (not local, no election, no .strm left
	// to clean up — i.e. Reconcile already handled them in a prior
	// cycle) are safe to delete outright.
	if _, err := db.ExecContext(ctx, `
		DELETE FROM catalog_items
		WHERE local = 0 AND primary_peer_id IS NULL AND strm_path IS NULL
	`); err != nil {
		return fmt.Errorf("deleting inert catalog items: %w", err)
	}

	return nil
}

func deleteStaleLocalItems(ctx context.Context, db *sql.DB, local map[string]Entry) error {
	rows, err := db.QueryContext(ctx, `SELECT global_id FROM catalog_items WHERE local = 1`)
	if err != nil {
		return err
	}
	var stale []string
	for rows.Next() {
		var globalID string
		if err := rows.Scan(&globalID); err != nil {
			rows.Close()
			return err
		}
		if _, ok := local[globalID]; !ok {
			stale = append(stale, globalID)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, globalID := range stale {
		if _, err := db.ExecContext(ctx, `DELETE FROM catalog_items WHERE global_id = ?`, globalID); err != nil {
			return err
		}
	}
	return nil
}

func clearOrphanedElections(ctx context.Context, db *sql.DB, local map[string]Entry, remote map[string]map[string]Entry) error {
	rows, err := db.QueryContext(ctx, `SELECT global_id FROM catalog_items WHERE local = 0 AND primary_peer_id IS NOT NULL`)
	if err != nil {
		return err
	}
	var orphaned []string
	for rows.Next() {
		var globalID string
		if err := rows.Scan(&globalID); err != nil {
			rows.Close()
			return err
		}
		if _, isLocal := local[globalID]; isLocal {
			continue
		}
		if _, hasOffer := remote[globalID]; hasOffer {
			continue
		}
		orphaned = append(orphaned, globalID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, globalID := range orphaned {
		if _, err := db.ExecContext(ctx, `
			UPDATE catalog_items SET primary_peer_id = NULL, primary_item_id = NULL WHERE global_id = ?
		`, globalID); err != nil {
			return err
		}
	}
	return nil
}

func electPrimary(offers map[string]Entry) string {
	ids := make([]string, 0, len(offers))
	for id := range offers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids[0]
}

// SyncInterval is how often RunSyncLoop-style callers should re-sync.
const SyncInterval = syncInterval
