// Package catalog exchanges item metadata between nodes and decides, for
// each globally-identified item, whether it's served locally or by a
// remote peer.
package catalog

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"jellysync/internal/jellyfin"
	"jellysync/internal/logging"
	"jellysync/internal/peerauth"
	"jellysync/internal/peers"
)

// peerClient is the client for requests to registry's peers. It has no
// Timeout of its own: callers bound each request via the context they pass
// in instead, so the deadline stays configurable (see
// config.PeerFetchTimeout) rather than fixed here.
func peerClient(registry *peers.Registry) *http.Client {
	return &http.Client{Transport: registry.Transport()}
}

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
// excluding any item that's actually one of jellysync's own generated .strm
// redirects rather than real local media — Jellyfin can't tell the two
// apart in its /Items listing (both just look like a FileSystem-located
// item), so without this exclusion a node would "find" its own
// remote-elected item on the next sync, mark it local, and delete the very
// .strm it just wrote, in an endless loop.
//
// Two independent checks catch this, since either alone is fragile:
//   - path-prefix: the item's file lives under strmDir. This only matches
//     when Jellyfin and jellysync agree on that directory's absolute path,
//     which breaks silently if they mount the shared folder differently
//     (e.g. two separate containers with different bind-mount targets, or
//     a symlink one side resolves and the other doesn't) — the item then
//     slips through as if it were real local media.
//   - extension: every file jellysync writes ends in ".strm" (see
//     internal/strm), and real local media never does. This holds
//     regardless of how the two sides mount the directory, so it's the
//     more robust of the two — kept alongside the path check rather than
//     replacing it, in case a user's library legitimately contains
//     unrelated .strm files from another tool.
func LocalEntries(ctx context.Context, jf *jellyfin.Client, strmDir string) ([]Entry, error) {
	items, err := jf.ListItems(ctx)
	if err != nil {
		return nil, err
	}
	excluded := 0
	defer func() {
		slog.Debug("local catalog built from jellyfin", "jellyfin_items", len(items), "kept", len(items)-excluded, "excluded_strm", excluded)
	}()
	cleanStrmDir := filepath.Clean(strmDir) + string(filepath.Separator)

	isStrm := func(it jellyfin.CatalogItem) bool {
		return (strmDir != "" && strings.HasPrefix(filepath.Clean(it.Path)+string(filepath.Separator), cleanStrmDir)) ||
			strings.EqualFold(filepath.Ext(it.Path), ".strm")
	}

	// Series roots are folders, so neither check in isStrm catches them when
	// Jellyfin sees the output dir under a different mount path. A root whose
	// episodes are all jellysync's own redirects is one too: drop it, or it
	// would win the election as "local" and be re-offered to peers.
	seriesEps := make(map[string]int)  // series global ID -> episodes seen
	seriesReal := make(map[string]int) // ... of which are real local media
	for _, it := range items {
		if it.MediaType != "episode" {
			continue
		}
		seriesEps[it.SeriesGlobalID]++
		if !isStrm(it) {
			seriesReal[it.SeriesGlobalID]++
		}
	}

	entries := make([]Entry, 0, len(items))
	for _, it := range items {
		if isStrm(it) || (it.MediaType == "series" && seriesEps[it.GlobalID()] > 0 && seriesReal[it.GlobalID()] == 0) {
			excluded++
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

// Handler serves GET /api/v1/catalog, the legacy full-catalog endpoint still
// used by peers that predate the change feed. It's served from the local
// index rather than a live Jellyfin listing, so answering it no longer
// costs this node a full library scan per request.
func Handler(ix *Index) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		if err := ix.EnsureIndexed(r.Context()); err != nil {
			slog.Error("serving full catalog: indexing local library", "caller", caller(r), logging.Err(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		entries, err := ix.Entries(r.Context())
		if err != nil {
			slog.Error("serving full catalog", "caller", caller(r), logging.Err(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, r, catalogResponse{Items: entries})
		slog.Info("served full catalog (legacy endpoint, peer predates the change feed)",
			"caller", caller(r), "items", len(entries), "duration", time.Since(started).Round(time.Millisecond))
	}
}

// caller identifies who made a peer-facing request, for logs: the
// authenticated caller on the peer port, else the X-Jellysync-Peer header
// (the caller's NODE_ID) when sent, plus its address.
func caller(r *http.Request) string {
	if c, ok := peerauth.CallerFrom(r.Context()); ok {
		return c.ID() + "@" + r.RemoteAddr
	}
	if id := r.Header.Get(peerHeader); id != "" {
		return id + "@" + r.RemoteAddr
	}
	return r.RemoteAddr
}

// peerHeader carries the calling node's NODE_ID on peer-to-peer requests
// (same header internal/proxy uses).
const peerHeader = "X-Jellysync-Peer"

const (
	defaultChangesLimit = 1000
	maxChangesLimit     = 5000
)

// ChangesHandler serves GET /api/v1/catalog/changes?epoch=E&since=N&limit=L:
// one page of this node's catalog changes after rev N. A peer that's up to
// date gets an empty page back, so a routine sync with no library changes
// costs one tiny request instead of a full catalog transfer. See Feed.
func ChangesHandler(ix *Index) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		q := r.URL.Query()
		since, err := strconv.ParseInt(q.Get("since"), 10, 64)
		if err != nil && q.Get("since") != "" {
			slog.Warn("bad catalog changes request", "caller", caller(r), "since", q.Get("since"))
			http.Error(w, "invalid since", http.StatusBadRequest)
			return
		}
		limit := defaultChangesLimit
		if v := q.Get("limit"); v != "" {
			if limit, err = strconv.Atoi(v); err != nil || limit <= 0 {
				slog.Warn("bad catalog changes request", "caller", caller(r), "limit", v)
				http.Error(w, "invalid limit", http.StatusBadRequest)
				return
			}
			limit = min(limit, maxChangesLimit)
		}
		if err := ix.EnsureIndexed(r.Context()); err != nil {
			slog.Error("serving catalog changes: indexing local library", "caller", caller(r), logging.Err(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		feed, err := ix.Changes(r.Context(), q.Get("epoch"), since, limit)
		if err != nil {
			slog.Error("serving catalog changes", "caller", caller(r), logging.Err(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, r, feed)
		if feed.Reset {
			slog.Info("peer catalog cursor reset, serving from the beginning",
				"caller", caller(r), "reason", feed.resetReason, "their_since", since, "head", feed.Head)
		}
		slog.Debug("served catalog changes",
			"caller", caller(r), "since", since, "changes", len(feed.Changes), "next", feed.Next,
			"head", feed.Head, "more", feed.More, "duration", time.Since(started).Round(time.Millisecond))
	}
}

// NotifyHandler serves POST /api/v1/catalog/notify, a peer's hint that its
// catalog just changed. wake should schedule a pull-only sync (no local
// Jellyfin re-read) without blocking; repeated hints are expected to
// coalesce. The hint carries no data, so trusting it costs at most one
// cheap, empty change-feed request per peer.
func NotifyHandler(wake func()) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slog.Info("peer reported catalog changes, queuing a pull", "caller", caller(r))
		wake()
		w.WriteHeader(http.StatusAccepted)
	}
}

// notifyTimeout bounds each best-effort change notification to a peer.
const notifyTimeout = 5 * time.Second

// NotifyPeers tells every ONLINE peer that this node's catalog changed, so
// they pull the delta now instead of at their next scheduled sync.
// Best-effort and fire-and-forget: peers that miss it (or predate the
// endpoint) still catch up on their own schedule.
func NotifyPeers(registry *peers.Registry, selfID string) {
	client := peerClient(registry)
	for _, p := range registry.List() {
		if p.State != peers.StateOnline {
			slog.Debug("not notifying peer of catalog changes: not online", "peer", p.ID, "state", p.State)
			continue
		}
		go func(p peers.Peer) {
			ctx, cancel := context.WithTimeout(context.Background(), notifyTimeout)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.URL+"/api/v1/catalog/notify", nil)
			if err != nil {
				return
			}
			req.Header.Set(peerHeader, selfID)
			resp, err := client.Do(req)
			if err != nil {
				slog.Warn("notifying peer of catalog changes failed; it will catch up on its own schedule", "peer", p.ID, logging.Err(err))
				return
			}
			resp.Body.Close()
			switch resp.StatusCode {
			case http.StatusAccepted:
				slog.Debug("notified peer of catalog changes", "peer", p.ID)
			case http.StatusNotFound, http.StatusMethodNotAllowed:
				slog.Debug("peer predates change notifications; it will catch up on its own schedule", "peer", p.ID)
			case http.StatusForbidden:
				// One-way pairing: we're its client, it doesn't pull from us.
				slog.Debug("peer does not pull from this node; not notifying it", "peer", p.ID)
			default:
				slog.Warn("notifying peer of catalog changes: unexpected status", "peer", p.ID, "status", resp.StatusCode)
			}
		}(p)
	}
}

// writeJSON encodes v as the response, gzip-compressed when the client
// accepts it. Catalog JSON is highly repetitive and compresses ~10x; Go's
// HTTP client asks for and transparently decodes gzip on its own, so every
// jellysync peer, old or new, benefits without doing anything.
func writeJSON(w http.ResponseWriter, r *http.Request, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Add("Vary", "Accept-Encoding")
	if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		json.NewEncoder(w).Encode(v)
		return
	}
	w.Header().Set("Content-Encoding", "gzip")
	gz := gzip.NewWriter(w)
	defer gz.Close()
	json.NewEncoder(gz).Encode(v)
}

// FetchPeerCatalog fetches a peer node's whole catalog over the legacy
// GET /api/v1/catalog endpoint, for peers that predate the change feed.
func FetchPeerCatalog(ctx context.Context, client *http.Client, peerURL, selfID string) ([]Entry, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, peerURL+"/api/v1/catalog", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set(peerHeader, selfID)
	resp, err := client.Do(req)
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

// Sync brings catalog_items up to date:
//  1. if refreshLocal, re-reads the local Jellyfin library into the index
//     (skipped for a pull-only sync triggered by a peer's notification);
//  2. pulls every ONLINE peer's changes since the last sync into its local
//     mirror (peer_catalog), concurrently and page by page;
//  3. re-runs the election over the index and mirrors and writes only rows
//     that actually changed, in a single transaction.
//
// The election rule is unchanged:
//   - if an item exists locally, local always wins;
//   - otherwise the peer with the lexicographically smallest ID among those
//     offering the item is elected primary. This is a pure function of peer
//     IDs (no latency/timing involved), which avoids two nodes racing to a
//     different answer for the same item.
//
// Hidden items (see hidden.go) stay in catalog_items with hidden set: a
// hidden local item still wins the election (so no peer's copy replaces
// it), a hidden remote item keeps its elected peer but Reconcile gives it
// no .strm file. Hides of items nothing offers any more are dropped.
//
// Only ONLINE peers' mirrors take part in the election. A peer whose pull
// failed this cycle but is still ONLINE keeps offering what it last
// reported, rather than having all its items withdrawn over one slow
// request.
//
// The returned stats (partial on error) say what each step did; the
// caller notifies peers when Local.Changed() > 0.
func Sync(ctx context.Context, db *sql.DB, ix *Index, registry *peers.Registry, refreshLocal bool, peerFetchTimeout time.Duration, selfID string) (SyncStats, error) {
	var st SyncStats
	if refreshLocal {
		local, err := ix.Refresh(ctx)
		if err != nil {
			return st, fmt.Errorf("local catalog: %w", err)
		}
		st.Local, st.LocalRefreshed = local, true
	} else {
		slog.Debug("skipping local jellyfin re-read (pull-only sync)")
		if err := ix.EnsureIndexed(ctx); err != nil {
			return st, fmt.Errorf("local catalog: %w", err)
		}
	}

	all := registry.List()
	online := make([]peers.Peer, 0, len(all))
	onlineIDs := make(map[string]bool, len(all))
	for _, p := range all {
		if p.State == peers.StateOnline {
			online = append(online, p)
			onlineIDs[p.ID] = true
		} else {
			slog.Info("skipping peer this cycle: not online; its items are withdrawn until it is", "peer", p.ID, "state", p.State)
		}
	}
	st.PeersSkipped = len(all) - len(online)
	st.Peers = pullPeers(ctx, db, peerClient(registry), online, peerFetchTimeout, selfID)
	if err := pruneRemovedPeers(ctx, db, all); err != nil {
		return st, fmt.Errorf("pruning removed peers: %w", err)
	}

	started := time.Now()
	desired, offered, err := elect(ctx, db, onlineIDs)
	if err != nil {
		return st, fmt.Errorf("electing: %w", err)
	}
	st.Election, err = writeElection(ctx, db, desired, offered)
	if err != nil {
		return st, err
	}
	st.Election.Duration = time.Since(started)
	slog.Debug("election written",
		"local_items", st.Election.Local, "remote_items", st.Election.Remote,
		"upserted", st.Election.Upserted, "stale_local_deleted", st.Election.StaleLocalDeleted,
		"orphans_cleared", st.Election.OrphansCleared, "inert_deleted", st.Election.InertDeleted,
		"hidden", st.Election.Hidden, "hides_dropped", st.Election.HidesDropped,
		"duration", st.Election.Duration.Round(time.Millisecond))
	return st, nil
}

// SyncStats summarizes one Sync.
type SyncStats struct {
	Local          RefreshStats // zero unless LocalRefreshed
	LocalRefreshed bool
	Peers          []PeerPullStats // one per ONLINE peer
	PeersSkipped   int             // peers not ONLINE
	Election       ElectionStats
}

// ElectionStats describes what writeElection changed in catalog_items.
type ElectionStats struct {
	Local, Remote     int // desired rows by owner
	Upserted          int // rows inserted or changed
	StaleLocalDeleted int
	OrphansCleared    int // remote rows no peer offers any more
	InertDeleted      int
	Hidden            int // desired rows the user hid
	HidesDropped      int // hides of items nothing offers any more
	Duration          time.Duration
}

// itemState is the election-relevant content of one catalog_items row.
type itemState struct {
	name, mediaType             string
	local                       bool
	localItemID                 string // "" stored as NULL
	peerID, peerItemID          string // "" stored as NULL
	seriesGlobalID, seriesName  string
	seasonNumber, episodeNumber int
	hidden                      bool
}

func stateFromEntry(e Entry) itemState {
	return itemState{
		name: e.Name, mediaType: e.MediaType,
		seriesGlobalID: e.SeriesGlobalID, seriesName: e.SeriesName,
		seasonNumber: e.SeasonNumber, episodeNumber: e.EpisodeNumber,
	}
}

// elect computes the desired catalog_items content from the local index and
// the mirrors of the given peers. offered is the hide key (see hideKey) of
// every item any source still has, ONLINE or not, for pruneHidden.
func elect(ctx context.Context, db *sql.DB, onlinePeers map[string]bool) (desired map[string]itemState, offered map[string]bool, err error) {
	desired = make(map[string]itemState)
	offered = make(map[string]bool)

	hidden, err := hiddenSet(ctx, db)
	if err != nil {
		return nil, nil, err
	}

	// A local row's hidden flag is the one Refresh recorded, i.e. whether
	// peers were actually told it's gone, not the live hidden_items.
	rows, err := db.QueryContext(ctx, `SELECT entry, hidden FROM local_catalog WHERE deleted = 0`)
	if err != nil {
		return nil, nil, err
	}
	for rows.Next() {
		var raw string
		var e Entry
		var isHid bool
		err := rows.Scan(&raw, &isHid)
		if err == nil {
			err = json.Unmarshal([]byte(raw), &e)
		}
		if err != nil {
			rows.Close()
			return nil, nil, err
		}
		offered[hideKey(e)] = true
		st := stateFromEntry(e)
		st.local = true
		st.localItemID = e.ItemID
		st.hidden = isHid
		desired[e.GlobalID] = st
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	// Ordered by peer_id, so the first offer seen for an item comes from
	// the lexicographically smallest peer: that's the elected primary.
	rows, err = db.QueryContext(ctx, `SELECT peer_id, global_id, entry FROM peer_catalog ORDER BY global_id, peer_id`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var peerID, globalID, raw string
		if err := rows.Scan(&peerID, &globalID, &raw); err != nil {
			return nil, nil, err
		}
		var e Entry
		if err := json.Unmarshal([]byte(raw), &e); err != nil {
			return nil, nil, err
		}
		offered[hideKey(e)] = true
		if !onlinePeers[peerID] {
			continue
		}
		if _, taken := desired[globalID]; taken {
			continue // local always wins, then the first (smallest) peer
		}
		st := stateFromEntry(e)
		st.peerID = peerID
		st.peerItemID = e.ItemID
		st.hidden = isHidden(hidden, e)
		desired[globalID] = st
	}
	return desired, offered, rows.Err()
}

// writeElection diffs desired against catalog_items and applies only the
// differences, in one transaction:
//   - new or changed items are upserted (strm_path is left alone; Reconcile
//     owns it);
//   - rows still marked local whose global_id is no longer produced are
//     deleted outright. This happens whenever the fallback title/season/
//     episode hash an item resolves to changes (e.g. a join that
//     momentarily failed to attach series/season/episode data), and local
//     items never have a strm_path to clean up;
//   - remote rows no current peer offers (typically: the peer went offline
//     or was removed) have their election cleared rather than being deleted,
//     so Reconcile (which runs right after, reading this same table) sees
//     peer_id="" and removes the orphaned .strm this same cycle;
//   - rows that are fully inert (not local, no election, no .strm left to
//     clean up — Reconcile handled them in a prior cycle) are deleted;
//   - hides of items no source offers any more are dropped.
func writeElection(ctx context.Context, db *sql.DB, desired map[string]itemState, offered map[string]bool) (ElectionStats, error) {
	var st ElectionStats
	hidden, err := hiddenSet(ctx, db)
	if err != nil {
		return st, err
	}
	for _, d := range desired {
		if d.hidden {
			st.Hidden++
		}
		if d.local {
			st.Local++
		} else {
			st.Remote++
		}
	}
	existing := make(map[string]itemState)
	rows, err := db.QueryContext(ctx, `
		SELECT global_id, name, media_type, local, local_item_id, primary_peer_id, primary_item_id,
		       series_global_id, series_name, season_number, episode_number, hidden
		FROM catalog_items
	`)
	if err != nil {
		return st, fmt.Errorf("reading catalog items: %w", err)
	}
	for rows.Next() {
		var id string
		var row itemState
		var localItemID, peerID, peerItemID sql.NullString
		if err := rows.Scan(&id, &row.name, &row.mediaType, &row.local, &localItemID, &peerID, &peerItemID,
			&row.seriesGlobalID, &row.seriesName, &row.seasonNumber, &row.episodeNumber, &row.hidden); err != nil {
			rows.Close()
			return st, fmt.Errorf("reading catalog items: %w", err)
		}
		row.localItemID, row.peerID, row.peerItemID = localItemID.String, peerID.String, peerItemID.String
		existing[id] = row
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return st, fmt.Errorf("reading catalog items: %w", err)
	}

	now := time.Now().Unix()
	return st, withTx(ctx, db, func(tx *sql.Tx) error {
		for id, d := range desired {
			if old, ok := existing[id]; ok && old == d {
				continue
			}
			st.Upserted++
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO catalog_items (global_id, name, media_type, local, local_item_id, primary_peer_id, primary_item_id, series_global_id, series_name, season_number, episode_number, hidden, updated_at)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				ON CONFLICT(global_id) DO UPDATE SET
					name             = excluded.name,
					media_type       = excluded.media_type,
					local            = excluded.local,
					local_item_id    = excluded.local_item_id,
					primary_peer_id  = excluded.primary_peer_id,
					primary_item_id  = excluded.primary_item_id,
					series_global_id = excluded.series_global_id,
					series_name      = excluded.series_name,
					season_number    = excluded.season_number,
					episode_number   = excluded.episode_number,
					hidden           = excluded.hidden,
					updated_at       = excluded.updated_at
			`, id, d.name, d.mediaType, d.local, nullIfEmpty(d.localItemID), nullIfEmpty(d.peerID), nullIfEmpty(d.peerItemID),
				d.seriesGlobalID, d.seriesName, d.seasonNumber, d.episodeNumber, d.hidden, now); err != nil {
				return fmt.Errorf("upserting catalog item %s: %w", id, err)
			}
		}

		for id, old := range existing {
			if _, ok := desired[id]; ok {
				continue
			}
			switch {
			case old.local:
				if _, err := tx.ExecContext(ctx, `DELETE FROM catalog_items WHERE global_id = ?`, id); err != nil {
					return fmt.Errorf("deleting stale local catalog item %s: %w", id, err)
				}
				st.StaleLocalDeleted++
			case old.peerID != "":
				if _, err := tx.ExecContext(ctx, `UPDATE catalog_items SET primary_peer_id = NULL, primary_item_id = NULL WHERE global_id = ?`, id); err != nil {
					return fmt.Errorf("clearing orphaned catalog item %s: %w", id, err)
				}
				st.OrphansCleared++
			}
		}

		res, err := tx.ExecContext(ctx, `
			DELETE FROM catalog_items
			WHERE local = 0 AND primary_peer_id IS NULL AND strm_path IS NULL
		`)
		if err != nil {
			return fmt.Errorf("deleting inert catalog items: %w", err)
		}
		inert, _ := res.RowsAffected()
		st.InertDeleted = int(inert)

		st.HidesDropped, err = pruneHidden(ctx, tx, hidden, offered)
		return err
	})
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
