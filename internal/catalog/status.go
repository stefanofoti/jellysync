package catalog

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

const (
	defaultItemsLimit = 50
	maxItemsLimit     = 200
)

const itemColumns = `global_id, name, media_type, local, local_item_id, primary_peer_id, primary_item_id, strm_path,
	       series_global_id, series_name, season_number, episode_number,
	       hidden, ` + hiddenWantedExpr

// hiddenWantedExpr is whether the user currently wants a catalog_items row
// hidden (an episode by its series), which the row's own hidden column only
// reflects after the next sync.
const hiddenWantedExpr = `EXISTS (SELECT 1 FROM hidden_items h WHERE h.key =
	CASE WHEN media_type = 'episode' THEN ` + seriesKeyExpr + ` ELSE global_id END)`

type itemDTO struct {
	GlobalID       string `json:"global_id"`
	Name           string `json:"name"`
	MediaType      string `json:"media_type"`
	Local          bool   `json:"local"`
	PrimaryPeerID  string `json:"primary_peer_id,omitempty"`
	StrmPath       string `json:"strm_path,omitempty"`
	SeriesGlobalID string `json:"series_global_id,omitempty"`
	SeriesName     string `json:"series_name,omitempty"`
	SeasonNumber   int    `json:"season_number,omitempty"`
	EpisodeNumber  int    `json:"episode_number,omitempty"`

	// Hidden is whether the user hid this item (an episode: its series)
	// from sync. HidePending means that choice isn't applied yet: it takes
	// effect at the next sync.
	Hidden      bool `json:"hidden"`
	HidePending bool `json:"hide_pending,omitempty"`

	// StreamPeerID/StreamItemID are the {peerID}/{itemID} pair to hit
	// GET /api/v1/proxy/stream/{peerID}/{itemID} for this item directly —
	// "local"+local_item_id when this node owns it, primary_peer_id+
	// primary_item_id otherwise. Empty for series-root rows, which have no
	// playable file of their own. Precomputed here so the dashboard doesn't
	// need to re-derive local-vs-remote routing itself.
	StreamPeerID string `json:"stream_peer_id,omitempty"`
	StreamItemID string `json:"stream_item_id,omitempty"`
}

// itemsPage is the envelope returned when /api/v1/items is called with a
// `type` filter: pagination is only meaningful once the flat catalog_items
// rows are grouped into the "list entries" the dashboard actually paginates
// over (one entry per movie, one per series — see below), so an unfiltered
// request keeps returning the plain array for backward compatibility.
type itemsPage struct {
	Items  []itemDTO `json:"items"`
	Total  int       `json:"total"`
	Limit  int       `json:"limit"`
	Offset int       `json:"offset"`
}

// seriesSummaryDTO is one list entry on the series tab: counts only, no
// episode rows. The dashboard fetches a series' actual episodes on demand
// (see SeriesEpisodesHandler) when its row is expanded, rather than having
// them embedded in the paginated list up front.
type seriesSummaryDTO struct {
	SeriesKey  string `json:"series_key"`
	SeriesName string `json:"series_name"`
	LocalCount int    `json:"local_count"`
	TotalCount int    `json:"total_count"`
	// Hidden/HidePending as on itemDTO, for the whole series.
	Hidden      bool `json:"hidden"`
	HidePending bool `json:"hide_pending,omitempty"`
}

type seriesSummaryPage struct {
	Items  []seriesSummaryDTO `json:"items"`
	Total  int                `json:"total"`
	Limit  int                `json:"limit"`
	Offset int                `json:"offset"`
}

// seriesKeyExpr groups catalog_items episode rows into series: by
// series_global_id when present (the cross-node-stable key), falling back to
// series_name for older rows / providerless series.
const seriesKeyExpr = `COALESCE(NULLIF(series_global_id, ''), series_name)`

func scanItemRows(rows *sql.Rows) ([]itemDTO, error) {
	items := make([]itemDTO, 0)
	for rows.Next() {
		var it itemDTO
		var localInt int
		var hiddenApplied bool
		var localItemID, primaryPeerID, primaryItemID, strmPath sql.NullString
		if err := rows.Scan(&it.GlobalID, &it.Name, &it.MediaType, &localInt, &localItemID, &primaryPeerID, &primaryItemID, &strmPath,
			&it.SeriesGlobalID, &it.SeriesName, &it.SeasonNumber, &it.EpisodeNumber, &hiddenApplied, &it.Hidden); err != nil {
			return nil, err
		}
		it.HidePending = it.Hidden != hiddenApplied
		it.Local = localInt != 0
		it.PrimaryPeerID = primaryPeerID.String
		it.StrmPath = strmPath.String

		if it.MediaType != "series" {
			if it.Local {
				it.StreamPeerID, it.StreamItemID = "local", localItemID.String
			} else if primaryPeerID.Valid && primaryItemID.Valid {
				it.StreamPeerID, it.StreamItemID = primaryPeerID.String, primaryItemID.String
			}
		}

		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func parsePageParams(r *http.Request) (limit, offset int, err error) {
	limit = defaultItemsLimit
	offset = 0
	if v := r.URL.Query().Get("limit"); v != "" {
		limit, err = strconv.Atoi(v)
		if err != nil || limit <= 0 {
			return 0, 0, errBadRequest("invalid limit")
		}
		if limit > maxItemsLimit {
			limit = maxItemsLimit
		}
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		offset, err = strconv.Atoi(v)
		if err != nil || offset < 0 {
			return 0, 0, errBadRequest("invalid offset")
		}
	}
	return limit, offset, nil
}

// ownerFilter turns the optional `owners` query param (comma-separated
// owner keys: "local" and/or peer ids, the same keys ownerOf uses on the
// dashboard) into a SQL condition plus its args. With no `owners` param
// every row passes; with `owners=` (present but empty) none do, so a caller
// that has unchecked every source gets an empty page rather than everything.
func ownerFilter(r *http.Request) (string, []any) {
	q := r.URL.Query()
	if !q.Has("owners") {
		return "1 = 1", nil
	}
	var includeLocal bool
	var peerIDs []any
	for _, o := range strings.Split(q.Get("owners"), ",") {
		switch o = strings.TrimSpace(o); o {
		case "":
		case "local":
			includeLocal = true
		default:
			peerIDs = append(peerIDs, o)
		}
	}
	conds := make([]string, 0, 2)
	if includeLocal {
		conds = append(conds, "local = 1")
	}
	if len(peerIDs) > 0 {
		conds = append(conds, "(local = 0 AND primary_peer_id IN (?"+strings.Repeat(", ?", len(peerIDs)-1)+"))")
	}
	if len(conds) == 0 {
		return "1 = 0", nil
	}
	return "(" + strings.Join(conds, " OR ") + ")", peerIDs
}

type errBadRequest string

func (e errBadRequest) Error() string { return string(e) }

// ItemsHandler serves GET /api/v1/items: a dashboard-facing view of
// catalog_items (distinct from the peer-facing GET /api/v1/catalog wire
// format, which only ever describes this node's own local library).
//
// With no `type` query param it returns the full flat array, unchanged from
// before pagination existed. With `type=movie` it paginates over movie rows
// (via `limit`/`offset`) and wraps the result in an itemsPage envelope
// carrying the total entry count, so a caller can render a pager without
// fetching the whole catalog. With `type=series` it paginates over distinct
// series instead, returning summary counts rather than episode rows — see
// servePagedSeries and SeriesEpisodesHandler for the per-series episode
// fetch this is meant to be paired with.
func ItemsHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("type") {
		case "movie":
			servePagedMovies(w, r, db)
		case "series":
			servePagedSeries(w, r, db)
		case "":
			serveAllItems(w, r, db)
		default:
			http.Error(w, "invalid type: must be movie or series", http.StatusBadRequest)
		}
	}
}

func serveAllItems(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	rows, err := db.QueryContext(r.Context(), `
		SELECT `+itemColumns+`
		FROM catalog_items
		ORDER BY name
	`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	items, err := scanItemRows(rows)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(items)
}

// servePagedMovies paginates directly over catalog_items rows: each movie
// is exactly one row, so a page of movies is just LIMIT/OFFSET by name.
// Filtered by `owners` (see ownerFilter) before paginating, so every page is
// full and the total matches the filter.
func servePagedMovies(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	limit, offset, err := parsePageParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ownerCond, ownerArgs := ownerFilter(r)

	var total int
	if err := db.QueryRowContext(r.Context(), `SELECT COUNT(*) FROM catalog_items WHERE media_type = 'movie' AND `+ownerCond, ownerArgs...).Scan(&total); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	rows, err := db.QueryContext(r.Context(), `
		SELECT `+itemColumns+`
		FROM catalog_items
		WHERE media_type = 'movie' AND `+ownerCond+`
		ORDER BY name
		LIMIT ? OFFSET ?
	`, append(ownerArgs, limit, offset)...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	items, err := scanItemRows(rows)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(itemsPage{Items: items, Total: total, Limit: limit, Offset: offset})
}

// servePagedSeries paginates over distinct series, not raw rows: a series
// is spread across one row per episode (grouped by series_global_id,
// falling back to series_name). Each page entry is a count-only summary —
// episode rows for a given series are fetched separately, on demand, via
// SeriesEpisodesHandler, once the caller actually needs them (e.g. the
// dashboard expanding that series' row) — so this response stays bounded by
// `limit` regardless of how many episodes those series have.
//
// `owners` (see ownerFilter) filters episodes before grouping: a series is
// listed iff at least one of its episodes comes from a selected source, and
// its counts cover only those episodes — matching what the dashboard shows
// once the row is expanded and its episodes are filtered the same way.
func servePagedSeries(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	limit, offset, err := parsePageParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ownerCond, ownerArgs := ownerFilter(r)

	var total int
	if err := db.QueryRowContext(r.Context(), `
		SELECT COUNT(*) FROM (
			SELECT 1 FROM catalog_items WHERE media_type = 'episode' AND `+ownerCond+` GROUP BY `+seriesKeyExpr+`
		)
	`, ownerArgs...).Scan(&total); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	rows, err := db.QueryContext(r.Context(), `
		SELECT `+seriesKeyExpr+` AS skey, MIN(series_name), SUM(local), COUNT(*),
		       MAX(hidden), EXISTS (SELECT 1 FROM hidden_items h WHERE h.key = `+seriesKeyExpr+`)
		FROM catalog_items
		WHERE media_type = 'episode' AND `+ownerCond+`
		GROUP BY skey
		ORDER BY MIN(series_name)
		LIMIT ? OFFSET ?
	`, append(ownerArgs, limit, offset)...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	items := make([]seriesSummaryDTO, 0)
	for rows.Next() {
		var it seriesSummaryDTO
		var hiddenApplied bool
		if err := rows.Scan(&it.SeriesKey, &it.SeriesName, &it.LocalCount, &it.TotalCount, &hiddenApplied, &it.Hidden); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		it.HidePending = it.Hidden != hiddenApplied
		items = append(items, it)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(seriesSummaryPage{Items: items, Total: total, Limit: limit, Offset: offset})
}

// SeriesEpisodesHandler serves GET /api/v1/items/series/episodes?key=<key>:
// every episode row belonging to one series, keyed the same way
// servePagedSeries groups its pages (COALESCE(series_global_id,
// series_name)). The dashboard calls this lazily when a series row is
// expanded, instead of the paginated series list embedding every episode of
// every series on the page up front.
func SeriesEpisodesHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "missing key", http.StatusBadRequest)
			return
		}

		rows, err := db.QueryContext(r.Context(), `
			SELECT `+itemColumns+`
			FROM catalog_items
			WHERE media_type = 'episode' AND `+seriesKeyExpr+` = ?
			ORDER BY season_number, episode_number
		`, key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		items, err := scanItemRows(rows)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(items)
	}
}
