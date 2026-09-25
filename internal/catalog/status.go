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
	       series_global_id, series_name, season_number, episode_number`

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

func scanItemRows(rows *sql.Rows) ([]itemDTO, error) {
	items := make([]itemDTO, 0)
	for rows.Next() {
		var it itemDTO
		var localInt int
		var localItemID, primaryPeerID, primaryItemID, strmPath sql.NullString
		if err := rows.Scan(&it.GlobalID, &it.Name, &it.MediaType, &localInt, &localItemID, &primaryPeerID, &primaryItemID, &strmPath,
			&it.SeriesGlobalID, &it.SeriesName, &it.SeasonNumber, &it.EpisodeNumber); err != nil {
			return nil, err
		}
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

type errBadRequest string

func (e errBadRequest) Error() string { return string(e) }

// ItemsHandler serves GET /api/v1/items: a dashboard-facing view of
// catalog_items (distinct from the peer-facing GET /api/v1/catalog wire
// format, which only ever describes this node's own local library).
//
// With no `type` query param it returns the full flat array, unchanged from
// before pagination existed. With `type=movie` or `type=series` it paginates
// over that tab's list entries (via `limit`/`offset`) and wraps the result
// in an itemsPage envelope carrying the total entry count, so a caller can
// render a pager without fetching the whole catalog.
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
func servePagedMovies(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	limit, offset, err := parsePageParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var total int
	if err := db.QueryRowContext(r.Context(), `SELECT COUNT(*) FROM catalog_items WHERE media_type = 'movie'`).Scan(&total); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	rows, err := db.QueryContext(r.Context(), `
		SELECT `+itemColumns+`
		FROM catalog_items
		WHERE media_type = 'movie'
		ORDER BY name
		LIMIT ? OFFSET ?
	`, limit, offset)
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
// falling back to series_name exactly like the dashboard's client-side
// grouping does), so a page of series has to be picked first and then
// expanded back out to every episode row belonging to it — otherwise a
// LIMIT/OFFSET on raw rows would cut a series in half across a page
// boundary.
func servePagedSeries(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	limit, offset, err := parsePageParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	const seriesKey = `COALESCE(NULLIF(series_global_id, ''), series_name)`

	var total int
	if err := db.QueryRowContext(r.Context(), `
		SELECT COUNT(*) FROM (
			SELECT 1 FROM catalog_items WHERE media_type = 'episode' GROUP BY `+seriesKey+`
		)
	`).Scan(&total); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	keyRows, err := db.QueryContext(r.Context(), `
		SELECT `+seriesKey+` AS skey
		FROM catalog_items
		WHERE media_type = 'episode'
		GROUP BY skey
		ORDER BY MIN(series_name)
		LIMIT ? OFFSET ?
	`, limit, offset)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	keys := make([]string, 0, limit)
	for keyRows.Next() {
		var k string
		if err := keyRows.Scan(&k); err != nil {
			keyRows.Close()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		keys = append(keys, k)
	}
	if err := keyRows.Err(); err != nil {
		keyRows.Close()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	keyRows.Close()

	items := make([]itemDTO, 0)
	if len(keys) > 0 {
		placeholders := make([]string, len(keys))
		args := make([]any, len(keys))
		for i, k := range keys {
			placeholders[i] = "?"
			args[i] = k
		}

		rows, err := db.QueryContext(r.Context(), `
			SELECT `+itemColumns+`
			FROM catalog_items
			WHERE media_type = 'episode' AND `+seriesKey+` IN (`+strings.Join(placeholders, ",")+`)
			ORDER BY series_name, season_number, episode_number
		`, args...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		items, err = scanItemRows(rows)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(itemsPage{Items: items, Total: total, Limit: limit, Offset: offset})
}
