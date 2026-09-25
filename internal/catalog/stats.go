package catalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
)

// OwnerCounts is the item count for one owner ("local" or a peer id),
// broken down by media type.
type OwnerCounts struct {
	Owner    string `json:"owner"`
	Movies   int    `json:"movies"`
	Series   int    `json:"series"`
	Episodes int    `json:"episodes"`
}

// Stats groups catalog_items by owner (local, or the electing peer) and
// media type. Used by both the dashboard's library panel and the
// OpenMetrics endpoint.
func Stats(ctx context.Context, db *sql.DB) ([]OwnerCounts, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			CASE WHEN local THEN 'local' ELSE COALESCE(primary_peer_id, 'unknown') END AS owner,
			media_type,
			COUNT(*)
		FROM catalog_items
		GROUP BY owner, media_type
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	byOwner := make(map[string]*OwnerCounts)
	order := make([]string, 0)
	for rows.Next() {
		var owner, mediaType string
		var n int
		if err := rows.Scan(&owner, &mediaType, &n); err != nil {
			return nil, err
		}
		oc, ok := byOwner[owner]
		if !ok {
			oc = &OwnerCounts{Owner: owner}
			byOwner[owner] = oc
			order = append(order, owner)
		}
		switch mediaType {
		case "movie":
			oc.Movies = n
		case "series":
			oc.Series = n
		case "episode":
			oc.Episodes = n
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]OwnerCounts, 0, len(order))
	for _, owner := range order {
		out = append(out, *byOwner[owner])
	}
	return out, nil
}

// StatsHandler serves GET /api/v1/stats: per-owner library counts for the
// dashboard's library panel.
func StatsHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		stats, err := Stats(r.Context(), db)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(stats)
	}
}
