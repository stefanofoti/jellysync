package catalog

import (
	"database/sql"
	"encoding/json"
	"net/http"
)

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
}

// ItemsHandler serves GET /api/v1/items: a dashboard-facing view of
// catalog_items (distinct from the peer-facing GET /api/v1/catalog wire
// format, which only ever describes this node's own local library).
func ItemsHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rows, err := db.QueryContext(r.Context(), `
			SELECT global_id, name, media_type, local, primary_peer_id, strm_path,
			       series_global_id, series_name, season_number, episode_number
			FROM catalog_items
			ORDER BY name
		`)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		items := make([]itemDTO, 0)
		for rows.Next() {
			var it itemDTO
			var localInt int
			var primaryPeerID, strmPath sql.NullString
			if err := rows.Scan(&it.GlobalID, &it.Name, &it.MediaType, &localInt, &primaryPeerID, &strmPath,
				&it.SeriesGlobalID, &it.SeriesName, &it.SeasonNumber, &it.EpisodeNumber); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			it.Local = localInt != 0
			it.PrimaryPeerID = primaryPeerID.String
			it.StrmPath = strmPath.String
			items = append(items, it)
		}
		if err := rows.Err(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(items)
	}
}
