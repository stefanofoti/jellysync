package settings

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"time"
)

type settingsDTO struct {
	SyncIntervalSeconds int `json:"sync_interval_seconds"`
}

// GetHandler serves GET /api/v1/settings.
func GetHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		interval, err := GetSyncInterval(r.Context(), db)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(settingsDTO{SyncIntervalSeconds: int(interval.Seconds())})
	}
}

// PutHandler serves PUT /api/v1/settings with a {"sync_interval_seconds"} JSON body.
func PutHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body settingsDTO
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		if body.SyncIntervalSeconds <= 0 {
			http.Error(w, "sync_interval_seconds must be positive", http.StatusBadRequest)
			return
		}
		if err := SetSyncInterval(r.Context(), db, time.Duration(body.SyncIntervalSeconds)*time.Second); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
