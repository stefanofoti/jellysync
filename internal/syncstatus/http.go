package syncstatus

import (
	"encoding/json"
	"net/http"
)

// StatusHandler serves GET /api/v1/sync/status, returning every tracked
// peer's (and "local"'s) last known sync Status.
func StatusHandler(tracker *Tracker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(tracker.All())
	}
}
