package peers

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"jellysync/internal/logging"
)

type peerDTO struct {
	ID      string `json:"id"`
	Name    string `json:"name,omitempty"`
	URL     string `json:"url"`
	State   string `json:"state"`
	Version string `json:"version,omitempty"`
	IP      string `json:"ip,omitempty"`
	// LastSyncAt is when this node last pulled the peer's catalog
	// successfully; absent if it never has.
	LastSyncAt *time.Time `json:"last_sync_at,omitempty"`
}

// ListHandler serves GET /api/v1/peers. lastSync supplies each peer's
// last successful catalog sync (catalog.LastSyncTimes); it's injected
// because the catalog package depends on this one, not the other way round.
func ListHandler(registry *Registry, lastSync func(context.Context) (map[string]time.Time, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		synced, err := lastSync(r.Context())
		if err != nil {
			// Best-effort: the peer list is still useful without it.
			slog.Warn("reading last sync times for peer list", logging.Err(err))
		}
		list := registry.List()
		out := make([]peerDTO, 0, len(list))
		for _, p := range list {
			dto := peerDTO{ID: p.ID, Name: p.Name, URL: p.URL, State: string(p.State), Version: p.Version, IP: p.IP}
			if t, ok := synced[p.ID]; ok {
				dto.LastSyncAt = &t
			}
			out = append(out, dto)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
	}
}

// AddHandler serves POST /api/v1/peers with a {"url"} JSON body. The peer's
// display name is read from its own /health "node_id", and its id is
// derived from that name (see idFor) — nothing about identity comes from
// client input.
func AddHandler(registry *Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			URL string `json:"url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		if body.URL == "" {
			http.Error(w, "url is required", http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		name, version, err := registry.Probe(ctx, strings.TrimRight(body.URL, "/"))
		if err != nil {
			slog.Warn("adding peer: unreachable", "url", body.URL, logging.Err(err))
			http.Error(w, "peer unreachable: "+err.Error(), http.StatusUnprocessableEntity)
			return
		}
		id := registry.NewID(name)
		if err := registry.AddPeer(ctx, id, body.URL, name, version); err != nil {
			slog.Error("adding peer", "url", body.URL, logging.Err(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// RemoveHandler serves DELETE /api/v1/peers/{peerID}.
func RemoveHandler(registry *Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("peerID")
		if id == "" {
			http.Error(w, "missing peer id", http.StatusBadRequest)
			return
		}
		if err := registry.RemovePeer(r.Context(), id); err != nil {
			slog.Error("removing peer", "peer", id, logging.Err(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
