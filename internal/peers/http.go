package peers

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

type peerDTO struct {
	ID      string `json:"id"`
	Name    string `json:"name,omitempty"`
	URL     string `json:"url"`
	State   string `json:"state"`
	Version string `json:"version,omitempty"`
}

// ListHandler serves GET /api/v1/peers.
func ListHandler(registry *Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		list := registry.List()
		out := make([]peerDTO, 0, len(list))
		for _, p := range list {
			out = append(out, peerDTO{ID: p.ID, Name: p.Name, URL: p.URL, State: string(p.State), Version: p.Version})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
	}
}

// AddHandler serves POST /api/v1/peers with a {"url"} JSON body. The peer's
// id is generated here (opaque, never user-supplied) and its display name
// is read from the peer's own /health "node_id" — nothing about identity
// comes from client input.
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
			http.Error(w, "peer unreachable: "+err.Error(), http.StatusUnprocessableEntity)
			return
		}
		id := uuid.NewString()
		if err := registry.AddPeer(ctx, id, body.URL, name, version); err != nil {
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
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
