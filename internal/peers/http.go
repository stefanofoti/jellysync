package peers

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

type peerDTO struct {
	ID    string `json:"id"`
	URL   string `json:"url"`
	State string `json:"state"`
}

// ListHandler serves GET /api/v1/peers.
func ListHandler(registry *Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		list := registry.List()
		out := make([]peerDTO, 0, len(list))
		for _, p := range list {
			out = append(out, peerDTO{ID: p.ID, URL: p.URL, State: string(p.State)})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
	}
}

// AddHandler serves POST /api/v1/peers with a {"id","url"} JSON body.
func AddHandler(registry *Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			ID  string `json:"id"`
			URL string `json:"url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		if body.ID == "" || body.URL == "" {
			http.Error(w, "id and url are required", http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		if err := registry.AddPeer(ctx, body.ID, body.URL); err != nil {
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
