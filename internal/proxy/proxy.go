// Package proxy streams media bytes between this node's local Jellyfin
// and a requesting peer, without ever exposing a Jellyfin API key to
// anyone outside the host it belongs to.
package proxy

import (
	"io"
	"log"
	"net/http"
	"time"

	"jellysync/internal/jellyfin"
	"jellysync/internal/peers"
)

const responseHeaderTimeout = 10 * time.Second

// Handler serves "GET /api/v1/proxy/stream/{peerID}/{itemID}".
//
// When peerID is "local", the request is served directly from this node's
// own Jellyfin instance using its own API key — this is what a peer's
// jellysync calls on our behalf, so our key never leaves this host.
//
// For any other peerID, the request is forwarded to that peer's own
// jellysync node at ".../stream/local/{itemID}", which serves it from ITS
// local Jellyfin the same way. This node never talks to a peer's Jellyfin
// directly.
func Handler(jf *jellyfin.Client, registry *peers.Registry) http.HandlerFunc {
	client := &http.Client{
		Transport: &http.Transport{
			ResponseHeaderTimeout: responseHeaderTimeout,
		},
	}

	return func(w http.ResponseWriter, r *http.Request) {
		peerID := r.PathValue("peerID")
		itemID := r.PathValue("itemID")

		var upstream *http.Request
		var err error

		if peerID == "local" {
			upstream, err = jf.NewDownloadRequest(r.Context(), itemID)
		} else if p, found := registry.Get(peerID); found {
			upstream, err = http.NewRequestWithContext(r.Context(), http.MethodGet,
				p.URL+"/api/v1/proxy/stream/local/"+itemID, nil)
		} else {
			http.Error(w, "unknown peer", http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, "bad upstream request", http.StatusInternalServerError)
			return
		}

		if rng := r.Header.Get("Range"); rng != "" {
			upstream.Header.Set("Range", rng)
		}

		resp, err := client.Do(upstream)
		if err != nil {
			log.Printf("proxy: peer=%s item=%s: %v", peerID, itemID, err)
			http.Error(w, "upstream unreachable", http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		for _, h := range []string{"Content-Type", "Content-Length", "Content-Range", "Accept-Ranges"} {
			if v := resp.Header.Get(h); v != "" {
				w.Header().Set(h, v)
			}
		}
		w.WriteHeader(resp.StatusCode)

		// Stream, don't buffer: io.Copy uses a small internal buffer and
		// never loads the whole file into memory.
		if _, err := io.Copy(w, resp.Body); err != nil {
			log.Printf("proxy: peer=%s item=%s: copy: %v", peerID, itemID, err)
		}
	}
}
