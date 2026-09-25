// Package proxy streams media bytes between this node's local Jellyfin
// and a requesting peer, without ever exposing a Jellyfin API key to
// anyone outside the host it belongs to.
package proxy

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"time"

	"jellysync/internal/jellyfin"
	"jellysync/internal/metrics"
	"jellysync/internal/peers"
)

const (
	responseHeaderTimeout = 10 * time.Second
	errorSnippetSize      = 512

	// peerHeader carries the calling node's own ID on a forwarded stream
	// request, so the peer serving the "local" branch can attribute
	// outgoing traffic to the right caller. Self-reported, like every
	// other cross-node interaction in this system (see "Security model"
	// in CLAUDE.md) — trust boundary is the VPN, not this header.
	peerHeader = "X-Jellysync-Peer"
)

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
func Handler(jf *jellyfin.Client, registry *peers.Registry, nodeID string, collector *metrics.Collector) http.HandlerFunc {
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
		// trafficPeer is who this transfer should be attributed to, and
		// trafficDir is the direction relative to this node.
		var trafficPeer string
		var trafficDir metrics.Direction

		if peerID == "local" {
			upstream, err = jf.NewDownloadRequest(r.Context(), itemID)
			trafficDir = metrics.Out
			trafficPeer = r.Header.Get(peerHeader)
			if trafficPeer == "" {
				trafficPeer = "unknown"
			}
		} else if p, found := registry.Get(peerID); found {
			upstream, err = http.NewRequestWithContext(r.Context(), http.MethodGet,
				p.URL+"/api/v1/proxy/stream/local/"+itemID, nil)
			if err == nil {
				upstream.Header.Set(peerHeader, nodeID)
			}
			trafficDir = metrics.In
			trafficPeer = peerID
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

		body := io.Reader(resp.Body)
		if resp.StatusCode >= 400 {
			// Log a snippet of the error body so a failing peer or Jellyfin
			// is diagnosable from this side, then still forward it in full.
			snippet := make([]byte, errorSnippetSize)
			n, _ := io.ReadFull(resp.Body, snippet)
			snippet = snippet[:n]
			log.Printf("proxy: peer=%s item=%s: upstream %s returned %d: %q",
				peerID, itemID, upstream.URL.Redacted(), resp.StatusCode, snippet)
			body = io.MultiReader(bytes.NewReader(snippet), resp.Body)
		}

		for _, h := range []string{"Content-Type", "Content-Length", "Content-Range", "Accept-Ranges"} {
			if v := resp.Header.Get(h); v != "" {
				w.Header().Set(h, v)
			}
		}
		w.WriteHeader(resp.StatusCode)

		// Stream, don't buffer: io.Copy uses a small internal buffer and
		// never loads the whole file into memory.
		n, err := io.Copy(w, body)
		collector.RecordBytes(trafficPeer, trafficDir, n)
		if err != nil {
			log.Printf("proxy: peer=%s item=%s: copy: %v", peerID, itemID, err)
		}
	}
}
