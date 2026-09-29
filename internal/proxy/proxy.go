// Package proxy streams media bytes between this node's local Jellyfin
// and a requesting peer, without ever exposing a Jellyfin API key to
// anyone outside the host it belongs to.
package proxy

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"jellysync/internal/jellyfin"
	"jellysync/internal/logging"
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
//
// streamBufferKB sizes the read/write buffer used to relay bytes, in KiB.
// Both sides of the relay are network sockets (never a file), so Go can't
// hand the copy off to sendfile/splice — every byte takes a userspace
// read+write syscall pair, and a bigger buffer means fewer, larger
// syscalls per second for a given bitrate.
func Handler(jf *jellyfin.Client, registry *peers.Registry, nodeID string, collector *metrics.Collector, streamBufferKB int) http.HandlerFunc {
	client := &http.Client{
		Transport: &http.Transport{
			ResponseHeaderTimeout: responseHeaderTimeout,
		},
	}

	bufferSize := streamBufferKB * 1024
	copyBufferPool := sync.Pool{
		New: func() any {
			b := make([]byte, bufferSize)
			return &b
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
			trafficPeer = callerPeerID(registry, r)
		} else if p, found := registry.Get(peerID); found {
			upstream, err = http.NewRequestWithContext(r.Context(), http.MethodGet,
				p.URL+"/api/v1/proxy/stream/local/"+itemID, nil)
			if err == nil {
				upstream.Header.Set(peerHeader, nodeID)
			}
			trafficDir = metrics.In
			trafficPeer = peerID
		} else {
			slog.Warn("stream requested for unknown peer (stale .strm?)", "peer", peerID, "item", itemID)
			http.Error(w, "unknown peer", http.StatusNotFound)
			return
		}
		if err != nil {
			slog.Error("building upstream stream request", "peer", peerID, "item", itemID, logging.Err(err))
			http.Error(w, "bad upstream request", http.StatusInternalServerError)
			return
		}

		rng := r.Header.Get("Range")
		if rng != "" {
			upstream.Header.Set("Range", rng)
		}
		log := slog.With("peer", peerID, "item", itemID, "direction", trafficDir, "traffic_peer", trafficPeer)
		started := time.Now()
		log.Debug("stream started", "range", rng)

		resp, err := client.Do(upstream)
		if err != nil {
			log.Warn("stream upstream unreachable", "upstream", upstream.URL.Redacted(), logging.Err(err))
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
			log.Warn("stream upstream returned an error",
				"upstream", upstream.URL.Redacted(), "status", resp.StatusCode, "body", string(snippet))
			body = io.MultiReader(bytes.NewReader(snippet), resp.Body)
		}

		for _, h := range []string{"Content-Type", "Content-Length", "Content-Range", "Accept-Ranges"} {
			if v := resp.Header.Get(h); v != "" {
				w.Header().Set(h, v)
			}
		}
		w.WriteHeader(resp.StatusCode)

		// Stream, don't buffer: this never loads the whole file into memory,
		// just a fixed-size chunk at a time (see copyBufferSize).
		buf := copyBufferPool.Get().(*[]byte)
		n, err := io.CopyBuffer(w, body, *buf)
		copyBufferPool.Put(buf)
		collector.RecordBytes(trafficPeer, trafficDir, n)
		elapsed := time.Since(started)
		switch {
		case err != nil && r.Context().Err() != nil:
			// The player went away mid-transfer: routine on seek/stop.
			log.Debug("stream closed by client", "status", resp.StatusCode, "bytes", n, "duration", elapsed.Round(time.Millisecond))
		case err != nil:
			log.Warn("stream interrupted", "status", resp.StatusCode, "bytes", n, "duration", elapsed.Round(time.Millisecond), logging.Err(err))
		default:
			log.Debug("stream finished", "status", resp.StatusCode, "bytes", n, "duration", elapsed.Round(time.Millisecond),
				"mbit_per_s", fmt.Sprintf("%.1f", float64(n)*8/1e6/max(elapsed.Seconds(), 0.001)))
		}
	}
}

// callerPeerID attributes a "local" stream request to the registry id of
// the peer that made it. The header carries the caller's own NODE_ID, which
// is our peer's Name, not the id we assigned it — recording the raw header
// value would split one peer's traffic across two keys (incoming under its
// id, outgoing under its name). Falls back to matching the source IP, then
// to the raw header value, then "unknown".
func callerPeerID(registry *peers.Registry, r *http.Request) string {
	nodeID := r.Header.Get(peerHeader)
	remoteIP, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		remoteIP = r.RemoteAddr
	}
	if id := registry.ResolveCaller(nodeID, remoteIP); id != "" {
		return id
	}
	if nodeID != "" {
		return nodeID
	}
	return "unknown"
}
