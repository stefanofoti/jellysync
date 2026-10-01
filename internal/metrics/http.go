package metrics

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"

	"jellysync/internal/catalog"
	"jellysync/internal/logging"
	"jellysync/internal/peers"
)

// TrafficHandler serves GET /api/v1/traffic: per-peer byte totals, current
// bitrate, and recent history for the dashboard's traffic chart.
func TrafficHandler(c *Collector, registry *peers.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resolvedSnapshot(c, registry))
	}
}

// ResetHandler serves DELETE /api/v1/traffic: clears all traffic stats.
func ResetHandler(c *Collector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := c.Reset(r.Context()); err != nil {
			slog.Error("clearing traffic stats", logging.Err(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		slog.Info("traffic stats cleared")
		w.WriteHeader(http.StatusNoContent)
	}
}

// resolvedSnapshot is c.Snapshot() with any entry keyed by a peer's Name
// (its self-reported node id) folded into that peer's registry id. Older
// builds recorded outgoing traffic under the caller's node id rather than
// our id for it, and those totals are persisted, so without this one peer
// would show up twice: incoming under its id, outgoing under its name.
func resolvedSnapshot(c *Collector, registry *peers.Registry) map[string]PeerTraffic {
	snap := c.Snapshot()
	for _, p := range registry.List() {
		if p.Name == "" || p.Name == p.ID {
			continue
		}
		legacy, ok := snap[p.Name]
		if !ok {
			continue
		}
		snap[p.ID] = mergeTraffic(snap[p.ID], legacy)
		delete(snap, p.Name)
	}
	return snap
}

func mergeTraffic(a, b PeerTraffic) PeerTraffic {
	byT := make(map[int64]Sample, len(a.History)+len(b.History))
	for _, h := range [][]Sample{a.History, b.History} {
		for _, s := range h {
			m := byT[s.T]
			m.T = s.T
			m.InBps += s.InBps
			m.OutBps += s.OutBps
			byT[s.T] = m
		}
	}
	history := make([]Sample, 0, len(byT))
	for _, s := range byT {
		history = append(history, s)
	}
	sort.Slice(history, func(i, j int) bool { return history[i].T < history[j].T })

	return PeerTraffic{
		TotalIn:    a.TotalIn + b.TotalIn,
		TotalOut:   a.TotalOut + b.TotalOut,
		CurrentIn:  a.CurrentIn + b.CurrentIn,
		CurrentOut: a.CurrentOut + b.CurrentOut,
		History:    history,
	}
}

// OpenMetricsHandler serves GET /metrics in OpenMetrics text exposition
// format: library size by owner/media type, peer reachability, and
// per-peer traffic totals plus current bitrate.
func OpenMetricsHandler(db *sql.DB, c *Collector, registry *peers.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var b strings.Builder

		stats, err := catalog.Stats(r.Context(), db)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		b.WriteString("# TYPE jellysync_library_items gauge\n")
		b.WriteString("# HELP jellysync_library_items Catalog item count by owner and media type.\n")
		for _, s := range stats {
			fmt.Fprintf(&b, "jellysync_library_items{owner=%q,media_type=\"movie\"} %d\n", s.Owner, s.Movies)
			fmt.Fprintf(&b, "jellysync_library_items{owner=%q,media_type=\"series\"} %d\n", s.Owner, s.Series)
			fmt.Fprintf(&b, "jellysync_library_items{owner=%q,media_type=\"episode\"} %d\n", s.Owner, s.Episodes)
		}

		b.WriteString("# TYPE jellysync_peer_up gauge\n")
		b.WriteString("# HELP jellysync_peer_up 1 if the peer's last heartbeat was healthy, else 0.\n")
		for _, p := range registry.List() {
			up := 0
			if p.State == peers.StateOnline {
				up = 1
			}
			fmt.Fprintf(&b, "jellysync_peer_up{peer_id=%q} %d\n", p.ID, up)
		}

		b.WriteString("# TYPE jellysync_traffic_bytes counter\n")
		b.WriteString("# HELP jellysync_traffic_bytes Cumulative bytes transferred per peer and direction.\n")
		b.WriteString("# TYPE jellysync_traffic_bitrate_bytes_per_second gauge\n")
		b.WriteString("# HELP jellysync_traffic_bitrate_bytes_per_second Current transfer rate per peer and direction.\n")
		for peerID, t := range resolvedSnapshot(c, registry) {
			fmt.Fprintf(&b, "jellysync_traffic_bytes{peer_id=%q,direction=\"in\"} %d\n", peerID, t.TotalIn)
			fmt.Fprintf(&b, "jellysync_traffic_bytes{peer_id=%q,direction=\"out\"} %d\n", peerID, t.TotalOut)
			fmt.Fprintf(&b, "jellysync_traffic_bitrate_bytes_per_second{peer_id=%q,direction=\"in\"} %f\n", peerID, t.CurrentIn)
			fmt.Fprintf(&b, "jellysync_traffic_bitrate_bytes_per_second{peer_id=%q,direction=\"out\"} %f\n", peerID, t.CurrentOut)
		}

		b.WriteString("# EOF\n")

		w.Header().Set("Content-Type", "application/openmetrics-text; version=1.0.0; charset=utf-8")
		w.Write([]byte(b.String()))
	}
}
