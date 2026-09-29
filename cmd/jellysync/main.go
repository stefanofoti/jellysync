package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"time"

	"jellysync/internal/catalog"
	"jellysync/internal/config"
	"jellysync/internal/db"
	"jellysync/internal/jellyfin"
	"jellysync/internal/logging"
	"jellysync/internal/metrics"
	"jellysync/internal/peers"
	"jellysync/internal/proxy"
	"jellysync/internal/settings"
	"jellysync/internal/syncrun"
	"jellysync/internal/syncstatus"
	"jellysync/internal/synctrigger"
	"jellysync/internal/webui"
)

// version identifies this node's build to peers over /health, so a remote
// jellysync can tell what version it's talking to. Overridden at build time
// via -ldflags "-X main.version=...": the Docker build stamps it from the
// VERSION file, and a plain "go build"/"go run" leaves it as "dev".
var version = "dev"

func main() {
	startedAt := time.Now()

	cfg, err := config.Load()
	if err != nil {
		fatal("invalid configuration", err)
	}
	logging.Setup(cfg.LogLevel)
	slog.Info("jellysync starting",
		"node", cfg.NodeID, "version", version, "listen", cfg.ListenAddr, "log_level", cfg.LogLevel.String(),
		"db", cfg.DBPath, "jellyfin", cfg.Jellyfin.URL, "output_dir", cfg.Strm.OutputDir, "base_url", cfg.Strm.BaseURL,
		"configured_peers", len(cfg.Peers), "jellyfin_timeout", cfg.JellyfinTimeout,
		"peer_fetch_timeout", cfg.PeerFetchTimeout, "stream_buffer_kb", cfg.StreamBufferKB)

	store, err := db.Open(cfg.DBPath)
	if err != nil {
		fatal("opening database", err, "path", cfg.DBPath)
	}
	defer store.Close()

	jf := jellyfin.New(cfg.Jellyfin.URL, cfg.Jellyfin.APIKey, cfg.NodeID, cfg.JellyfinTimeout)
	pingCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := jf.Ping(pingCtx); err != nil {
		fatal("cannot reach local jellyfin", err, "url", cfg.Jellyfin.URL)
	}
	slog.Info("connected to local jellyfin", "url", cfg.Jellyfin.URL)

	registry, err := peers.NewRegistry(context.Background(), store, cfg.Peers)
	if err != nil {
		fatal("loading peers", err)
	}
	go registry.RunHeartbeat(context.Background())
	for _, p := range registry.List() {
		slog.Info("tracking peer", "peer", p.ID, "url", p.URL, "state", p.State)
	}

	collector, err := metrics.NewCollector(context.Background(), store)
	if err != nil {
		fatal("loading metrics", err)
	}
	go collector.Run(context.Background())

	index, err := catalog.NewIndex(context.Background(), store, jf, cfg.Strm.OutputDir)
	if err != nil {
		fatal("opening catalog index", err)
	}

	tracker := syncstatus.NewTracker()
	// Manual "sync now" triggers and peers' change notifications queue a
	// run here; requests arriving while one is pending coalesce rather than
	// each queuing a run of their own.
	queue := syncrun.NewQueue()
	go runSyncAndReconcileLoop(context.Background(), store, jf, index, registry, cfg, tracker, queue)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"node_id":        cfg.NodeID,
			"version":        version,
			"uptime_seconds": int(time.Since(startedAt).Seconds()),
		})
	})
	mux.HandleFunc("/api/v1/catalog", catalog.Handler(index))
	mux.HandleFunc("GET /api/v1/catalog/changes", catalog.ChangesHandler(index))
	mux.HandleFunc("POST /api/v1/catalog/notify", catalog.NotifyHandler(func() {
		queue.Push(syncrun.Request{Reason: syncrun.ReasonPeerNotify})
	}))
	mux.HandleFunc("GET /api/v1/proxy/stream/{peerID}/{itemID}", proxy.Handler(jf, registry, cfg.NodeID, collector, cfg.StreamBufferKB))
	mux.HandleFunc("GET /api/v1/peers", peers.ListHandler(registry, func(ctx context.Context) (map[string]time.Time, error) {
		return catalog.LastSyncTimes(ctx, store)
	}))
	mux.HandleFunc("POST /api/v1/peers", peers.AddHandler(registry))
	mux.HandleFunc("DELETE /api/v1/peers/{peerID}", peers.RemoveHandler(registry))
	mux.HandleFunc("GET /api/v1/items", catalog.ItemsHandler(store))
	mux.HandleFunc("GET /api/v1/items/series/episodes", catalog.SeriesEpisodesHandler(store))
	mux.HandleFunc("GET /api/v1/stats", catalog.StatsHandler(store))
	mux.HandleFunc("GET /api/v1/traffic", metrics.TrafficHandler(collector, registry))
	mux.HandleFunc("GET /metrics", metrics.OpenMetricsHandler(store, collector, registry))
	mux.HandleFunc("GET /api/v1/settings", settings.GetHandler(store))
	mux.HandleFunc("PUT /api/v1/settings", settings.PutHandler(store))
	mux.HandleFunc("GET /api/v1/sync/status", syncstatus.StatusHandler(tracker))
	mux.HandleFunc("POST /api/v1/sync/trigger/{peerID}", synctrigger.Handler(registry, tracker, func(force bool) {
		reason := syncrun.ReasonManual
		if force {
			reason = syncrun.ReasonManualForce
		}
		queue.Push(syncrun.Request{ForceScan: force, RefreshLocal: true, Reason: reason})
	}))

	webHandler, err := webui.Handler()
	if err != nil {
		fatal("loading web ui", err)
	}
	mux.Handle("/", webHandler)

	slog.Info("listening", "addr", cfg.ListenAddr)
	fatal("http server stopped", http.ListenAndServe(cfg.ListenAddr, mux))
}

// runSyncAndReconcileLoop runs syncrun.RunLocal (optionally a Jellyfin
// refresh, then catalog.Sync, then strm.Reconcile — Reconcile always runs
// right after Sync so a fresh election is reflected in .strm files within
// the same cycle rather than racing an independent timer) on the
// user-configured interval, or immediately whenever queue has a request
// (a manual "sync now", or a peer's change notification). The scheduled
// interval re-reads the local Jellyfin index but never forces a rescan —
// only a manual trigger with force=true does, since that's an explicit
// "check for anything new" request rather than routine catalog upkeep.
func runSyncAndReconcileLoop(ctx context.Context, store *sql.DB, jf *jellyfin.Client, index *catalog.Index, registry *peers.Registry, cfg *config.Config, tracker *syncstatus.Tracker, queue *syncrun.Queue) {
	req := syncrun.Request{RefreshLocal: true, Reason: syncrun.ReasonStartup}
	for {
		// RunLocal logs its own outcome, errors included.
		_ = syncrun.RunLocal(ctx, store, jf, index, registry, cfg.Strm, cfg.PeerFetchTimeout, req, cfg.NodeID, tracker)

		interval, err := settings.GetSyncInterval(ctx, store)
		if err != nil {
			slog.Warn("reading sync interval, using default", logging.Err(err), "default", settings.DefaultSyncInterval)
			interval = settings.DefaultSyncInterval
		}
		slog.Debug("next scheduled sync", "in", interval, "at", time.Now().Add(interval).Format(time.RFC3339))

		req = waitForNextRun(ctx, queue, interval)
		if ctx.Err() != nil {
			return
		}
	}
}

// waitForNextRun blocks until a queued request arrives or interval elapses,
// returning what the next run should do.
func waitForNextRun(ctx context.Context, queue *syncrun.Queue, interval time.Duration) syncrun.Request {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return syncrun.Request{}
		case <-queue.C():
			// The wake token can outlive the request it announced (the
			// timer branch below may already have taken it).
			if req, ok := queue.Take(); ok {
				return req
			}
		case <-timer.C:
			req := syncrun.Request{RefreshLocal: true, Reason: syncrun.ReasonScheduled}
			if queued, ok := queue.Take(); ok {
				req = req.Merge(queued)
			}
			return req
		}
	}
}

// fatal logs err at ERROR and exits.
func fatal(msg string, err error, args ...any) {
	slog.Error(msg, append([]any{logging.Err(err)}, args...)...)
	os.Exit(1)
}
