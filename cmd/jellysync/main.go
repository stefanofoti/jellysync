package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"jellysync/internal/catalog"
	"jellysync/internal/config"
	"jellysync/internal/db"
	"jellysync/internal/jellyfin"
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
		log.Fatalf("config: %v", err)
	}

	store, err := db.Open(cfg.DBPath)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer store.Close()

	jf := jellyfin.New(cfg.Jellyfin.URL, cfg.Jellyfin.APIKey, cfg.NodeID, cfg.JellyfinTimeout)
	pingCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := jf.Ping(pingCtx); err != nil {
		log.Fatalf("cannot reach local jellyfin at %s: %v", cfg.Jellyfin.URL, err)
	}
	log.Printf("connected to local jellyfin at %s", cfg.Jellyfin.URL)

	registry, err := peers.NewRegistry(context.Background(), store, cfg.Peers)
	if err != nil {
		log.Fatalf("peers: %v", err)
	}
	go registry.RunHeartbeat(context.Background())
	log.Printf("tracking %d configured peer(s)", len(cfg.Peers))

	collector, err := metrics.NewCollector(context.Background(), store)
	if err != nil {
		log.Fatalf("metrics: %v", err)
	}
	go collector.Run(context.Background())

	index, err := catalog.NewIndex(context.Background(), store, jf, cfg.Strm.OutputDir)
	if err != nil {
		log.Fatalf("catalog: %v", err)
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
		queue.Push(syncrun.Request{})
	}))
	mux.HandleFunc("GET /api/v1/proxy/stream/{peerID}/{itemID}", proxy.Handler(jf, registry, cfg.NodeID, collector, cfg.StreamBufferKB))
	mux.HandleFunc("GET /api/v1/peers", peers.ListHandler(registry))
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
		queue.Push(syncrun.Request{ForceScan: force, RefreshLocal: true})
	}))

	webHandler, err := webui.Handler()
	if err != nil {
		log.Fatalf("webui: %v", err)
	}
	mux.Handle("/", webHandler)

	log.Printf("jellysync (node %s) listening on %s", cfg.NodeID, cfg.ListenAddr)
	log.Fatal(http.ListenAndServe(cfg.ListenAddr, mux))
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
	req := syncrun.Request{RefreshLocal: true}
	for {
		if err := syncrun.RunLocal(ctx, store, jf, index, registry, cfg.Strm, cfg.PeerFetchTimeout, req, cfg.NodeID, tracker); err != nil {
			log.Printf("sync: %v", err)
		}

		interval, err := settings.GetSyncInterval(ctx, store)
		if err != nil {
			log.Printf("reading sync interval: %v", err)
			interval = settings.DefaultSyncInterval
		}

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
			req := syncrun.Request{RefreshLocal: true}
			if queued, ok := queue.Take(); ok {
				req = req.Merge(queued)
			}
			return req
		}
	}
}
