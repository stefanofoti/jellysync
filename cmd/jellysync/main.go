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

	jf := jellyfin.New(cfg.Jellyfin.URL, cfg.Jellyfin.APIKey, cfg.NodeID)
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

	tracker := syncstatus.NewTracker()
	// Buffered size 1: a manual "sync now" trigger just needs to guarantee
	// the loop wakes up promptly, not that every trigger while a sync is
	// already running queues up a second one.
	triggerCh := make(chan struct{}, 1)
	go runSyncAndReconcileLoop(context.Background(), store, jf, registry, cfg.Strm, tracker, triggerCh)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"node_id":        cfg.NodeID,
			"version":        version,
			"uptime_seconds": int(time.Since(startedAt).Seconds()),
		})
	})
	mux.HandleFunc("/api/v1/catalog", catalog.Handler(jf, cfg.Strm.OutputDir))
	mux.HandleFunc("GET /api/v1/proxy/stream/{peerID}/{itemID}", proxy.Handler(jf, registry, cfg.NodeID, collector))
	mux.HandleFunc("GET /api/v1/peers", peers.ListHandler(registry))
	mux.HandleFunc("POST /api/v1/peers", peers.AddHandler(registry))
	mux.HandleFunc("DELETE /api/v1/peers/{peerID}", peers.RemoveHandler(registry))
	mux.HandleFunc("GET /api/v1/items", catalog.ItemsHandler(store))
	mux.HandleFunc("GET /api/v1/stats", catalog.StatsHandler(store))
	mux.HandleFunc("GET /api/v1/traffic", metrics.TrafficHandler(collector))
	mux.HandleFunc("GET /metrics", metrics.OpenMetricsHandler(store, collector, registry))
	mux.HandleFunc("GET /api/v1/settings", settings.GetHandler(store))
	mux.HandleFunc("PUT /api/v1/settings", settings.PutHandler(store))
	mux.HandleFunc("GET /api/v1/sync/status", syncstatus.StatusHandler(tracker))
	mux.HandleFunc("POST /api/v1/sync/trigger/{peerID}", synctrigger.Handler(registry, tracker, func() {
		select {
		case triggerCh <- struct{}{}:
		default:
		}
	}))

	webHandler, err := webui.Handler()
	if err != nil {
		log.Fatalf("webui: %v", err)
	}
	mux.Handle("/", webHandler)

	log.Printf("jellysync (node %s) listening on %s", cfg.NodeID, cfg.ListenAddr)
	log.Fatal(http.ListenAndServe(cfg.ListenAddr, mux))
}

// runSyncAndReconcileLoop runs syncrun.RunLocal (Jellyfin refresh, then
// catalog.Sync, then strm.Reconcile — Reconcile always runs right after
// Sync so a fresh election is reflected in .strm files within the same
// cycle rather than racing an independent timer) on the user-configured
// interval, or immediately whenever triggerCh fires (a manual "sync now").
func runSyncAndReconcileLoop(ctx context.Context, store *sql.DB, jf *jellyfin.Client, registry *peers.Registry, strmCfg config.Strm, tracker *syncstatus.Tracker, triggerCh <-chan struct{}) {
	for {
		if err := syncrun.RunLocal(ctx, store, jf, registry, strmCfg, tracker); err != nil {
			log.Printf("sync: %v", err)
		}

		interval, err := settings.GetSyncInterval(ctx, store)
		if err != nil {
			log.Printf("reading sync interval: %v", err)
			interval = settings.DefaultSyncInterval
		}

		select {
		case <-ctx.Done():
			return
		case <-triggerCh:
		case <-time.After(interval):
		}
	}
}
