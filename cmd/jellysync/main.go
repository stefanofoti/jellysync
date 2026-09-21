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
	"jellysync/internal/peers"
	"jellysync/internal/proxy"
	"jellysync/internal/strm"
	"jellysync/internal/webui"
)

const version = "dev"

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

	go runSyncAndReconcileLoop(context.Background(), store, jf, registry, cfg.Strm)

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
	mux.HandleFunc("GET /api/v1/proxy/stream/{peerID}/{itemID}", proxy.Handler(jf, registry))
	mux.HandleFunc("GET /api/v1/peers", peers.ListHandler(registry))
	mux.HandleFunc("POST /api/v1/peers", peers.AddHandler(registry))
	mux.HandleFunc("DELETE /api/v1/peers/{peerID}", peers.RemoveHandler(registry))
	mux.HandleFunc("GET /api/v1/items", catalog.ItemsHandler(store))

	webHandler, err := webui.Handler()
	if err != nil {
		log.Fatalf("webui: %v", err)
	}
	mux.Handle("/", webHandler)

	log.Printf("jellysync (node %s) listening on %s", cfg.NodeID, cfg.ListenAddr)
	log.Fatal(http.ListenAndServe(cfg.ListenAddr, mux))
}

// runSyncAndReconcileLoop syncs the catalog with all peers, then reconciles
// .strm files against the result, on a fixed interval. Reconcile always
// runs right after Sync so a fresh election is reflected in .strm files
// within the same cycle rather than racing an independent timer.
func runSyncAndReconcileLoop(ctx context.Context, store *sql.DB, jf *jellyfin.Client, registry *peers.Registry, strmCfg config.Strm) {
	for {
		if err := catalog.Sync(ctx, store, jf, registry, strmCfg.OutputDir); err != nil {
			log.Printf("catalog sync: %v", err)
		} else if err := strm.Reconcile(ctx, store, jf, strmCfg); err != nil {
			log.Printf("strm reconcile: %v", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(catalog.SyncInterval):
		}
	}
}
