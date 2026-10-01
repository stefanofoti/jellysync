package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"jellysync/internal/app"
	"jellysync/internal/config"
	"jellysync/internal/db"
	"jellysync/internal/logging"
)

// version identifies this node's build to peers over /health, so a remote
// jellysync can tell what version it's talking to. Overridden at build time
// via -ldflags "-X main.version=...": the Docker build stamps it from the
// VERSION file, and a plain "go build"/"go run" leaves it as "dev".
var version = "dev"

func main() {
	cfg, err := config.Load()
	if err != nil {
		fatal("invalid configuration", err)
	}
	logging.Setup(cfg.LogLevel)
	slog.Info("jellysync starting",
		"node", cfg.NodeID, "version", version, "listen", cfg.ListenAddr, "http_peers", cfg.HTTPPeers,
		"mtls_peers", cfg.MTLSPeers, "peer_listen", cfg.PeerListenAddr,
		"public_url", cfg.PublicURL, "log_level", cfg.LogLevel.String(),
		"db", cfg.DBPath, "jellyfin", cfg.Jellyfin.URL, "output_dir", cfg.Strm.OutputDir, "base_url", cfg.Strm.BaseURL,
		"configured_peers", len(cfg.Peers), "jellyfin_timeout", cfg.JellyfinTimeout,
		"peer_fetch_timeout", cfg.PeerFetchTimeout, "stream_buffer_kb", cfg.StreamBufferKB)

	store, err := db.Open(cfg.DBPath)
	if err != nil {
		fatal("opening database", err, "path", cfg.DBPath)
	}
	defer store.Close()

	ctx := context.Background()
	node, err := app.New(ctx, cfg, store, app.Options{Version: version})
	if err != nil {
		fatal("starting node", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := node.PingJellyfin(pingCtx); err != nil {
		fatal("cannot reach local jellyfin", err, "url", cfg.Jellyfin.URL)
	}
	slog.Info("connected to local jellyfin", "url", cfg.Jellyfin.URL)

	go node.Run(ctx)

	// The peer port is opt-in (MTLS_PEERS) and additive: if it can't
	// listen (say, the port is taken on a bare-metal host), the dashboard
	// port and every legacy peer on it keep working.
	if cfg.MTLSPeers {
		go func() {
			slog.Info("peer port listening (mTLS)", "addr", cfg.PeerListenAddr, "invites_enabled", cfg.PublicURL != "")
			err := node.PeerServer(cfg.PeerListenAddr).ListenAndServeTLS("", "")
			slog.Error("peer port stopped; paired peers can't reach this node, legacy peers are unaffected", "addr", cfg.PeerListenAddr, logging.Err(err))
		}()
	}
	slog.Info("listening", "addr", cfg.ListenAddr)
	fatal("http server stopped", http.ListenAndServe(cfg.ListenAddr, node.DashboardHandler()))
}

// fatal logs err at ERROR and exits.
func fatal(msg string, err error, args ...any) {
	slog.Error(msg, append([]any{logging.Err(err)}, args...)...)
	os.Exit(1)
}
