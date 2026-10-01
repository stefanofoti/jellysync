// Package app wires one jellysync node together: its stores, the sync loop
// and the two HTTP surfaces — the dashboard port (dashboard plus, with
// HTTP_PEERS, the legacy unauthenticated peer API; never meant to be
// public) and, with MTLS_PEERS, the mTLS peer port (see internal/peerauth). Kept out of main so tests can run several
// nodes in one process.
package app

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"jellysync/internal/catalog"
	"jellysync/internal/config"
	"jellysync/internal/jellyfin"
	"jellysync/internal/logging"
	"jellysync/internal/metrics"
	"jellysync/internal/peerauth"
	"jellysync/internal/peers"
	"jellysync/internal/proxy"
	"jellysync/internal/settings"
	"jellysync/internal/syncrun"
	"jellysync/internal/syncstatus"
	"jellysync/internal/synctrigger"
	"jellysync/internal/webui"
)

const (
	// inviteTTL is how long an issued invite can be redeemed.
	inviteTTL = 24 * time.Hour
	// forceRefreshEvery limits how often each client may make this node
	// re-read its Jellyfin library ("force peer to refresh"), the one
	// costly thing a client can trigger.
	forceRefreshEvery = 10 * time.Minute
)

// Options are the parts of a node that aren't configuration.
type Options struct {
	// Version is this build's version, reported on /health.
	Version string
	// Now is the clock for invite expiry and rate limits; time.Now if nil.
	Now func() time.Time
}

// App is one wired-up node.
type App struct {
	cfg       *config.Config
	opts      Options
	startedAt time.Time

	db        *sql.DB
	jf        *jellyfin.Client
	identity  *peerauth.Identity
	auth      *peerauth.Store
	registry  *peers.Registry
	collector *metrics.Collector
	index     *catalog.Index
	tracker   *syncstatus.Tracker
	queue     *syncrun.Queue
	limiter   *peerauth.Limiter

	dashboard http.Handler
	peer      http.Handler
}

// New builds a node on an open DB. Nothing runs until Run.
func New(ctx context.Context, cfg *config.Config, store *sql.DB, opts Options) (*App, error) {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	a := &App{
		cfg:       cfg,
		opts:      opts,
		startedAt: time.Now(),
		db:        store,
		jf:        jellyfin.New(cfg.Jellyfin.URL, cfg.Jellyfin.APIKey, cfg.NodeID, cfg.JellyfinTimeout),
		tracker:   syncstatus.NewTracker(),
		// Manual "sync now" triggers and peers' change notifications queue
		// a run here; requests arriving while one is pending coalesce
		// rather than each queuing a run of their own.
		queue:   syncrun.NewQueue(),
		limiter: peerauth.NewLimiter(forceRefreshEvery, opts.Now),
	}

	var err error
	if a.identity, err = peerauth.LoadOrCreate(ctx, store); err != nil {
		return nil, fmt.Errorf("loading node identity: %w", err)
	}
	if a.auth, err = peerauth.NewStore(ctx, store, opts.Now); err != nil {
		return nil, fmt.Errorf("loading clients: %w", err)
	}
	if a.registry, err = peers.NewRegistry(ctx, store, cfg.Peers, a.identity); err != nil {
		return nil, fmt.Errorf("loading peers: %w", err)
	}
	a.registry.SetOtherIDs(a.auth)
	a.registry.SetTransports(cfg.HTTPPeers, cfg.MTLSPeers)
	for _, p := range a.registry.List() {
		slog.Info("tracking peer", "peer", p.ID, "url", p.URL, "state", p.State, "mtls", p.Pin != "")
	}
	if a.collector, err = metrics.NewCollector(ctx, store); err != nil {
		return nil, fmt.Errorf("loading metrics: %w", err)
	}
	if a.index, err = catalog.NewIndex(ctx, store, a.jf, cfg.Strm.OutputDir); err != nil {
		return nil, fmt.Errorf("opening catalog index: %w", err)
	}
	slog.Info("peer transports", "http_peers", cfg.HTTPPeers, "mtls_peers", cfg.MTLSPeers,
		"pin", a.identity.Pin(), "public_url", cfg.PublicURL, "clients", len(a.auth.Clients()))

	streams := proxy.Handler(a.jf, a.registry, cfg.NodeID, a.collector, cfg.StreamBufferKB)
	trigger := synctrigger.Handler(a.registry, a.tracker,
		func() { a.queue.Push(syncrun.Request{RefreshLocal: true, Reason: syncrun.ReasonManual}) },
		func() { a.queue.Push(syncrun.Request{Reason: syncrun.ReasonManual}) },
	)
	notify := catalog.NotifyHandler(func() { a.queue.Push(syncrun.Request{Reason: syncrun.ReasonPeerNotify}) })

	a.peer = a.peerMux(streams, trigger, notify)
	a.dashboard, err = a.dashboardMux(streams, trigger, notify)
	if err != nil {
		return nil, err
	}
	return a, nil
}

// Run starts the heartbeats and metrics, then runs the sync loop until ctx
// is done.
func (a *App) Run(ctx context.Context) {
	go a.registry.RunHeartbeat(ctx)
	go a.collector.Run(ctx)
	a.runSyncAndReconcileLoop(ctx)
}

// PingJellyfin checks the local Jellyfin is reachable.
func (a *App) PingJellyfin(ctx context.Context) error { return a.jf.Ping(ctx) }

// DashboardHandler serves the dashboard port: the dashboard, its API, and
// the legacy unauthenticated peer API. Never expose it publicly.
func (a *App) DashboardHandler() http.Handler { return a.dashboard }

// PeerHandler serves the mTLS peer port. It must be served with
// PeerTLSConfig.
func (a *App) PeerHandler() http.Handler { return a.peer }

// PeerTLSConfig is the peer port's TLS config: only keys of clients, peers
// and pending invites complete the handshake.
func (a *App) PeerTLSConfig() *tls.Config {
	return a.identity.ServerTLSConfig(func(pin string) bool {
		_, ok := a.resolveCaller(pin)
		return ok
	})
}

// PeerServer is the http.Server for the peer port, listening on addr.
// Serve it with ListenAndServeTLS("", "").
func (a *App) PeerServer(addr string) *http.Server {
	return &http.Server{
		Addr:      addr,
		Handler:   a.peer,
		TLSConfig: a.PeerTLSConfig(),
		// Non-nil and empty: no HTTP/2 (see peerauth.NewTransport).
		TLSNextProto:      map[string]func(*http.Server, *tls.Conn, http.Handler){},
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    64 << 10,
		// Every port scan and rejected key is a handshake error: worth
		// seeing when debugging, not above that.
		ErrorLog: slog.NewLogLogger(slog.Default().Handler(), slog.LevelDebug),
	}
}

// SyncNow runs one sync cycle synchronously, as a manual sync would.
func (a *App) SyncNow(ctx context.Context, refreshLocal bool) error {
	return syncrun.RunLocal(ctx, a.db, a.jf, a.index, a.registry, a.cfg.Strm, a.cfg.PeerFetchTimeout,
		syncrun.Request{RefreshLocal: refreshLocal, Reason: syncrun.ReasonManual}, a.cfg.NodeID, a.tracker)
}

// Registry, Clients, Collector, Queue and Identity expose the node's parts
// to tests.
func (a *App) Registry() *peers.Registry     { return a.registry }
func (a *App) Clients() *peerauth.Store      { return a.auth }
func (a *App) Collector() *metrics.Collector { return a.collector }
func (a *App) Queue() *syncrun.Queue         { return a.queue }
func (a *App) Identity() *peerauth.Identity  { return a.identity }

// resolveCaller maps a key pin to who it belongs to: a client, a peer, both,
// or a pending invite.
func (a *App) resolveCaller(pin string) (peerauth.Caller, bool) {
	c := peerauth.Caller{Pin: pin}
	if cl, ok := a.auth.ClientByPin(pin); ok {
		c.ClientID = cl.ID
		a.auth.Touch(pin)
	}
	if p, ok := a.registry.PeerByPin(pin); ok {
		c.PeerID = p.ID
	}
	if c.ClientID == "" && c.PeerID == "" {
		c.Pairing = a.auth.PendingInvite(pin)
	}
	return c, c.ClientID != "" || c.PeerID != "" || c.Pairing
}

func (a *App) health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"node_id":        a.cfg.NodeID,
		"version":        a.opts.Version,
		"uptime_seconds": int(time.Since(a.startedAt).Seconds()),
	})
}

// peerMux is the peer port: only what a paired node needs, each route
// limited to the kind of caller that needs it. Everything else, the
// dashboard and its API included, is not routed here at all.
func (a *App) peerMux(streams, trigger, notify http.HandlerFunc) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /health", peerauth.RequireClientOrPeer(http.HandlerFunc(a.health)))
	mux.Handle("GET /api/v1/catalog", peerauth.RequireClient(catalog.Handler(a.index)))
	mux.Handle("GET /api/v1/catalog/changes", peerauth.RequireClient(catalog.ChangesHandler(a.index)))
	// Only our own library: a client must never use this node as a relay
	// toward the nodes it pulls from.
	mux.Handle("GET /api/v1/proxy/stream/{peerID}/{itemID}", peerauth.RequireClient(localOnly(streams)))
	mux.Handle("POST /api/v1/sync/trigger/{peerID}", peerauth.RequireClient(localOnly(a.rateLimited(trigger))))
	mux.Handle("GET /api/v1/sync/status", peerauth.RequireClient(http.HandlerFunc(a.localSyncStatus)))
	// Notifications come from nodes we pull from (we redeemed their
	// invite), so they're peers here, not clients.
	mux.Handle("POST /api/v1/catalog/notify", peerauth.RequirePeer(notify))
	mux.Handle("POST /api/v1/pair", peerauth.RequirePairing(peerauth.PairHandler(a.auth, a.registry.ClientID, a.cfg.NodeID, a.opts.Version)))
	return peerauth.Authenticate(a.resolveCaller, mux)
}

// localOnly 404s any {peerID} but "local".
func localOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("peerID") != "local" {
			http.NotFound(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// rateLimited allows each caller one request per forceRefreshEvery.
func (a *App) rateLimited(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, _ := peerauth.CallerFrom(r.Context())
		if !a.limiter.Allow(c.ID()) {
			slog.Info("refresh request from client refused: too frequent", "client", c.ID(), "every", forceRefreshEvery)
			w.Header().Set("Retry-After", strconv.Itoa(int(forceRefreshEvery.Seconds())))
			http.Error(w, "too many refresh requests", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// localSyncStatus is GET /api/v1/sync/status for clients: only this node's
// own ("local") status, which is what a forced refresh polls for — not the
// ids and states of the nodes this one pulls from.
func (a *App) localSyncStatus(w http.ResponseWriter, r *http.Request) {
	out := map[string]syncstatus.Status{}
	if st, ok := a.tracker.Get("local"); ok {
		out["local"] = st
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

func (a *App) dashboardMux(streams, trigger, notify http.HandlerFunc) (http.Handler, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", a.health)
	// The legacy, unauthenticated peer API. stream/{peerID} stays either
	// way: it's also how this node's own Jellyfin plays .strm files and how
	// the dashboard previews items.
	if a.cfg.HTTPPeers {
		mux.HandleFunc("/api/v1/catalog", catalog.Handler(a.index))
		mux.HandleFunc("GET /api/v1/catalog/changes", catalog.ChangesHandler(a.index))
		mux.HandleFunc("POST /api/v1/catalog/notify", notify)
	}
	mux.HandleFunc("GET /api/v1/proxy/stream/{peerID}/{itemID}", streams)
	mux.HandleFunc("GET /api/v1/peers", peers.ListHandler(a.registry, func(ctx context.Context) (map[string]time.Time, error) {
		return catalog.LastSyncTimes(ctx, a.db)
	}))
	mux.HandleFunc("POST /api/v1/peers", peers.AddHandler(a.registry))
	mux.HandleFunc("POST /api/v1/peers/redeem", peers.RedeemHandler(a.registry, a.cfg.NodeID))
	mux.HandleFunc("DELETE /api/v1/peers/{peerID}", peers.RemoveHandler(a.registry))
	mux.HandleFunc("GET /api/v1/peerport", a.peerPortInfo)
	if a.cfg.MTLSPeers {
		mux.HandleFunc("POST /api/v1/invites", peerauth.CreateInviteHandler(a.auth, peerauth.InviteConfig{
			Identity: a.identity, PublicURL: a.cfg.PublicURL, NodeID: a.cfg.NodeID, TTL: inviteTTL,
		}))
	} else {
		mux.HandleFunc("POST /api/v1/invites", func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "pairing by invite is turned off on this node: set MTLS_PEERS=true", http.StatusConflict)
		})
	}
	mux.HandleFunc("GET /api/v1/invites", peerauth.ListInvitesHandler(a.auth))
	mux.HandleFunc("DELETE /api/v1/invites/{pin}", peerauth.CancelInviteHandler(a.auth))
	mux.HandleFunc("GET /api/v1/clients", peerauth.ListClientsHandler(a.auth))
	mux.HandleFunc("DELETE /api/v1/clients/{clientID}", peerauth.RevokeClientHandler(a.auth))
	mux.HandleFunc("GET /api/v1/items", catalog.ItemsHandler(a.db))
	mux.HandleFunc("GET /api/v1/items/series/episodes", catalog.SeriesEpisodesHandler(a.db))
	mux.HandleFunc("GET /api/v1/stats", catalog.StatsHandler(a.db))
	mux.HandleFunc("GET /api/v1/traffic", metrics.TrafficHandler(a.collector, a.registry))
	mux.HandleFunc("DELETE /api/v1/traffic", metrics.ResetHandler(a.collector))
	mux.HandleFunc("GET /metrics", metrics.OpenMetricsHandler(a.db, a.collector, a.registry))
	mux.HandleFunc("GET /api/v1/settings", settings.GetHandler(a.db))
	mux.HandleFunc("PUT /api/v1/settings", settings.PutHandler(a.db))
	mux.HandleFunc("GET /api/v1/sync/status", syncstatus.StatusHandler(a.tracker))
	mux.HandleFunc("POST /api/v1/sync/trigger/{peerID}", trigger)

	webHandler, err := webui.Handler()
	if err != nil {
		return nil, fmt.Errorf("loading web ui: %w", err)
	}
	mux.Handle("/", webHandler)
	return mux, nil
}

// peerPortInfo serves GET /api/v1/peerport: what the dashboard shows about
// this node's mTLS peer port.
func (a *App) peerPortInfo(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"http_peers":      a.cfg.HTTPPeers,
		"mtls_peers":      a.cfg.MTLSPeers,
		"public_url":      a.cfg.PublicURL,
		"pin":             a.identity.Pin(),
		"invites_enabled": a.cfg.MTLSPeers && a.cfg.PublicURL != "",
	})
}

// runSyncAndReconcileLoop runs syncrun.RunLocal (optionally a local
// catalog refresh, then catalog.Sync, then strm.Reconcile — Reconcile always runs
// right after Sync so a fresh election is reflected in .strm files within
// the same cycle rather than racing an independent timer) on the
// user-configured interval, or immediately whenever queue has a request
// (a manual "sync now", or a peer's change notification). No run ever
// asks Jellyfin to scan its files on disk; the local refresh only reads
// Jellyfin's current index.
func (a *App) runSyncAndReconcileLoop(ctx context.Context) {
	req := syncrun.Request{RefreshLocal: true, Reason: syncrun.ReasonStartup}
	for {
		// RunLocal logs its own outcome, errors included.
		_ = syncrun.RunLocal(ctx, a.db, a.jf, a.index, a.registry, a.cfg.Strm, a.cfg.PeerFetchTimeout, req, a.cfg.NodeID, a.tracker)

		interval, err := settings.GetSyncInterval(ctx, a.db)
		if err != nil {
			slog.Warn("reading sync interval, using default", logging.Err(err), "default", settings.DefaultSyncInterval)
			interval = settings.DefaultSyncInterval
		}
		slog.Debug("next scheduled sync", "in", interval, "at", time.Now().Add(interval).Format(time.RFC3339))

		req = waitForNextRun(ctx, a.queue, interval)
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
