# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

jellysync lets peers share Jellyfin libraries without copying files or sharing Jellyfin credentials. One jellysync node runs alongside each peer's Jellyfin. Nodes exchange catalog metadata; for any item a node doesn't have locally but a peer offers, jellysync writes a `.strm` pointer file into the local Jellyfin library. Playing that item streams bytes through the peer's jellysync node on demand — no bulk copying, no shared Jellyfin login. See README.md for the full user-facing explanation, env var reference, and setup flow.

## Commands

Backend (Go, module `jellysync`, root of repo):
```sh
go build ./...              # build
go run ./cmd/jellysync       # run (needs NODE_ID, JELLYFIN_URL, JELLYFIN_API_KEY, OUTPUT_DIR, BASE_URL set)
go test ./...                 # all tests
go test ./internal/strm/...   # single package
go test ./internal/strm/ -run TestPathForEpisode   # single test
go vet ./...
```

Frontend (Svelte + Vite + Tailwind, in `web/`):
```sh
cd web && npm install
npm run dev      # vite dev server
npm run build    # builds into internal/webui/static, embedded into the Go binary
```
`internal/webui/static/index.html` as checked into git is just a placeholder so `go build` works without Node installed — the real dashboard only exists after `npm run build` (or the Docker build, which does this automatically, see `Dockerfile`).

Full stack: `docker compose up -d` (see README.md for required env vars in `docker-compose.yml`).

The `VERSION` file at repo root is the node's own version, reported on `GET /health` and, in turn, to any peer that probes it. The Docker build stamps it in via `-ldflags -X main.version=$(cat VERSION)`; a plain `go build`/`go run` leaves it as `"dev"` since that ldflag isn't applied.

## Architecture

Single Go binary (`cmd/jellysync/main.go`) wiring together `internal/*` packages plus an embedded Svelte dashboard. No external services besides the node's own local Jellyfin and an embedded SQLite DB (`internal/db`, via `modernc.org/sqlite`, WAL mode).

Main loop, started in `main.go`: `catalog.Sync` then `strm.Reconcile`, every `catalog.SyncInterval` (60s), always in that order in the same cycle — Reconcile reads the table Sync just wrote, so a fresh election is reflected in `.strm` files within the same cycle rather than racing an independent timer.

**internal/jellyfin** — thin client for the node's own local Jellyfin (`ListItems`, `Ping`, `RefreshLibrary`, `NewDownloadRequest`). Computes each item's `GlobalID()`: first available provider ID (`tmdb:`/`tvdb:`/`imdb:`), falling back to a normalized title+year hash (episodes hash series+season+episode instead, since two different shows can share an episode title). This ID is the cross-node dedup key everywhere downstream.

**internal/catalog** — the sync protocol and election logic.
- `LocalEntries`/`Handler` serve this node's own catalog at `GET /api/v1/catalog`, excluding anything under the `.strm` output dir (jellysync's own generated redirects must never be re-offered as if they were real local media — Jellyfin's `/Items` listing can't tell the two apart, so without this exclusion a node would find its own remote-elected item, mark it local, and delete the `.strm` it just wrote, looping forever).
- `Sync` fetches local + every `ONLINE` peer's catalog and applies the dedup rule into `catalog_items`: local always wins; otherwise the peer with the lexicographically smallest ID among those offering the item is elected primary (a pure function of peer IDs — no latency/timing races between nodes).
- `status.go`'s `ItemsHandler` (`GET /api/v1/items`) is the dashboard-facing view of `catalog_items`, distinct from the peer-facing wire format in `catalog.go`.

**internal/strm** — turns `catalog_items` rows into `.strm` files under `OUTPUT_DIR/{movies,series}/<peerID>/...` and back. Series roots never get a `.strm` (only episodes are playable). The path's disambiguator suffix (provider ID, or a truncated hash) is load-bearing, not cosmetic — two items can share a display name and would otherwise silently overwrite each other's file. `Reconcile` also triggers a Jellyfin library refresh iff anything actually changed on disk.

**internal/peers** — tracks configured peers and their reachability. DB is the source of truth after first boot (peers added via the dashboard persist even if never in the `PEERS` env var). Each peer gets its own heartbeat goroutine (`GET /health`, ~15s + jitter) driving `ONLINE → DEGRADED → OFFLINE`; `Sync` only fetches catalogs from `ONLINE` peers. Every probe (add-peer handshake and heartbeat) also reads the `version` field off that peer's `/health` response and caches it on the `Peer`, so `GET /api/v1/peers` can show what jellysync build each peer is running — best-effort only, an older peer without the field just reports an empty version.

**internal/proxy** — streams playback bytes, never exposes an API key off its own host. `GET /api/v1/proxy/stream/{peerID}/{itemID}`: `peerID=local` serves directly from this node's own Jellyfin using its own key (this is what a *peer's* jellysync calls on our behalf); any other `peerID` forwards the request to that peer's own node at `.../stream/local/{itemID}`. A node's own Jellyfin key never leaves its own host, and this node never talks to a peer's Jellyfin directly.

**internal/webui** — `go:embed` of `static/`, the built Svelte dashboard (`web/`). Talks to the `/api/v1/*` endpoints above.

## Security model (v1)

No auth between peer nodes yet — anything reachable at a configured peer address is trusted. The security boundary is a private network (VPN) between peers, not the app. Keep this in mind before adding any feature that assumes peer requests are otherwise verified.
