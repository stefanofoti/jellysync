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

Main loop, started in `main.go` (`syncrun.RunLocal`): `catalog.Sync` then `strm.Reconcile`, on the user-configured sync interval (`internal/settings`, default 2h), always in that order in the same cycle — Reconcile reads the table Sync just wrote, so a fresh election is reflected in `.strm` files within the same cycle rather than racing an independent timer. **jellysync never triggers a Jellyfin library scan** (`/Library/Refresh`) — scanning files on disk is Jellyfin's own business, on any node; new/removed `.strm` files appear in Jellyfin whenever it next scans. Three levels: a *local* sync (`trigger/local`) re-reads this node's Jellyfin index (`GET /Items`) into the catalog; a *remote* sync (`trigger/{peerID}`) just pulls peers' cached catalogs and leaves the peer alone; a *remote sync with force* (`?force=true`, the dashboard's "force peer to refresh its catalog" checkbox) first asks that peer's node to run its local sync, waits for it, then pulls.

**internal/jellyfin** — thin client for the node's own local Jellyfin (`ListItems`, `Ping`, `NewDownloadRequest`). Computes each item's `GlobalID()`: first available provider ID (`tmdb:`/`tvdb:`/`imdb:`), falling back to a normalized title+year hash (episodes hash series+season+episode instead, since two different shows can share an episode title). `ListItems` also drops orphans — items whose `Path` isn't under any library in `/Library/VirtualFolders` (Jellyfin keeps such rows when a library is removed while its folder is unreachable); the filter is skipped if the library list can't be fetched. This ID is the cross-node dedup key everywhere downstream.

**internal/catalog** — the sync protocol and election logic.
- `Index` (`index.go`) is this node's own catalog as served to peers: `LocalEntries` (the Jellyfin listing, excluding anything under the `.strm` output dir — jellysync's own generated redirects must never be re-offered as if they were real local media; Jellyfin's `/Items` listing can't tell the two apart, so without this exclusion a node would find its own remote-elected item, mark it local, and delete the `.strm` it just wrote, looping forever) persisted in `local_catalog` as a revisioned change log. `Refresh` diffs a fresh listing against the table and gives every added/changed/removed row the next rev (removals stay as tombstones for 30 days). An `epoch` (random, in `settings`) identifies the history those revs belong to. Peers are served from this table, never from a live Jellyfin listing.
- Peer protocol: `GET /api/v1/catalog/changes?epoch=&since=&limit=` returns one page of changes after rev `since` (`Feed`); an unknown epoch, a cursor older than the tombstone GC watermark, or one ahead of head yields `reset: true` and restarts from 0. `GET /api/v1/catalog` (full list) is kept for pre-feed peers. `POST /api/v1/catalog/notify` is a data-less hint that a peer's catalog changed; it queues a pull-only sync. Responses are gzip-encoded when accepted.
- `pull.go` mirrors each `ONLINE` peer's feed into `peer_catalog`, concurrently across peers, one transaction per page together with the cursor in `peer_sync_state` (resumable). A reset doesn't drop the mirror up front: the resync is written under a new `gen` and older-gen rows are swept only once caught up, so a peer's items never vanish mid-resync. A 404 on the feed falls back to the legacy full fetch.
- `Sync` = optional `Index.Refresh` → pull peers → elect over `local_catalog` + mirrors of `ONLINE` peers → write only the rows that differ into `catalog_items`, in one transaction. Dedup rule: local always wins; otherwise the peer with the lexicographically smallest ID among those offering the item is elected primary (a pure function of peer IDs — no latency/timing races between nodes). An ONLINE peer whose pull failed keeps its last-known mirror in the election. `peer_sync_state.synced_at` is set only when a peer's whole pull succeeds; `catalog.LastSyncTimes` exposes it as `last_sync_at` on `GET /api/v1/peers` (injected into `peers.ListHandler`, since catalog imports peers), shown on the dashboard as "synced N min ago". `syncrun.RunLocal` calls `NotifyPeers` whenever the local refresh found changes; a notified node runs a pull-only sync (no local refresh), so notifications can't ping-pong. `syncrun.Queue` coalesces triggers/notifications into one pending run.
- `status.go`'s `ItemsHandler` (`GET /api/v1/items`) is the dashboard-facing view of `catalog_items`, distinct from the peer-facing wire format in `catalog.go`.

**internal/strm** — turns `catalog_items` rows into `.strm` files under `OUTPUT_DIR/{movies,series}/<peerID>/...` and back. Series roots never get a `.strm` (only episodes are playable). The path's disambiguator suffix (provider ID, or a truncated hash) is load-bearing, not cosmetic — two items can share a display name and would otherwise silently overwrite each other's file. `Reconcile` never asks Jellyfin to rescan.

**internal/peers** — tracks configured peers and their reachability. DB is the source of truth after first boot (peers added via the dashboard persist even if never in the `PEERS` env var). Each peer gets its own heartbeat goroutine (`GET /health`, ~15s + jitter) driving `ONLINE → DEGRADED → OFFLINE`; `Sync` only fetches catalogs from `ONLINE` peers. Every probe (add-peer handshake and heartbeat) also reads the `version` field off that peer's `/health` response and caches it on the `Peer`, so `GET /api/v1/peers` can show what jellysync build each peer is running — best-effort only, an older peer without the field just reports an empty version. Each heartbeat also resolves the peer URL's host to an IP (`Peer.IP`, not persisted), shown on the dashboard and used as a fallback by `ResolveCaller`.

**internal/proxy** — streams playback bytes, never exposes an API key off its own host. `GET /api/v1/proxy/stream/{peerID}/{itemID}`: `peerID=local` serves directly from this node's own Jellyfin using its own key (this is what a *peer's* jellysync calls on our behalf); any other `peerID` forwards the request to that peer's own node at `.../stream/local/{itemID}`. A node's own Jellyfin key never leaves its own host, and this node never talks to a peer's Jellyfin directly. Outgoing (`local`) traffic is attributed via `ResolveCaller`: the caller's `X-Jellysync-Peer` header is its own `NODE_ID`, i.e. our peer's *Name*, not the id we assigned it, so it must be mapped back to the registry id (falling back to source IP) — otherwise one peer's traffic splits across two keys. `/api/v1/traffic` and `/metrics` also fold totals persisted under a peer's name by older builds into its id.

**internal/webui** — `go:embed` of `static/`, the built Svelte dashboard (`web/`). Talks to the `/api/v1/*` endpoints above.

## Logging

Everything logs through `log/slog` (text handler, `key=value` attributes), configured once in `main.go` from `LOG_LEVEL` (`internal/logging`, default DEBUG). Conventions: errors go in an `err` attribute (`logging.Err(err)`), durations are rounded to ms. INFO is one summary line per sync step (`sync started`/`sync finished` carry a `reason`: startup, scheduled, manual, peer-notify), plus peer state changes, notifications and config changes; DEBUG is per page/file/stream detail; WARN is anything degraded that the node recovers from (a failed peer pull, an unreachable peer); ERROR is a failed step. `catalog.Sync` and `strm.Reconcile` return stats structs (`SyncStats`, `strm.Stats`) that `syncrun.RunLocal` turns into its summary lines. Don't add per-heartbeat or per-dashboard-poll logs above DEBUG.

## Security model (v1)

No auth between peer nodes yet — anything reachable at a configured peer address is trusted. The security boundary is a private network (VPN) between peers, not the app. Keep this in mind before adding any feature that assumes peer requests are otherwise verified.
