<script>
  let health = $state(null);
  let peers = $state([]);
  let stats = $state([]);
  let traffic = $state({});
  let error = $state("");

  const ITEMS_PAGE_SIZE = 50;

  // Movies and series are fetched as separate paginated pages against
  // GET /api/v1/items?type=movie|series&limit=&offset=, rather than the
  // single flat /api/v1/items array — a library can be too big to fetch
  // in one shot. Both pages are kept loaded (not just the active tab's) so
  // the source filter dropdown below and tab-switching don't need their own
  // fetch/loading-state dance.
  let moviePage = $state({ items: [], total: 0, limit: ITEMS_PAGE_SIZE, offset: 0 });
  let seriesPage = $state({ items: [], total: 0, limit: ITEMS_PAGE_SIZE, offset: 0 });

  let newPeerUrl = $state("");
  let addingPeer = $state(false);

  let settingsOpen = $state(false);
  const syncIntervalOptions = [0.5, 1, 2, 4, 6, 8, 12];
  let syncIntervalHours = $state(2);
  let savingInterval = $state(false);
  let syncStatus = $state({}); // peer id ("local" included) -> {state, percent, started_at, finished_at, error}
  let triggering = $state(new Set()); // scopes currently mid-request: "local" | "remote" | "both"
  let forceScan = $state(false); // remote sync only: first ask the peer to refresh its catalog from its own Jellyfin, instead of pulling its cached one

  // Source filtering happens server-side (the `owners` param), before
  // pagination, so every page is full and the series tab is filtered too —
  // series summary rows carry no per-episode ownership to filter by here.
  // Only sent while some source is unchecked, so an owner the dropdown
  // doesn't list yet is never hidden by accident.
  async function fetchItemsPage(type, offset, limit) {
    const owners = filterSources.every((id) => enabledPeers.has(id))
      ? ""
      : `&owners=${[...enabledPeers].map(encodeURIComponent).join(",")}`;
    const res = await fetch(`/api/v1/items?type=${type}&limit=${limit}&offset=${offset}${owners}`);
    if (!res.ok) throw new Error(await res.text());
    return res.json();
  }

  // Only the latest request per tab may land: a periodic refresh started
  // before a filter toggle must not overwrite the page the toggle loaded.
  let movieSeq = 0;
  let seriesSeq = 0;

  async function loadMovies(offset = moviePage.offset) {
    const seq = ++movieSeq;
    const page = await fetchItemsPage("movie", offset, moviePage.limit);
    if (seq === movieSeq) moviePage = page;
  }

  async function loadSeries(offset = seriesPage.offset) {
    const seq = ++seriesSeq;
    const page = await fetchItemsPage("series", offset, seriesPage.limit);
    if (seq === seriesSeq) seriesPage = page;
  }

  function goToPage(type, offset) {
    if (type === "movies") loadMovies(offset);
    else loadSeries(offset);
  }

  function pagerInfo(page) {
    return {
      from: page.total === 0 ? 0 : page.offset + 1,
      to: Math.min(page.offset + page.limit, page.total),
      hasPrev: page.offset > 0,
      hasNext: page.offset + page.limit < page.total,
    };
  }

  async function refresh() {
    try {
      const [healthRes, peersRes, statsRes, trafficRes, syncStatusRes] = await Promise.all([
        fetch("/health"),
        fetch("/api/v1/peers"),
        fetch("/api/v1/stats"),
        fetch("/api/v1/traffic"),
        fetch("/api/v1/sync/status"),
      ]);
      health = await healthRes.json();
      peers = await peersRes.json();
      stats = await statsRes.json();
      traffic = await trafficRes.json();
      syncStatus = await syncStatusRes.json();
      syncFilterSources();
      await Promise.all([loadMovies(moviePage.offset), loadSeries(seriesPage.offset)]);
      error = "";
    } catch (e) {
      error = String(e);
    }
  }

  async function addPeer() {
    if (!newPeerUrl) return;
    addingPeer = true;
    try {
      const res = await fetch("/api/v1/peers", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ url: newPeerUrl }),
      });
      if (res.ok) {
        newPeerUrl = "";
        error = "";
        await refresh();
      } else {
        error = await res.text();
      }
    } finally {
      addingPeer = false;
    }
  }

  async function removePeer(id) {
    const res = await fetch(`/api/v1/peers/${encodeURIComponent(id)}`, { method: "DELETE" });
    if (res.ok) {
      await refresh();
    } else {
      error = await res.text();
    }
  }

  async function clearTraffic() {
    if (!confirm("Clear all traffic statistics? Totals and history for every peer are reset to zero.")) return;
    const res = await fetch("/api/v1/traffic", { method: "DELETE" });
    if (res.ok) {
      await refresh();
    } else {
      error = await res.text();
    }
  }

  // ---- Settings modal: interval + manual sync -------------------------
  async function openSettings() {
    settingsOpen = true;
    try {
      const res = await fetch("/api/v1/settings");
      if (!res.ok) throw new Error(await res.text());
      const settings = await res.json();
      syncIntervalHours = Math.round((settings.sync_interval_seconds / 3600) * 100) / 100;
    } catch (e) {
      error = String(e);
    }
  }

  async function saveInterval() {
    if (!(syncIntervalHours > 0)) return;
    savingInterval = true;
    try {
      const res = await fetch("/api/v1/settings", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sync_interval_seconds: Math.round(syncIntervalHours * 3600) }),
      });
      if (!res.ok) throw new Error(await res.text());
      error = "";
    } catch (e) {
      error = String(e);
    } finally {
      savingInterval = false;
    }
  }

  // scope is "local", "remote", or "both". "remote" and "both" trigger every
  // configured peer individually, so one unreachable peer never blocks the
  // others — each gets its own status row regardless.
  async function triggerSync(scope) {
    const targets = [];
    if (scope === "local" || scope === "both") targets.push("local");
    if (scope === "remote" || scope === "both") targets.push(...peers.map((p) => p.id));
    if (targets.length === 0) return;

    triggering = new Set([...triggering, scope]);
    try {
      const suffix = forceScan ? "?force=true" : "";
      await Promise.all(
        targets.map((id) => fetch(`/api/v1/sync/trigger/${encodeURIComponent(id)}${suffix}`, { method: "POST" })),
      );
      await refresh();
    } catch (e) {
      error = String(e);
    } finally {
      const next = new Set(triggering);
      next.delete(scope);
      triggering = next;
    }
  }

  function syncStateColor(state) {
    if (state === "running") return "bg-blue-500/20 text-blue-400";
    if (state === "success") return "bg-green-500/20 text-green-400";
    if (state === "error" || state === "unreachable") return "bg-red-500/20 text-red-400";
    return "bg-neutral-700 text-neutral-400";
  }

  function syncBarColor(state) {
    if (state === "running") return "bg-blue-500";
    if (state === "success") return "bg-green-500";
    if (state === "error" || state === "unreachable") return "bg-red-500";
    return "bg-neutral-700";
  }

  // Only older peers (which reported a Jellyfin scan percentage) or a missing
  // stage carry a percent; current stages have none to show — an
  // indeterminate bar communicates "still working" without faking a number.
  function syncShowsPercent(status) {
    return !status.stage || status.stage === "scanning";
  }

  function syncStageLabel(stage) {
    if (stage === "scanning") return "scanning library";
    if (stage === "syncing_catalog") return "syncing catalog";
    if (stage === "writing_files") return "writing files";
    return "";
  }

  // ---- Library + traffic stats ---------------------------------------
  function formatBytes(n) {
    if (!n) return "0 B";
    const units = ["B", "KB", "MB", "GB", "TB"];
    let i = 0;
    let v = n;
    while (v >= 1024 && i < units.length - 1) {
      v /= 1024;
      i++;
    }
    return `${v.toFixed(v < 10 && i > 0 ? 1 : 0)} ${units[i]}`;
  }

  function formatBitrate(bytesPerSec) {
    return `${formatBytes(bytesPerSec)}/s`;
  }

  // ---- Peer last sync ------------------------------------------------
  // last_sync_at is when this node last pulled the peer's catalog in full;
  // absent if it never has. Re-rendered on every 5s refresh, which is
  // plenty of resolution for a "N min ago" label.
  function formatAgo(iso) {
    const seconds = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 1000));
    if (seconds < 60) return "just now";
    const minutes = Math.floor(seconds / 60);
    if (minutes < 60) return `${minutes} min ago`;
    const hours = Math.floor(minutes / 60);
    if (hours < 48) return `${hours} h ${minutes % 60} min ago`;
    return `${Math.floor(hours / 24)} days ago`;
  }

  function lastSyncLabel(peer) {
    return peer.last_sync_at ? `synced ${formatAgo(peer.last_sync_at)}` : "never synced";
  }

  function lastSyncTitle(peer) {
    return peer.last_sync_at
      ? `Last successful catalog sync: ${new Date(peer.last_sync_at).toLocaleString()}`
      : "No successful catalog sync with this peer yet";
  }

  let libraryTotal = $derived(
    stats.reduce(
      (acc, s) => ({ movies: acc.movies + s.movies, series: acc.series + s.series, episodes: acc.episodes + s.episodes }),
      { movies: 0, series: 0, episodes: 0 },
    ),
  );

  let trafficPeerIds = $derived(Object.keys(traffic).sort((a, b) => peerLabel(a).localeCompare(peerLabel(b))));

  // "local" first, then every configured peer — the fixed set of rows the
  // settings modal shows a sync status bar for.
  let syncTargets = $derived([{ id: "local" }, ...peers.map((p) => ({ id: p.id }))]);

  // Builds an SVG polyline points attribute for a peer's in+out bitrate
  // history, scaled to a fixed 200x40 viewbox. Empty/flat history still
  // renders a flat line at the bottom rather than an empty chart.
  function sparklinePoints(history, key) {
    if (!history || history.length === 0) return "";
    const values = history.map((s) => s[key] ?? 0);
    const max = Math.max(1, ...values);
    const w = 200;
    const h = 40;
    const step = history.length > 1 ? w / (history.length - 1) : 0;
    return values.map((v, i) => `${(i * step).toFixed(1)},${(h - (v / max) * h).toFixed(1)}`).join(" ");
  }

  // Owner/peer keys everywhere in the API ("local", a peer's registry id,
  // or — for traffic from a caller we couldn't match — a raw node id or
  // "unknown") mapped to what the dashboard should show instead.
  let peersById = $derived(new Map(peers.map((p) => [p.id, p])));

  function peerLabel(id) {
    if (id === "local") return "local";
    const peer = peersById.get(id);
    if (peer) return peer.name || shortId(peer.id);
    return id;
  }

  function peerIp(id) {
    return peersById.get(id)?.ip ?? "";
  }

  function shortId(id) {
    return id.length > 8 ? `${id.slice(0, 8)}…` : id;
  }

  function stateColor(state) {
    if (state === "ONLINE") return "bg-green-500/20 text-green-400";
    if (state === "DEGRADED") return "bg-yellow-500/20 text-yellow-400";
    return "bg-red-500/20 text-red-400";
  }

  let expandedSeries = $state(new Set());

  // Episodes for one series are fetched lazily, the first time its row is
  // expanded, from GET /api/v1/items/series/episodes?key= — the paginated
  // series list itself only carries per-series counts (see servePagedSeries
  // in internal/catalog/status.go), not episode rows. Keyed by series_key;
  // {status: "loading"|"loaded"|"error", episodes}.
  let seriesEpisodesCache = $state(new Map());

  async function ensureSeriesEpisodesLoaded(key) {
    const entry = seriesEpisodesCache.get(key);
    if (entry && (entry.status === "loaded" || entry.status === "loading")) return;

    const loading = new Map(seriesEpisodesCache);
    loading.set(key, { status: "loading", episodes: [] });
    seriesEpisodesCache = loading;

    try {
      const res = await fetch(`/api/v1/items/series/episodes?key=${encodeURIComponent(key)}`);
      if (!res.ok) throw new Error(await res.text());
      const episodes = await res.json();
      const loaded = new Map(seriesEpisodesCache);
      loaded.set(key, { status: "loaded", episodes });
      seriesEpisodesCache = loaded;
    } catch (e) {
      const failed = new Map(seriesEpisodesCache);
      failed.set(key, { status: "error", episodes: [], error: String(e) });
      seriesEpisodesCache = failed;
    }
  }

  function toggleSeries(key) {
    const next = new Set(expandedSeries);
    if (next.has(key)) {
      next.delete(key);
    } else {
      next.add(key);
      ensureSeriesEpisodesLoaded(key);
    }
    expandedSeries = next;
  }

  // Groups one series' cached episodes into seasons, filtered to the
  // currently enabled peers. Reads seriesEpisodesCache/enabledPeers directly
  // so Svelte re-runs it wherever it's called in the template as either
  // changes. Series rows themselves are filtered by the backend (see
  // fetchItemsPage); this applies the same filter to the episode list.
  function seasonsFor(key) {
    const entry = seriesEpisodesCache.get(key);
    if (!entry || entry.status !== "loaded") return [];
    const bySeason = new Map();
    for (const episode of entry.episodes) {
      if (!enabledPeers.has(ownerOf(episode))) continue;
      const number = episode.season_number ?? 0;
      let list = bySeason.get(number);
      if (!list) {
        list = [];
        bySeason.set(number, list);
      }
      list.push(episode);
    }
    return [...bySeason.entries()]
      .sort((a, b) => a[0] - b[0])
      .map(([number, episodes]) => ({
        number,
        episodes: episodes.sort((a, b) => (a.episode_number ?? 0) - (b.episode_number ?? 0)),
      }));
  }

  function seasonLabel(number) {
    return number === 0 ? "Specials" : `Season ${number}`;
  }

  function episodeLabel(item) {
    const season = String(item.season_number ?? 0).padStart(2, "0");
    const episode = String(item.episode_number ?? 0).padStart(2, "0");
    return `S${season}E${episode} · ${item.name}`;
  }

  // "local" or the offering peer's id — the same key used both for the
  // filter dropdown and to decide whether an item passes the current
  // filter.
  function ownerOf(item) {
    return item.local ? "local" : item.primary_peer_id;
  }

  // ---- Peer filter -------------------------------------------------
  // enabledPeers holds the set of owners ("local" or a peer id) whose
  // items should be shown. New sources (a peer just added, or one still
  // referenced by an item after being removed) default to enabled so the
  // filter never silently hides something the user hasn't chosen to hide.
  let enabledPeers = $state(new Set(["local"]));
  let filterOpen = $state(false);

  // Built from configured peers plus every owner in the library stats, which
  // cover the whole catalog (not just the loaded page) — so a peer that was
  // removed but still owns items stays filterable. "unknown" (an item with no
  // primary peer) isn't an owner the `owners` param can select, so it's left
  // out.
  let filterSources = $derived.by(() => {
    const set = new Set(["local"]);
    for (const p of peers) set.add(p.id);
    for (const s of stats) {
      if (s.owner !== "unknown") set.add(s.owner);
    }
    return [...set].sort((a, b) => (a === "local" ? -1 : b === "local" ? 1 : peerLabel(a).localeCompare(peerLabel(b))));
  });

  // Sources the filter has already offered. Only a source seen for the
  // first time gets auto-enabled — one the user unchecked must stay unchecked
  // across refreshes, not be switched back on every 5s.
  let seenSources = new Set(["local"]);

  function syncFilterSources() {
    const next = new Set(enabledPeers);
    let changed = false;
    for (const id of filterSources) {
      if (seenSources.has(id)) continue;
      seenSources.add(id);
      next.add(id);
      changed = true;
    }
    if (changed) enabledPeers = next;
  }

  function toggleFilterSource(id) {
    const next = new Set(enabledPeers);
    if (next.has(id)) {
      next.delete(id);
    } else {
      next.add(id);
    }
    enabledPeers = next;
    // The filter changes what's on every page, so the current offsets no
    // longer mean anything — start both tabs over from the first page.
    Promise.all([loadMovies(0), loadSeries(0)]).catch((e) => (error = String(e)));
  }

  // ---- Movies/series tabs -------------------------------------------
  let activeTab = $state("movies");

  // Movies need no grouping — each row in moviePage.items is already one
  // list entry, in the order the backend paginated (and source-filtered)
  // them, by name.
  let movieRows = $derived(
    moviePage.items.map((item) => ({ type: "movie", key: item.global_id, name: item.name, item })),
  );

  // seriesPage.items is already one summary entry per series (series_key,
  // series_name, local_count, total_count — see servePagedSeries), in the
  // order the backend paginated them by name. Already source-filtered by the
  // backend: a series is listed iff one of its episodes comes from an enabled
  // source, and its counts cover only those episodes — the same set its
  // expanded episode list shows (see seasonsFor).
  let seriesRows = $derived(
    seriesPage.items.map((item) => ({
      type: "series",
      key: item.series_key,
      name: item.series_name || "Unknown series",
      localCount: item.local_count,
      totalCount: item.total_count,
    })),
  );

  // ---- Per-item reachability test + mini player ----------------------
  let testResults = $state({}); // global_id -> {status: "testing"|"ok"|"fail", detail}
  let playingKey = $state(null); // global_id of the item whose player is open

  function playable(item) {
    return Boolean(item.stream_peer_id && item.stream_item_id);
  }

  function streamUrl(item) {
    return `/api/v1/proxy/stream/${encodeURIComponent(item.stream_peer_id)}/${encodeURIComponent(item.stream_item_id)}`;
  }

  async function testItem(item) {
    testResults = { ...testResults, [item.global_id]: { status: "testing" } };
    try {
      const res = await fetch(streamUrl(item), { headers: { Range: "bytes=0-1" } });
      if (res.ok) {
        testResults = { ...testResults, [item.global_id]: { status: "ok", detail: `HTTP ${res.status}` } };
      } else {
        testResults = { ...testResults, [item.global_id]: { status: "fail", detail: `HTTP ${res.status}` } };
      }
    } catch (e) {
      testResults = { ...testResults, [item.global_id]: { status: "fail", detail: String(e) } };
    }
  }

  function togglePlayer(item) {
    playingKey = playingKey === item.global_id ? null : item.global_id;
  }

  function testBadgeColor(status) {
    if (status === "ok") return "text-green-400";
    if (status === "fail") return "text-red-400";
    return "text-neutral-500";
  }

  $effect(() => {
    refresh();
    const interval = setInterval(refresh, 5000);
    return () => clearInterval(interval);
  });
</script>

<div class="min-h-screen bg-neutral-950 text-neutral-200 px-3 py-4 sm:p-6 font-sans overflow-x-hidden">
  <div class="max-w-4xl mx-auto space-y-4 sm:space-y-6">
    <header class="flex flex-wrap items-center justify-between gap-2">
      <h1 class="text-xl font-semibold text-neutral-100">jellysync</h1>
      <div class="flex flex-wrap items-center justify-end gap-x-3 gap-y-1 min-w-0">
        {#if health}
          <span class="text-xs sm:text-sm text-neutral-500">
            node <span class="text-neutral-300">{health.node_id}</span>
            · <span class="font-mono text-neutral-400">v{health.version}</span>
            · up {health.uptime_seconds}s
          </span>
        {/if}
        <button
          class="rounded border border-neutral-700 px-3 py-1 text-sm text-neutral-400 hover:text-neutral-200"
          onclick={openSettings}
        >
          Settings
        </button>
      </div>
    </header>

    {#if error}
      <div class="rounded border border-red-800 bg-red-950/50 text-red-300 text-sm px-3 py-2 break-words">{error}</div>
    {/if}

    <div class="grid grid-cols-1 md:grid-cols-2 gap-4 sm:gap-6">
      <section class="rounded border border-neutral-800 bg-neutral-900/50 min-w-0">
        <h2 class="text-sm font-medium text-neutral-400 px-4 py-3 border-b border-neutral-800">Library</h2>
        <table class="w-full text-sm">
          <thead>
            <tr class="text-neutral-500 text-xs">
              <td class="px-4 py-1">source</td>
              <td class="px-4 py-1">movies</td>
              <td class="px-4 py-1">series</td>
              <td class="px-4 py-1">episodes</td>
            </tr>
          </thead>
          <tbody>
            {#each stats as s (s.owner)}
              <tr class="border-b border-neutral-800/60 last:border-0">
                <td class="px-4 py-2 break-all">{peerLabel(s.owner)}</td>
                <td class="px-4 py-2">{s.movies}</td>
                <td class="px-4 py-2">{s.series}</td>
                <td class="px-4 py-2">{s.episodes}</td>
              </tr>
            {:else}
              <tr><td class="px-4 py-3 text-neutral-500" colspan="4">No items yet.</td></tr>
            {/each}
          </tbody>
          {#if stats.length > 0}
            <tfoot>
              <tr class="border-t border-neutral-800 font-medium text-neutral-200">
                <td class="px-4 py-2">total</td>
                <td class="px-4 py-2">{libraryTotal.movies}</td>
                <td class="px-4 py-2">{libraryTotal.series}</td>
                <td class="px-4 py-2">{libraryTotal.episodes}</td>
              </tr>
            </tfoot>
          {/if}
        </table>
      </section>

      <section class="rounded border border-neutral-800 bg-neutral-900/50 min-w-0">
        <h2 class="text-sm font-medium text-neutral-400 px-4 py-3 border-b border-neutral-800">Peers</h2>
        <table class="w-full text-sm">
          <tbody>
            {#each peers as peer (peer.id)}
              <tr class="border-b border-neutral-800/60 last:border-0">
                <td class="px-4 py-2 min-w-0 max-w-0 w-full">
                  <div class="truncate">{peer.name || "(unnamed)"}</div>
                  <div class="text-neutral-500 font-mono text-xs truncate" title={peer.url}>
                    {peer.ip || "IP unknown"}<span class="text-neutral-600">&nbsp;· {peer.url}</span>
                  </div>
                </td>
                <td class="px-4 py-2 text-right whitespace-nowrap">
                  <span class={`rounded px-2 py-0.5 text-xs ${stateColor(peer.state)}`}>{peer.state}</span>
                  <div class="text-neutral-500 text-xs mt-1" title={lastSyncTitle(peer)}>{lastSyncLabel(peer)}</div>
                </td>
              </tr>
            {:else}
              <tr><td class="px-4 py-3 text-neutral-500" colspan="2">No peers configured.</td></tr>
            {/each}
          </tbody>
        </table>
      </section>
    </div>

    <section class="rounded border border-neutral-800 bg-neutral-900/50 min-w-0">
      <div class="flex items-center justify-between px-4 py-3 border-b border-neutral-800">
        <h2 class="text-sm font-medium text-neutral-400">Traffic</h2>
        {#if trafficPeerIds.length > 0}
          <button class="text-neutral-500 hover:text-red-400 text-xs" onclick={clearTraffic}>clear</button>
        {/if}
      </div>
      <table class="w-full text-sm">
        <tbody>
          {#each trafficPeerIds as peerId (peerId)}
            <tr class="border-b border-neutral-800/60 last:border-0">
              <td class="px-3 sm:px-4 py-2 align-top max-w-0 w-full">
                <div class="truncate" title={peerId}>{peerLabel(peerId)}</div>
                {#if peerIp(peerId)}
                  <div class="text-neutral-600 font-mono text-xs truncate">{peerIp(peerId)}</div>
                {:else if !peersById.has(peerId)}
                  <div class="text-neutral-600 text-xs">not a configured peer</div>
                {/if}
              </td>
              <td class="px-3 sm:px-4 py-2 align-top whitespace-nowrap">
                <div class="text-neutral-500 text-xs">in</div>
                <div>{formatBitrate(traffic[peerId].current_in_bps)}</div>
                <div class="text-neutral-600 text-xs">{formatBytes(traffic[peerId].total_in_bytes)} total</div>
              </td>
              <td class="px-3 sm:px-4 py-2 align-top whitespace-nowrap">
                <div class="text-neutral-500 text-xs">out</div>
                <div>{formatBitrate(traffic[peerId].current_out_bps)}</div>
                <div class="text-neutral-600 text-xs">{formatBytes(traffic[peerId].total_out_bytes)} total</div>
              </td>
              <td class="px-4 py-2 align-top hidden sm:table-cell">
                <svg viewBox="0 0 200 40" class="w-40 h-10">
                  <polyline
                    points={sparklinePoints(traffic[peerId].history, "in_bps")}
                    fill="none"
                    stroke="#60a5fa"
                    stroke-width="1.5"
                  />
                  <polyline
                    points={sparklinePoints(traffic[peerId].history, "out_bps")}
                    fill="none"
                    stroke="#c084fc"
                    stroke-width="1.5"
                  />
                </svg>
              </td>
            </tr>
          {:else}
            <tr><td class="px-4 py-3 text-neutral-500" colspan="4">No traffic yet.</td></tr>
          {/each}
        </tbody>
      </table>
    </section>

    <section class="rounded border border-neutral-800 bg-neutral-900/50 min-w-0">
      <div class="flex items-center justify-between gap-2 px-3 sm:px-4 py-3 border-b border-neutral-800">
        <div class="flex items-center gap-1">
          <button
            class={`rounded px-3 py-1 text-sm ${activeTab === "movies" ? "bg-neutral-700 text-neutral-100" : "text-neutral-500 hover:text-neutral-300"}`}
            onclick={() => (activeTab = "movies")}
          >
            Movies
          </button>
          <button
            class={`rounded px-3 py-1 text-sm ${activeTab === "series" ? "bg-neutral-700 text-neutral-100" : "text-neutral-500 hover:text-neutral-300"}`}
            onclick={() => (activeTab = "series")}
          >
            Series
          </button>
        </div>

        <div class="relative">
          <button
            class="rounded border border-neutral-700 px-3 py-1 text-sm text-neutral-400 hover:text-neutral-200"
            onclick={() => (filterOpen = !filterOpen)}
          >
            Sources ({enabledPeers.size}/{filterSources.length}) ▾
          </button>
          {#if filterOpen}
            <div class="absolute right-0 mt-1 w-56 max-w-[calc(100vw-2rem)] rounded border border-neutral-700 bg-neutral-900 shadow-lg z-10 py-1">
              {#each filterSources as id (id)}
                <label class="flex items-center gap-2 px-3 py-1.5 text-sm hover:bg-neutral-800 cursor-pointer">
                  <input type="checkbox" checked={enabledPeers.has(id)} onchange={() => toggleFilterSource(id)} />
                  <span class="min-w-0">
                    <span class="block truncate">{peerLabel(id)}</span>
                    {#if peerIp(id)}
                      <span class="block text-neutral-600 font-mono text-xs truncate">{peerIp(id)}</span>
                    {/if}
                  </span>
                </label>
              {/each}
            </div>
          {/if}
        </div>
      </div>

      <div class="overflow-x-auto">
      <table class="w-full text-sm">
        <tbody>
          {#if activeTab === "movies"}
            {#each movieRows as row (row.key)}
              <tr class="border-b border-neutral-800/60 last:border-0">
                <td class="px-3 sm:px-4 py-2 break-words">{row.item.name}</td>
                <td class="px-4 py-2 text-neutral-500 hidden sm:table-cell">movie</td>
                <td class="px-4 py-2">
                  {#if row.item.local}
                    <span class="rounded px-2 py-0.5 text-xs bg-blue-500/20 text-blue-400">local</span>
                  {:else}
                    <span class="inline-block max-w-[10rem] truncate align-middle whitespace-nowrap rounded px-2 py-0.5 text-xs bg-purple-500/20 text-purple-400">
                      remote · {peerLabel(row.item.primary_peer_id)}
                    </span>
                  {/if}
                </td>
                <td class="px-4 py-2 text-right whitespace-nowrap">
                  {#if playable(row.item)}
                    <button class="text-neutral-500 hover:text-neutral-200 text-xs mr-2" onclick={() => testItem(row.item)}>
                      test
                    </button>
                    {#if testResults[row.item.global_id]}
                      <span class={`text-xs mr-2 ${testBadgeColor(testResults[row.item.global_id].status)}`}>
                        {testResults[row.item.global_id].status === "testing" ? "…" : testResults[row.item.global_id].detail}
                      </span>
                    {/if}
                    <button class="text-neutral-500 hover:text-neutral-200 text-xs" onclick={() => togglePlayer(row.item)}>
                      {playingKey === row.item.global_id ? "close" : "play"}
                    </button>
                  {/if}
                </td>
              </tr>
              {#if playingKey === row.item.global_id}
                <tr class="border-b border-neutral-800/60 last:border-0 bg-neutral-900/30">
                  <td class="px-4 py-3" colspan="4">
                    <!-- svelte-ignore a11y_media_has_caption -->
                    <video controls preload="none" class="w-full max-h-80 rounded bg-black" src={streamUrl(row.item)}></video>
                  </td>
                </tr>
              {/if}
            {:else}
              <tr><td class="px-4 py-3 text-neutral-500" colspan="4">No movies match the current filter.</td></tr>
            {/each}
          {:else}
            {#each seriesRows as row (row.key)}
              <tr
                class="border-b border-neutral-800/60 last:border-0 cursor-pointer hover:bg-neutral-800/40"
                onclick={() => toggleSeries(row.key)}
              >
                <td class="px-3 sm:px-4 py-2 break-words">
                  <span class="inline-block w-4 text-neutral-500">{expandedSeries.has(row.key) ? "▾" : "▸"}</span>
                  {row.name}
                </td>
                <td class="px-4 py-2 text-neutral-500 hidden sm:table-cell">series</td>
                <td class="px-4 py-2">
                  {#if row.localCount === row.totalCount}
                    <span class="rounded px-2 py-0.5 text-xs bg-blue-500/20 text-blue-400">local</span>
                  {:else if row.localCount === 0}
                    <span class="inline-block max-w-[10rem] truncate align-middle whitespace-nowrap rounded px-2 py-0.5 text-xs bg-purple-500/20 text-purple-400">remote</span>
                  {:else}
                    <span class="rounded px-2 py-0.5 text-xs bg-neutral-700 text-neutral-300">mixed</span>
                  {/if}
                  <span class="text-neutral-600 text-xs ml-1">{row.localCount}/{row.totalCount} local</span>
                </td>
                <td class="px-4 py-2"></td>
              </tr>
              {#if expandedSeries.has(row.key)}
                {@const cacheEntry = seriesEpisodesCache.get(row.key)}
                {#if cacheEntry?.status === "loading"}
                  <tr class="border-b border-neutral-800/60 last:border-0 bg-neutral-900/30">
                    <td class="px-4 py-2 pl-9 text-neutral-500 text-xs" colspan="4">Loading episodes…</td>
                  </tr>
                {:else if cacheEntry?.status === "error"}
                  <tr class="border-b border-neutral-800/60 last:border-0 bg-neutral-900/30">
                    <td class="px-4 py-2 pl-9 text-red-400 text-xs" colspan="4">Failed to load episodes: {cacheEntry.error}</td>
                  </tr>
                {/if}
                {#each seasonsFor(row.key) as season (season.number)}
                  <tr class="border-b border-neutral-800/60 last:border-0 bg-neutral-900/30">
                    <td class="px-4 py-1.5 pl-9 text-neutral-500 text-xs" colspan="4">{seasonLabel(season.number)}</td>
                  </tr>
                  {#each season.episodes as episode (episode.global_id)}
                    <tr class="border-b border-neutral-800/60 last:border-0">
                      <td class="px-3 sm:px-4 py-2 pl-8 sm:pl-14 text-neutral-300 break-words">{episodeLabel(episode)}</td>
                      <td class="px-4 py-2 text-neutral-500 hidden sm:table-cell">episode</td>
                      <td class="px-4 py-2">
                        {#if episode.local}
                          <span class="rounded px-2 py-0.5 text-xs bg-blue-500/20 text-blue-400">local</span>
                        {:else}
                          <span class="inline-block max-w-[10rem] truncate align-middle whitespace-nowrap rounded px-2 py-0.5 text-xs bg-purple-500/20 text-purple-400">
                            remote · {peerLabel(episode.primary_peer_id)}
                          </span>
                        {/if}
                      </td>
                      <td class="px-4 py-2 text-right whitespace-nowrap">
                        {#if playable(episode)}
                          <button class="text-neutral-500 hover:text-neutral-200 text-xs mr-2" onclick={() => testItem(episode)}>
                            test
                          </button>
                          {#if testResults[episode.global_id]}
                            <span class={`text-xs mr-2 ${testBadgeColor(testResults[episode.global_id].status)}`}>
                              {testResults[episode.global_id].status === "testing" ? "…" : testResults[episode.global_id].detail}
                            </span>
                          {/if}
                          <button class="text-neutral-500 hover:text-neutral-200 text-xs" onclick={() => togglePlayer(episode)}>
                            {playingKey === episode.global_id ? "close" : "play"}
                          </button>
                        {/if}
                      </td>
                    </tr>
                    {#if playingKey === episode.global_id}
                      <tr class="border-b border-neutral-800/60 last:border-0 bg-neutral-900/30">
                        <td class="px-4 py-3" colspan="4">
                          <!-- svelte-ignore a11y_media_has_caption -->
                          <video controls preload="none" class="w-full max-h-80 rounded bg-black" src={streamUrl(episode)}></video>
                        </td>
                      </tr>
                    {/if}
                  {/each}
                {/each}
              {/if}
            {:else}
              <tr><td class="px-4 py-3 text-neutral-500" colspan="4">No series match the current filter.</td></tr>
            {/each}
          {/if}
        </tbody>
      </table>
      </div>

      {#if activeTab === "movies"}
        {@const p = pagerInfo(moviePage)}
        <div class="flex items-center justify-between gap-2 px-3 sm:px-4 py-2 border-t border-neutral-800 text-xs text-neutral-500">
          <span>{p.from}–{p.to} of {moviePage.total} movies</span>
          <div class="flex gap-2">
            <button
              class="rounded border border-neutral-700 px-2 py-1 disabled:opacity-40 hover:text-neutral-200"
              disabled={!p.hasPrev}
              onclick={() => goToPage("movies", moviePage.offset - moviePage.limit)}
            >
              prev
            </button>
            <button
              class="rounded border border-neutral-700 px-2 py-1 disabled:opacity-40 hover:text-neutral-200"
              disabled={!p.hasNext}
              onclick={() => goToPage("movies", moviePage.offset + moviePage.limit)}
            >
              next
            </button>
          </div>
        </div>
      {:else}
        {@const p = pagerInfo(seriesPage)}
        <div class="flex items-center justify-between gap-2 px-3 sm:px-4 py-2 border-t border-neutral-800 text-xs text-neutral-500">
          <span>{p.from}–{p.to} of {seriesPage.total} series</span>
          <div class="flex gap-2">
            <button
              class="rounded border border-neutral-700 px-2 py-1 disabled:opacity-40 hover:text-neutral-200"
              disabled={!p.hasPrev}
              onclick={() => goToPage("series", seriesPage.offset - seriesPage.limit)}
            >
              prev
            </button>
            <button
              class="rounded border border-neutral-700 px-2 py-1 disabled:opacity-40 hover:text-neutral-200"
              disabled={!p.hasNext}
              onclick={() => goToPage("series", seriesPage.offset + seriesPage.limit)}
            >
              next
            </button>
          </div>
        </div>
      {/if}
    </section>
  </div>

  {#if settingsOpen}
    <!-- svelte-ignore a11y_click_events_have_key_events -->
    <!-- svelte-ignore a11y_no_static_element_interactions -->
    <div
      class="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-2 sm:p-4"
      onclick={(e) => { if (e.target === e.currentTarget) settingsOpen = false; }}
    >
      <div class="w-full max-w-2xl max-h-[90vh] overflow-y-auto overflow-x-hidden rounded border border-neutral-700 bg-neutral-900 shadow-lg">
        <div class="flex items-center justify-between px-4 py-3 border-b border-neutral-800">
          <h2 class="text-sm font-medium text-neutral-200">Settings</h2>
          <button class="text-neutral-500 hover:text-neutral-200 text-sm" onclick={() => (settingsOpen = false)}>
            close
          </button>
        </div>

        <div class="px-4 py-4 space-y-3 border-b border-neutral-800">
          <h3 class="text-xs font-medium text-neutral-500 uppercase">Sync now</h3>
          <div class="flex flex-wrap gap-2">
            <button
              class="rounded bg-neutral-700 hover:bg-neutral-600 disabled:opacity-50 px-3 py-1 text-sm"
              disabled={triggering.has("local")}
              onclick={() => triggerSync("local")}
            >
              {triggering.has("local") ? "syncing…" : "local"}
            </button>
            <button
              class="rounded bg-neutral-700 hover:bg-neutral-600 disabled:opacity-50 px-3 py-1 text-sm"
              disabled={triggering.has("remote") || peers.length === 0}
              onclick={() => triggerSync("remote")}
            >
              {triggering.has("remote") ? "syncing…" : "remote"}
            </button>
            <button
              class="rounded bg-neutral-700 hover:bg-neutral-600 disabled:opacity-50 px-3 py-1 text-sm"
              disabled={triggering.has("both")}
              onclick={() => triggerSync("both")}
            >
              {triggering.has("both") ? "syncing…" : "local + remote"}
            </button>
          </div>

          <label class="flex items-center gap-2 text-sm text-neutral-400">
            <input type="checkbox" bind:checked={forceScan} />
            force peer to refresh its catalog
          </label>

          <div class="space-y-2">
            {#each syncTargets as target (target.id)}
              {@const status = syncStatus[target.id] ?? { state: "idle", percent: 0 }}
              <div class="text-sm">
                <div class="flex items-center justify-between gap-2 mb-1">
                  <span class="truncate">{peerLabel(target.id)}</span>
                  <span class={`rounded px-2 py-0.5 text-xs ${syncStateColor(status.state)}`}>
                    {status.state}{status.state === "running" && syncStageLabel(status.stage) ? ` · ${syncStageLabel(status.stage)}` : ""}
                  </span>
                </div>
                <div class="h-1.5 rounded bg-neutral-800 overflow-hidden">
                  {#if status.state === "running" && !syncShowsPercent(status)}
                    <div class="h-full w-2/5 rounded bg-blue-500 sync-bar-indeterminate"></div>
                  {:else}
                    <div
                      class={`h-full rounded transition-[width] duration-500 ease-out ${syncBarColor(status.state)}`}
                      style={`width: ${status.state === "success" ? 100 : status.percent}%`}
                    ></div>
                  {/if}
                </div>
                {#if status.error}
                  <div class="text-xs text-red-400 mt-1 break-words">{status.error}</div>
                {/if}
              </div>
            {/each}
          </div>
        </div>

        <div class="px-4 py-4 border-b border-neutral-800">
          <h3 class="text-xs font-medium text-neutral-500 uppercase mb-3">Peers</h3>
          <table class="w-full text-sm">
            <tbody>
              {#each peers as peer (peer.id)}
                <tr class="border-b border-neutral-800/60 last:border-0">
                  <td class="px-2 py-2 max-w-0 w-full">
                    <div class="truncate">{peer.name || "(unnamed)"}</div>
                    <div class="text-neutral-500 font-mono text-xs truncate" title={peer.url}>{peer.url}</div>
                    <div class="text-neutral-600 font-mono text-xs truncate">
                      {peer.ip || "IP unknown"} · {peer.version ? `v${peer.version}` : "version unknown"}
                    </div>
                    <div class="text-neutral-500 text-xs truncate" title={lastSyncTitle(peer)}>{lastSyncLabel(peer)}</div>
                    <div class="text-neutral-700 font-mono text-xs truncate" title={peer.id}>{peer.id}</div>
                  </td>
                  <td class="px-2 py-2 align-top">
                    <span class={`rounded px-2 py-0.5 text-xs ${stateColor(peer.state)}`}>{peer.state}</span>
                  </td>
                  <td class="px-2 py-2 text-right align-top">
                    <button
                      class="text-neutral-500 hover:text-red-400 text-xs"
                      onclick={() => removePeer(peer.id)}
                    >
                      remove
                    </button>
                  </td>
                </tr>
              {:else}
                <tr><td class="px-2 py-3 text-neutral-500" colspan="3">No peers configured.</td></tr>
              {/each}
            </tbody>
          </table>
          <form
            class="flex gap-2 pt-3"
            onsubmit={(e) => { e.preventDefault(); addPeer(); }}
          >
            <input
              class="flex-1 min-w-0 rounded bg-neutral-800 border border-neutral-700 px-2 py-1 text-sm font-mono"
              placeholder="http://peer-host:8080"
              bind:value={newPeerUrl}
            />
            <button
              class="rounded bg-neutral-700 hover:bg-neutral-600 disabled:opacity-50 px-3 py-1 text-sm"
              type="submit"
              disabled={addingPeer}
            >
              {addingPeer ? "checking…" : "add"}
            </button>
          </form>
        </div>

        <div class="px-4 py-4 space-y-4">
          <h3 class="text-xs font-medium text-neutral-500 uppercase">Library update frequency</h3>
          <form
            class="flex items-center gap-2"
            onsubmit={(e) => { e.preventDefault(); saveInterval(); }}
          >
            <select
              class="rounded bg-neutral-800 border border-neutral-700 px-2 py-1 text-sm font-mono"
              bind:value={syncIntervalHours}
            >
              {#each syncIntervalOptions as hours (hours)}
                <option value={hours}>{hours}</option>
              {/each}
            </select>
            <span class="text-sm text-neutral-500">hours</span>
            <button
              class="rounded bg-neutral-700 hover:bg-neutral-600 disabled:opacity-50 px-3 py-1 text-sm"
              type="submit"
              disabled={savingInterval}
            >
              {savingInterval ? "saving…" : "save"}
            </button>
          </form>
        </div>
      </div>
    </div>
  {/if}
</div>
