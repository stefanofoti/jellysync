<script>
  let health = $state(null);
  let peers = $state([]);
  let items = $state([]);
  let error = $state("");

  let newPeerId = $state("");
  let newPeerUrl = $state("");

  async function refresh() {
    try {
      const [healthRes, peersRes, itemsRes] = await Promise.all([
        fetch("/health"),
        fetch("/api/v1/peers"),
        fetch("/api/v1/items"),
      ]);
      health = await healthRes.json();
      peers = await peersRes.json();
      items = await itemsRes.json();
      error = "";
    } catch (e) {
      error = String(e);
    }
  }

  async function addPeer() {
    if (!newPeerId || !newPeerUrl) return;
    const res = await fetch("/api/v1/peers", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ id: newPeerId, url: newPeerUrl }),
    });
    if (res.ok) {
      newPeerId = "";
      newPeerUrl = "";
      await refresh();
    } else {
      error = await res.text();
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

  function stateColor(state) {
    if (state === "ONLINE") return "bg-green-500/20 text-green-400";
    if (state === "DEGRADED") return "bg-yellow-500/20 text-yellow-400";
    return "bg-red-500/20 text-red-400";
  }

  let expandedSeries = $state(new Set());

  function toggleSeries(key) {
    const next = new Set(expandedSeries);
    if (next.has(key)) {
      next.delete(key);
    } else {
      next.add(key);
    }
    expandedSeries = next;
  }

  function seasonLabel(number) {
    return number === 0 ? "Specials" : `Season ${number}`;
  }

  function episodeLabel(item) {
    const season = String(item.season_number ?? 0).padStart(2, "0");
    const episode = String(item.episode_number ?? 0).padStart(2, "0");
    return `S${season}E${episode} · ${item.name}`;
  }

  // The dashboard groups episodes under a collapsible series row instead
  // of listing every episode flat; movies (and any leftover series-root
  // rows, which carry no playable file of their own) stay as simple rows.
  // Grouping and sorting happen client-side against the flat /api/v1/items
  // response — series_global_id (falling back to series_name) is the same
  // cross-node-stable key the backend uses to elect a primary peer per
  // episode, so it's also what ties episodes back to the same series here.
  let catalogRows = $derived.by(() => {
    const seriesByKey = new Map();
    const rows = [];

    for (const item of items) {
      if (item.media_type === "episode") {
        const key = item.series_global_id || item.series_name || "unknown";
        let group = seriesByKey.get(key);
        if (!group) {
          group = { type: "series", key, name: item.series_name || "Unknown series", seasons: new Map() };
          seriesByKey.set(key, group);
          rows.push(group);
        }
        let season = group.seasons.get(item.season_number ?? 0);
        if (!season) {
          season = [];
          group.seasons.set(item.season_number ?? 0, season);
        }
        season.push(item);
      } else if (item.media_type === "movie") {
        rows.push({ type: "movie", key: item.global_id, name: item.name, item });
      }
    }

    for (const group of seriesByKey.values()) {
      group.seasons = [...group.seasons.entries()]
        .sort((a, b) => a[0] - b[0])
        .map(([number, episodes]) => ({
          number,
          episodes: episodes.sort((a, b) => (a.episode_number ?? 0) - (b.episode_number ?? 0)),
        }));
      group.localCount = group.seasons.reduce((n, s) => n + s.episodes.filter((e) => e.local).length, 0);
      group.totalCount = group.seasons.reduce((n, s) => n + s.episodes.length, 0);
    }

    rows.sort((a, b) => a.name.localeCompare(b.name));
    return rows;
  });

  $effect(() => {
    refresh();
    const interval = setInterval(refresh, 5000);
    return () => clearInterval(interval);
  });
</script>

<div class="min-h-screen bg-neutral-950 text-neutral-200 p-6 font-sans">
  <div class="max-w-4xl mx-auto space-y-6">
    <header class="flex items-baseline justify-between">
      <h1 class="text-xl font-semibold text-neutral-100">jellysync</h1>
      {#if health}
        <span class="text-sm text-neutral-500">
          node <span class="text-neutral-300">{health.node_id}</span>
          · up {health.uptime_seconds}s
        </span>
      {/if}
    </header>

    {#if error}
      <div class="rounded border border-red-800 bg-red-950/50 text-red-300 text-sm px-3 py-2">{error}</div>
    {/if}

    <section class="rounded border border-neutral-800 bg-neutral-900/50">
      <h2 class="text-sm font-medium text-neutral-400 px-4 py-3 border-b border-neutral-800">Peers</h2>
      <table class="w-full text-sm">
        <tbody>
          {#each peers as peer (peer.id)}
            <tr class="border-b border-neutral-800/60 last:border-0">
              <td class="px-4 py-2 font-mono">{peer.id}</td>
              <td class="px-4 py-2 text-neutral-500 font-mono">{peer.url}</td>
              <td class="px-4 py-2">
                <span class={`rounded px-2 py-0.5 text-xs ${stateColor(peer.state)}`}>{peer.state}</span>
              </td>
              <td class="px-4 py-2 text-right">
                <button
                  class="text-neutral-500 hover:text-red-400 text-xs"
                  onclick={() => removePeer(peer.id)}
                >
                  remove
                </button>
              </td>
            </tr>
          {:else}
            <tr><td class="px-4 py-3 text-neutral-500" colspan="4">No peers configured.</td></tr>
          {/each}
        </tbody>
      </table>
      <form
        class="flex gap-2 px-4 py-3 border-t border-neutral-800"
        onsubmit={(e) => { e.preventDefault(); addPeer(); }}
      >
        <input
          class="flex-none w-32 rounded bg-neutral-800 border border-neutral-700 px-2 py-1 text-sm font-mono"
          placeholder="peer id"
          bind:value={newPeerId}
        />
        <input
          class="flex-1 rounded bg-neutral-800 border border-neutral-700 px-2 py-1 text-sm font-mono"
          placeholder="http://peer-host:8080"
          bind:value={newPeerUrl}
        />
        <button class="rounded bg-neutral-700 hover:bg-neutral-600 px-3 py-1 text-sm" type="submit">
          add
        </button>
      </form>
    </section>

    <section class="rounded border border-neutral-800 bg-neutral-900/50">
      <h2 class="text-sm font-medium text-neutral-400 px-4 py-3 border-b border-neutral-800">Catalog</h2>
      <table class="w-full text-sm">
        <tbody>
          {#each catalogRows as row (row.key)}
            {#if row.type === "movie"}
              <tr class="border-b border-neutral-800/60 last:border-0">
                <td class="px-4 py-2">{row.item.name}</td>
                <td class="px-4 py-2 text-neutral-500">movie</td>
                <td class="px-4 py-2">
                  {#if row.item.local}
                    <span class="rounded px-2 py-0.5 text-xs bg-blue-500/20 text-blue-400">local</span>
                  {:else}
                    <span class="rounded px-2 py-0.5 text-xs bg-purple-500/20 text-purple-400">
                      remote · {row.item.primary_peer_id}
                    </span>
                  {/if}
                </td>
              </tr>
            {:else}
              <tr
                class="border-b border-neutral-800/60 last:border-0 cursor-pointer hover:bg-neutral-800/40"
                onclick={() => toggleSeries(row.key)}
              >
                <td class="px-4 py-2">
                  <span class="inline-block w-4 text-neutral-500">{expandedSeries.has(row.key) ? "▾" : "▸"}</span>
                  {row.name}
                </td>
                <td class="px-4 py-2 text-neutral-500">series</td>
                <td class="px-4 py-2">
                  {#if row.localCount === row.totalCount}
                    <span class="rounded px-2 py-0.5 text-xs bg-blue-500/20 text-blue-400">local</span>
                  {:else if row.localCount === 0}
                    <span class="rounded px-2 py-0.5 text-xs bg-purple-500/20 text-purple-400">remote</span>
                  {:else}
                    <span class="rounded px-2 py-0.5 text-xs bg-neutral-700 text-neutral-300">mixed</span>
                  {/if}
                  <span class="text-neutral-600 text-xs ml-1">{row.localCount}/{row.totalCount} local</span>
                </td>
              </tr>
              {#if expandedSeries.has(row.key)}
                {#each row.seasons as season (season.number)}
                  <tr class="border-b border-neutral-800/60 last:border-0 bg-neutral-900/30">
                    <td class="px-4 py-1.5 pl-9 text-neutral-500 text-xs" colspan="3">{seasonLabel(season.number)}</td>
                  </tr>
                  {#each season.episodes as episode (episode.global_id)}
                    <tr class="border-b border-neutral-800/60 last:border-0">
                      <td class="px-4 py-2 pl-14 text-neutral-300">{episodeLabel(episode)}</td>
                      <td class="px-4 py-2 text-neutral-500">episode</td>
                      <td class="px-4 py-2">
                        {#if episode.local}
                          <span class="rounded px-2 py-0.5 text-xs bg-blue-500/20 text-blue-400">local</span>
                        {:else}
                          <span class="rounded px-2 py-0.5 text-xs bg-purple-500/20 text-purple-400">
                            remote · {episode.primary_peer_id}
                          </span>
                        {/if}
                      </td>
                    </tr>
                  {/each}
                {/each}
              {/if}
            {/if}
          {:else}
            <tr><td class="px-4 py-3 text-neutral-500" colspan="3">No catalog items yet.</td></tr>
          {/each}
        </tbody>
      </table>
    </section>
  </div>
</div>
