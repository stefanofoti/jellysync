<script>
  // Details of one list entry. target is { kind: "item", item } for a movie
  // or episode (fetches GET /api/v1/items/info: media details and every
  // copy any source offers) or { kind: "series", row } for a series row
  // (its list summary only).
  let { target, peerLabel, onclose } = $props();

  let info = $state(null);
  let loadError = $state("");
  let copied = $state("");

  $effect(() => {
    if (target.kind !== "item") return;
    const id = target.item.global_id;
    info = null;
    loadError = "";
    fetch(`/api/v1/items/info?global_id=${encodeURIComponent(id)}`)
      .then(async (res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}: ${(await res.text()).trim()}`);
        return res.json();
      })
      .then((data) => (info = data))
      .catch((e) => (loadError = String(e)));
  });

  $effect(() => {
    const onKey = (e) => {
      if (e.key === "Escape") onclose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  });

  // Prefer the fetched row; fall back to the list's while loading.
  let item = $derived(info?.item ?? target.item);

  let title = $derived(target.kind === "series" ? target.row.name : episodeTitle(item));

  function episodeTitle(it) {
    if (it.media_type !== "episode") return it.name;
    const s = String(it.season_number ?? 0).padStart(2, "0");
    const e = String(it.episode_number ?? 0).padStart(2, "0");
    return `${it.series_name} · S${s}E${e} · ${it.name}`;
  }

  async function copy(text, key) {
    try {
      await navigator.clipboard.writeText(text);
      copied = key;
      setTimeout(() => {
        if (copied === key) copied = "";
      }, 1500);
    } catch {
      // Clipboard needs a secure context; the path stays selectable.
    }
  }

  function sourceLabel(source) {
    return source === "local" ? "this node" : peerLabel(source);
  }

  function offerNote(o) {
    const notes = [];
    if (o.elected) notes.push("elected");
    if (o.hidden) notes.push("hidden");
    if (o.excluded_folder) notes.push("excluded folder");
    if (o.source !== "local" && !o.peer_state) notes.push("peer removed");
    else if (o.source !== "local" && !o.offering) notes.push(`not offering (${o.peer_state})`);
    return notes;
  }
</script>

{#snippet field(label, value)}
  {#if value !== undefined && value !== null && value !== ""}
    <div class="grid grid-cols-[7rem_1fr] gap-2 py-1">
      <dt class="text-neutral-500">{label}</dt>
      <dd class="text-neutral-300 break-words min-w-0">{value}</dd>
    </div>
  {/if}
{/snippet}

{#snippet pathBox(label, path, key, note = "")}
  <div class="py-1">
    <div class="flex items-center justify-between gap-2">
      <span class="text-neutral-500">{label}</span>
      {#if path}
        <button class="text-xs text-neutral-500 hover:text-neutral-200" onclick={() => copy(path, key)}>
          {copied === key ? "copied" : "copy"}
        </button>
      {/if}
    </div>
    {#if path}
      <div class="mt-1 rounded bg-neutral-950 px-2 py-1 font-mono text-xs text-neutral-300 break-all select-all">{path}</div>
    {:else}
      <div class="mt-1 text-xs text-neutral-600">{note || "not available"}</div>
    {/if}
  </div>
{/snippet}

<!-- svelte-ignore a11y_click_events_have_key_events -->
<!-- svelte-ignore a11y_no_static_element_interactions -->
<div
  class="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-2 sm:p-4"
  onclick={(e) => {
    if (e.target === e.currentTarget) onclose();
  }}
>
  <div class="w-full max-w-2xl max-h-[90vh] overflow-y-auto overflow-x-hidden rounded border border-neutral-700 bg-neutral-900 shadow-lg">
    <div class="flex items-center justify-between gap-3 px-4 py-3 border-b border-neutral-800">
      <h2 class="text-sm font-medium text-neutral-200 break-words min-w-0">{title}</h2>
      <button class="text-neutral-500 hover:text-neutral-200 text-sm" onclick={onclose}>close</button>
    </div>

    {#if target.kind === "series"}
      {@const row = target.row}
      <div class="px-4 py-4 text-sm">
        <h3 class="text-xs font-medium text-neutral-500 uppercase mb-1">Series</h3>
        <dl>
          {@render field("Name", row.name)}
          {@render field("Key", row.key)}
          {@render field("Episodes", `${row.totalCount} (${row.localCount} local, ${row.totalCount - row.localCount} remote)`)}
          {@render field("Sync", row.hidden ? "hidden: excluded from sync" : "included in sync")}
          {@render field("Pending", row.hidePending ? "change applied at the next sync" : "")}
          {@render field("Folder rules", row.folderHiddenCount ? `${row.folderHiddenCount} episode(s) hidden by an excluded folder` : "")}
        </dl>
        <p class="mt-3 text-xs text-neutral-600">Expand the series and open an episode's info for its paths.</p>
      </div>
    {:else}
      <div class="px-4 py-4 space-y-4 text-sm">
        <section>
          <h3 class="text-xs font-medium text-neutral-500 uppercase mb-1">Media</h3>
          <dl>
            {@render field("Type", item.media_type)}
            {@render field("Name", item.name)}
            {@render field("Series", item.series_name)}
            {@render field("Season", item.media_type === "episode" ? item.season_number : "")}
            {@render field("Episode", item.media_type === "episode" ? item.episode_number : "")}
            {@render field("Year", info?.year)}
            {@render field("TMDB", info?.tmdb_id)}
            {@render field("TVDB", info?.tvdb_id)}
            {@render field("IMDb", info?.imdb_id)}
            {@render field("Global ID", item.global_id)}
          </dl>
        </section>

        <section>
          <h3 class="text-xs font-medium text-neutral-500 uppercase mb-1">Source</h3>
          <dl>
            {@render field("Served by", item.local ? "this node (local)" : `${peerLabel(item.primary_peer_id)} (remote)`)}
            {@render field(
              "Sync",
              item.hidden_folder
                ? "hidden by an excluded folder: no .strm file"
                : item.hidden
                  ? "hidden: excluded from sync"
                  : "included in sync",
            )}
            {@render field("Pending", item.hide_pending ? "change applied at the next sync" : "")}
          </dl>
        </section>

        <section>
          <h3 class="text-xs font-medium text-neutral-500 uppercase mb-1">Paths</h3>
          {@render pathBox(
            item.local ? "File on this node" : `File on ${peerLabel(item.primary_peer_id)}'s filesystem`,
            item.path,
            "path",
            item.local ? "" : "not reported: the peer runs an older jellysync",
          )}
          {#if !item.local}
            {@render pathBox(".strm on this node", item.strm_path, "strm", "no .strm file written")}
          {/if}
        </section>

        <section>
          <h3 class="text-xs font-medium text-neutral-500 uppercase mb-1">Available copies</h3>
          {#if loadError}
            <p class="text-xs text-red-400">Failed to load: {loadError}</p>
          {:else if !info}
            <p class="text-xs text-neutral-500">Loading…</p>
          {:else if info.offers.length === 0}
            <p class="text-xs text-neutral-500">No source offers this item any more.</p>
          {:else}
            <ul class="space-y-2">
              {#each info.offers as o (o.source)}
                <li class="rounded border border-neutral-800 px-2 py-1.5">
                  <div class="flex flex-wrap items-center gap-1">
                    <span class="text-neutral-300">{sourceLabel(o.source)}</span>
                    {#each offerNote(o) as note (note)}
                      <span
                        class={`rounded px-1.5 py-0.5 text-xs ${note === "elected" ? "bg-green-500/20 text-green-400" : "bg-amber-500/20 text-amber-400"}`}
                      >
                        {note}
                      </span>
                    {/each}
                  </div>
                  <div class="mt-1 font-mono text-xs text-neutral-500 break-all select-all">
                    {o.path || "path not reported"}
                  </div>
                </li>
              {/each}
            </ul>
          {/if}
        </section>
      </div>
    {/if}
  </div>
</div>
