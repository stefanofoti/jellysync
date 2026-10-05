<script>
  // A "…" button opening a small dropdown of actions. actions is a list of
  // { label, onclick, disabled?, title? }. The dropdown is position: fixed
  // so the table's overflow-x-auto wrapper can't clip it; it closes on any
  // click outside, Escape, scroll or resize, so at most one is open.
  let { actions, label = "Actions" } = $props();

  let open = $state(false);
  let pos = $state({ top: 0, right: 0 });
  let button;
  let menu = $state();

  function toggle(e) {
    e.stopPropagation();
    if (open) {
      open = false;
      return;
    }
    const r = button.getBoundingClientRect();
    pos = { top: r.bottom + 4, right: window.innerWidth - r.right };
    open = true;
  }

  // Flip above the button when the menu would run off the bottom.
  $effect(() => {
    if (!open || !menu) return;
    const r = button.getBoundingClientRect();
    const h = menu.offsetHeight;
    if (r.bottom + 4 + h > window.innerHeight && r.top - 4 - h > 0) pos = { ...pos, top: r.top - 4 - h };
  });

  $effect(() => {
    if (!open) return;
    const close = () => (open = false);
    const onPointer = (e) => {
      if (!menu?.contains(e.target) && !button.contains(e.target)) close();
    };
    const onKey = (e) => {
      if (e.key === "Escape") close();
    };
    window.addEventListener("pointerdown", onPointer, true);
    window.addEventListener("keydown", onKey);
    window.addEventListener("scroll", close, true);
    window.addEventListener("resize", close);
    return () => {
      window.removeEventListener("pointerdown", onPointer, true);
      window.removeEventListener("keydown", onKey);
      window.removeEventListener("scroll", close, true);
      window.removeEventListener("resize", close);
    };
  });

  function run(e, action) {
    e.stopPropagation();
    open = false;
    action.onclick();
  }
</script>

<button
  bind:this={button}
  class="rounded px-2 py-0.5 text-neutral-500 hover:text-neutral-200 hover:bg-neutral-800 text-sm leading-none"
  aria-label={label}
  aria-haspopup="menu"
  aria-expanded={open}
  title={label}
  onclick={toggle}
>
  …
</button>

{#if open}
  <div
    bind:this={menu}
    role="menu"
    class="fixed z-40 min-w-[8rem] rounded border border-neutral-700 bg-neutral-900 py-1 shadow-lg text-left"
    style={`top: ${pos.top}px; right: ${pos.right}px`}
  >
    {#each actions as action (action.label)}
      <button
        role="menuitem"
        class="block w-full px-3 py-1.5 text-left text-sm text-neutral-300 hover:bg-neutral-800 disabled:opacity-40 disabled:hover:bg-transparent"
        disabled={action.disabled}
        title={action.title}
        onclick={(e) => run(e, action)}
      >
        {action.label}
      </button>
    {/each}
  </div>
{/if}
