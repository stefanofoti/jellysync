package catalog

import (
	"context"
	"errors"
	"testing"

	"jellysync/internal/config"
)

func hiddenKeys(t *testing.T, n *node) map[string]bool {
	t.Helper()
	h, err := hiddenSet(context.Background(), n.db)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func hiddenRow(t *testing.T, n *node, globalID string) bool {
	t.Helper()
	var h bool
	if err := n.db.QueryRow(`SELECT hidden FROM catalog_items WHERE global_id = ?`, globalID).Scan(&h); err != nil {
		t.Fatalf("reading %s: %v", globalID, err)
	}
	return h
}

func TestHiddenLocalItemIsWithdrawnFromPeers(t *testing.T) {
	ctx := context.Background()
	a := newNode(t, movies(3, 1))
	b := newNode(t, nil)
	regA := a.registry(t)
	regB := b.registry(t, config.Peer{ID: "a", URL: a.srv.URL})
	mustSync(t, a, regA, true)
	mustSync(t, b, regB, true)
	assertItems(t, b.items(t), "tmdb:1=a", "tmdb:2=a", "tmdb:3=a")

	if err := SetHidden(ctx, a.db, "tmdb:2", true); err != nil {
		t.Fatal(err)
	}
	// Applied at A's next sync, not before.
	mustSync(t, b, regB, false)
	assertItems(t, b.items(t), "tmdb:1=a", "tmdb:2=a", "tmdb:3=a")

	st := mustSync(t, a, regA, true)
	if st.Local.Hidden != 1 || st.Local.Changed() != 1 {
		t.Errorf("refresh stats = %+v, want 1 hidden", st.Local)
	}
	// Still local on A (so no peer's copy would replace it), just hidden.
	assertItems(t, a.items(t), "tmdb:1=local", "tmdb:2=local", "tmdb:3=local")
	if !hiddenRow(t, a, "tmdb:2") {
		t.Error("tmdb:2 not marked hidden on A")
	}
	mustSync(t, b, regB, false)
	assertItems(t, b.items(t), "tmdb:1=a", "tmdb:3=a")
	if entries, _ := a.ix.Entries(ctx); len(entries) != 2 {
		t.Errorf("legacy full catalog has %d entries, want 2", len(entries))
	}
	if f, _ := a.ix.Changes(ctx, "", 0, 10); len(f.Changes) != 2 {
		t.Errorf("feed from scratch has %d entries, want 2", len(f.Changes))
	}

	if err := SetHidden(ctx, a.db, "tmdb:2", false); err != nil {
		t.Fatal(err)
	}
	if st := mustSync(t, a, regA, true); st.Local.Unhidden != 1 {
		t.Errorf("refresh stats = %+v, want 1 unhidden", st.Local)
	}
	mustSync(t, b, regB, false)
	assertItems(t, b.items(t), "tmdb:1=a", "tmdb:2=a", "tmdb:3=a")
}

func TestHiddenRemoteItemAndPruning(t *testing.T) {
	ctx := context.Background()
	a := newNode(t, movies(3, 1))
	b := newNode(t, nil)
	reg := b.registry(t, config.Peer{ID: "a", URL: a.srv.URL})
	mustSync(t, b, reg, true)

	if err := SetHidden(ctx, b.db, "tmdb:9", true); !errors.Is(err, errNoSuchItem) {
		t.Fatalf("hiding an unknown item: %v, want errNoSuchItem", err)
	}
	if err := SetHidden(ctx, b.db, "tmdb:2", true); err != nil {
		t.Fatal(err)
	}
	if st := mustSync(t, b, reg, false); st.Election.Hidden != 1 {
		t.Errorf("election stats = %+v, want 1 hidden", st.Election)
	}
	// Still listed, elected to its peer, but hidden: Reconcile skips it.
	assertItems(t, b.items(t), "tmdb:1=a", "tmdb:2=a", "tmdb:3=a")
	if !hiddenRow(t, b, "tmdb:2") || hiddenRow(t, b, "tmdb:1") {
		t.Error("wrong rows marked hidden")
	}

	// The peer removes it: the hide goes with it.
	a.jf.set(map[string]string{"1": "Movie 1", "3": "Movie 3"})
	if _, err := a.ix.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if st := mustSync(t, b, reg, false); st.Election.HidesDropped != 1 {
		t.Errorf("election stats = %+v, want 1 hide dropped", st.Election)
	}
	if h := hiddenKeys(t, b); len(h) != 0 {
		t.Errorf("hidden_items = %v, want empty", h)
	}

	// Back again: visible.
	a.jf.set(movies(3, 1))
	if _, err := a.ix.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	mustSync(t, b, reg, false)
	if hiddenRow(t, b, "tmdb:2") {
		t.Error("tmdb:2 hidden again after coming back")
	}
}

func TestHiddenSeriesCoversEpisodes(t *testing.T) {
	ep := func(id, n string) Entry {
		return Entry{GlobalID: id, Name: n, MediaType: "episode", SeriesGlobalID: "tvdb:7", SeriesName: "Show"}
	}
	hidden := map[string]bool{"tvdb:7": true}
	for _, e := range []Entry{
		ep("tvdb:71", "Pilot"),
		{GlobalID: "tvdb:7", Name: "Show", MediaType: "series"},
	} {
		if !isHidden(hidden, e) {
			t.Errorf("%s not hidden by its series", e.GlobalID)
		}
	}
	if isHidden(hidden, Entry{GlobalID: "tvdb:8", MediaType: "series"}) {
		t.Error("other series hidden")
	}
	// Providerless series group (and hide) by name.
	if !isHidden(map[string]bool{"Show": true}, Entry{GlobalID: "h:1", MediaType: "episode", SeriesName: "Show"}) {
		t.Error("episode not hidden by series name key")
	}
}
