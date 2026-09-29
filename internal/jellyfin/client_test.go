package jellyfin

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func TestBuildCatalogItemsJoinsSeries(t *testing.T) {
	raw := []rawItem{
		{
			Id:          "series-1",
			Name:        "Test Show",
			Type:        "Series",
			ProviderIds: map[string]string{"Tvdb": "42"},
		},
		{
			Id:                "ep-1",
			Name:              "Pilot",
			Type:              "Episode",
			Path:              "/media/show/s01e01.mkv",
			SeriesId:          "series-1",
			SeriesName:        "Test Show",
			ParentIndexNumber: 1,
			IndexNumber:       1,
		},
		{
			Id:   "movie-1",
			Name: "Test Movie",
			Type: "Movie",
			Path: "/media/movie.mkv",
		},
	}

	items := buildCatalogItems(raw)
	if len(items) != 3 {
		t.Fatalf("got %d items, want 3", len(items))
	}

	var ep, movie *CatalogItem
	for i := range items {
		switch items[i].ItemID {
		case "ep-1":
			ep = &items[i]
		case "movie-1":
			movie = &items[i]
		}
	}

	if ep == nil {
		t.Fatal("episode not found in result")
	}
	if want := "tvdb:42"; ep.SeriesGlobalID != want {
		t.Errorf("episode SeriesGlobalID = %q, want %q", ep.SeriesGlobalID, want)
	}
	if ep.SeriesName != "Test Show" {
		t.Errorf("episode SeriesName = %q, want %q", ep.SeriesName, "Test Show")
	}
	if ep.SeasonNumber != 1 || ep.EpisodeNumber != 1 {
		t.Errorf("episode season/episode = %d/%d, want 1/1", ep.SeasonNumber, ep.EpisodeNumber)
	}

	if movie == nil {
		t.Fatal("movie not found in result")
	}
	if movie.SeriesGlobalID != "" {
		t.Errorf("movie SeriesGlobalID = %q, want empty", movie.SeriesGlobalID)
	}
}

func TestEpisodeHashAvoidsCrossSeriesCollision(t *testing.T) {
	episode := func(id, seriesID string) rawItem {
		return rawItem{
			Id: id, Name: "Pilot", Type: "Episode", Path: "/media/" + id + ".mkv",
			SeriesId: seriesID, ParentIndexNumber: 1, IndexNumber: 1,
		}
	}
	a := buildCatalogItems([]rawItem{
		{Id: "s1", Name: "Show A", Type: "Series"},
		episode("e1", "s1"),
	})
	b := buildCatalogItems([]rawItem{
		{Id: "s2", Name: "Show B", Type: "Series"},
		episode("e2", "s2"),
	})

	epA := a[1]
	epB := b[1]
	if epA.GlobalID() == epB.GlobalID() {
		t.Errorf("episodes from different series with no provider IDs collided on GlobalID %q", epA.GlobalID())
	}
}

func TestBuildCatalogItemsExcludesVirtualItems(t *testing.T) {
	raw := []rawItem{
		{
			// Jellyseerr's "request this" placeholder: no backing file.
			Id: "virtual-1", Name: "Some Unreleased Movie", Type: "Movie",
			IsVirtualItem: true,
		},
		{
			// LocationType "Virtual" with no Path either.
			Id: "virtual-2", Name: "Another Placeholder", Type: "Movie",
			LocationType: "Virtual",
		},
		{
			// No Path at all (e.g. an item Jellyfin hasn't finished
			// importing): still excluded, since isRealMedia requires one
			// for movies/episodes regardless of LocationType.
			Id: "no-path", Name: "Missing Path", Type: "Movie",
		},
		{
			Id: "real-movie", Name: "Real Movie", Type: "Movie",
			Path: "/media/real.mkv", LocationType: "FileSystem",
		},
	}

	items := buildCatalogItems(raw)
	if len(items) != 1 {
		t.Fatalf("got %d items, want 1: %+v", len(items), items)
	}
	if items[0].ItemID != "real-movie" {
		t.Errorf("surviving item = %q, want %q", items[0].ItemID, "real-movie")
	}
}

// TestListItemsIntegration hits a real Jellyfin instance. It's skipped
// unless JELLYFIN_TEST_URL and JELLYFIN_TEST_API_KEY are set, since it
// depends on external state (a running Jellyfin with a known item).
func TestListItemsIntegration(t *testing.T) {
	url := os.Getenv("JELLYFIN_TEST_URL")
	apiKey := os.Getenv("JELLYFIN_TEST_API_KEY")
	if url == "" || apiKey == "" {
		t.Skip("JELLYFIN_TEST_URL / JELLYFIN_TEST_API_KEY not set")
	}

	c := New(url, apiKey, "test-node", 15*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := c.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	items, err := c.ListItems(ctx)
	if err != nil {
		t.Fatalf("ListItems: %v", err)
	}

	var found *CatalogItem
	for i := range items {
		if strings.HasPrefix(items[i].Name, "Test Movie") {
			found = &items[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("expected to find an item starting with %q among %d items", "Test Movie", len(items))
	}
	if found.MediaType != "movie" {
		t.Errorf("MediaType = %q, want %q", found.MediaType, "movie")
	}
	if found.Year != 2020 {
		t.Errorf("Year = %d, want 2020", found.Year)
	}
	if found.GlobalID() == "" {
		t.Error("GlobalID() is empty")
	}
	t.Logf("found item: %+v (GlobalID=%s)", *found, found.GlobalID())
}
