package strm

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPathForMovie(t *testing.T) {
	r := row{
		globalID:  "tmdb:123",
		name:      "Alice",
		mediaType: "movie",
		peerID:    "friend",
	}
	got := pathFor("/out", r)
	want := filepath.Join("/out", "movies", "friend", "Alice [tmdbid-123]", "Alice [tmdbid-123].strm")
	if got != want {
		t.Errorf("pathFor() = %q, want %q", got, want)
	}
}

func TestPathForEpisode(t *testing.T) {
	r := row{
		globalID:       "tvdb:999",
		name:           "Pilot",
		mediaType:      "episode",
		peerID:         "friend",
		seriesGlobalID: "tvdb:42",
		seriesName:     "Test Show",
		seasonNumber:   1,
		episodeNumber:  3,
	}
	got := pathFor("/out", r)
	want := filepath.Join("/out", "series", "friend", "Test Show [tvdbid-42]", "Season 01",
		"Test Show - S01E03 - Pilot [tvdbid-999].strm")
	if got != want {
		t.Errorf("pathFor() = %q, want %q", got, want)
	}
}

func TestPathForSpecialsSeason(t *testing.T) {
	r := row{
		globalID:       "tvdb:999",
		name:           "Behind the Scenes",
		mediaType:      "episode",
		peerID:         "friend",
		seriesGlobalID: "tvdb:42",
		seriesName:     "Test Show",
		seasonNumber:   0,
		episodeNumber:  1,
	}
	got := pathFor("/out", r)
	want := filepath.Join("/out", "series", "friend", "Test Show [tvdbid-42]", "Specials",
		"Test Show - S00E01 - Behind the Scenes [tvdbid-999].strm")
	if got != want {
		t.Errorf("pathFor() = %q, want %q", got, want)
	}
}

// TestPathForEpisodeMissingSeries covers an episode whose series couldn't
// be resolved (e.g. the series item was missing from the listing): it
// should fall back to the flat movie-style layout rather than nesting
// under an empty series folder name.
func TestPathForEpisodeMissingSeries(t *testing.T) {
	r := row{
		globalID:  "tvdb:999",
		name:      "Pilot",
		mediaType: "episode",
		peerID:    "friend",
	}
	got := pathFor("/out", r)
	want := filepath.Join("/out", "series", "friend", "Pilot [tvdbid-999]", "Pilot [tvdbid-999].strm")
	if got != want {
		t.Errorf("pathFor() = %q, want %q", got, want)
	}
}

// TestRemoveStrmStopsAtOutputDir guards against a regression that nearly
// shipped: a leftover .strm from before the movies/series split lives
// outside both of those folders, so the empty-parent-directory cleanup must
// still stop at outputDir itself (never walk above it) and must never
// remove the movies/ or series/ folders even when they end up empty.
func TestRemoveStrmStopsAtOutputDir(t *testing.T) {
	outputDir := t.TempDir()
	for _, d := range []string{"movies", "series"} {
		if err := os.MkdirAll(filepath.Join(outputDir, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	// Simulate a pre-split leftover: outputDir/peer/item/item.strm, with
	// no movies/series folder in between.
	itemDir := filepath.Join(outputDir, "peer", "Alice [tmdbid-123]")
	strmPath := filepath.Join(itemDir, "Alice [tmdbid-123].strm")
	if err := os.MkdirAll(itemDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(strmPath, []byte("http://example/stream"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := removeStrm(strmPath, outputDir); err != nil {
		t.Fatalf("removeStrm() error = %v", err)
	}

	if _, err := os.Stat(outputDir); err != nil {
		t.Fatalf("outputDir was removed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outputDir, "movies")); err != nil {
		t.Fatalf("movies dir was removed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outputDir, "series")); err != nil {
		t.Fatalf("series dir was removed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outputDir, "peer")); !os.IsNotExist(err) {
		t.Errorf("expected now-empty peer dir to be cleaned up, stat err = %v", err)
	}
}
