package catalog

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	_ "modernc.org/sqlite"
)

func testDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	_, err = db.Exec(`
		CREATE TABLE catalog_items (
			global_id        TEXT PRIMARY KEY,
			name             TEXT NOT NULL DEFAULT '',
			media_type       TEXT NOT NULL,
			local            INTEGER NOT NULL DEFAULT 0,
			local_item_id    TEXT,
			primary_peer_id  TEXT,
			primary_item_id  TEXT,
			strm_path        TEXT,
			series_global_id TEXT NOT NULL DEFAULT '',
			series_name      TEXT NOT NULL DEFAULT '',
			season_number    INTEGER NOT NULL DEFAULT 0,
			episode_number   INTEGER NOT NULL DEFAULT 0,
			hidden           INTEGER NOT NULL DEFAULT 0,
			updated_at       INTEGER NOT NULL
		);
		CREATE TABLE hidden_items (
			key        TEXT PRIMARY KEY,
			created_at INTEGER NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	return db
}

func seedMovies(t *testing.T, db *sql.DB, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		_, err := db.Exec(`INSERT INTO catalog_items (global_id, name, media_type, local, updated_at) VALUES (?, ?, 'movie', 1, 0)`,
			fmt.Sprintf("movie-%02d", i), fmt.Sprintf("Movie %02d", i))
		if err != nil {
			t.Fatalf("seed movie: %v", err)
		}
	}
}

func seedSeries(t *testing.T, db *sql.DB, name string, episodes int) {
	t.Helper()
	seriesID := "series:" + name
	_, err := db.Exec(`INSERT INTO catalog_items (global_id, name, media_type, local, updated_at) VALUES (?, ?, 'series', 1, 0)`,
		seriesID, name)
	if err != nil {
		t.Fatalf("seed series root: %v", err)
	}
	for i := 0; i < episodes; i++ {
		_, err := db.Exec(`
			INSERT INTO catalog_items (global_id, name, media_type, local, series_global_id, series_name, season_number, episode_number, updated_at)
			VALUES (?, ?, 'episode', 1, ?, ?, 1, ?, 0)
		`, fmt.Sprintf("%s-e%02d", seriesID, i), fmt.Sprintf("Episode %02d", i), seriesID, name, i)
		if err != nil {
			t.Fatalf("seed episode: %v", err)
		}
	}
}

func getItemsPage(t *testing.T, db *sql.DB, url string) itemsPage {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	ItemsHandler(db)(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s: status %d: %s", url, rec.Code, rec.Body.String())
	}
	var page itemsPage
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return page
}

func TestItemsHandlerNoType_ReturnsFlatArray(t *testing.T) {
	db := testDB(t)
	seedMovies(t, db, 3)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/items", nil)
	rec := httptest.NewRecorder()
	ItemsHandler(db)(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}

	var items []itemDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &items); err != nil {
		t.Fatalf("expected flat array response, got: %s (%v)", rec.Body.String(), err)
	}
	if len(items) != 3 {
		t.Fatalf("got %d items, want 3", len(items))
	}
}

func TestItemsHandlerMoviesPagination(t *testing.T) {
	db := testDB(t)
	seedMovies(t, db, 5)

	page := getItemsPage(t, db, "/api/v1/items?type=movie&limit=2&offset=0")
	if page.Total != 5 {
		t.Errorf("total = %d, want 5", page.Total)
	}
	if len(page.Items) != 2 {
		t.Fatalf("got %d items, want 2", len(page.Items))
	}
	if page.Items[0].Name != "Movie 00" || page.Items[1].Name != "Movie 01" {
		t.Errorf("unexpected page order: %+v", page.Items)
	}

	page2 := getItemsPage(t, db, "/api/v1/items?type=movie&limit=2&offset=4")
	if len(page2.Items) != 1 {
		t.Fatalf("last page: got %d items, want 1", len(page2.Items))
	}
	if page2.Items[0].Name != "Movie 04" {
		t.Errorf("last page item = %q, want Movie 04", page2.Items[0].Name)
	}
}

func getSeriesSummaryPage(t *testing.T, db *sql.DB, url string) seriesSummaryPage {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	ItemsHandler(db)(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s: status %d: %s", url, rec.Code, rec.Body.String())
	}
	var page seriesSummaryPage
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return page
}

func TestItemsHandlerSeriesPagination_ReturnsOneSummaryPerSeries(t *testing.T) {
	db := testDB(t)
	seedSeries(t, db, "Show A", 3)
	seedSeries(t, db, "Show B", 2)
	seedSeries(t, db, "Show C", 4)

	page := getSeriesSummaryPage(t, db, "/api/v1/items?type=series&limit=1&offset=0")
	if page.Total != 3 {
		t.Errorf("total = %d, want 3", page.Total)
	}
	if len(page.Items) != 1 {
		t.Fatalf("limit=1 must return exactly 1 series summary regardless of episode count; got %d items", len(page.Items))
	}
	if page.Items[0].SeriesName != "Show A" || page.Items[0].TotalCount != 3 {
		t.Errorf("unexpected summary: %+v", page.Items[0])
	}

	page2 := getSeriesSummaryPage(t, db, "/api/v1/items?type=series&limit=1&offset=1")
	if len(page2.Items) != 1 {
		t.Fatalf("got %d items, want 1", len(page2.Items))
	}
	if page2.Items[0].SeriesName != "Show B" || page2.Items[0].TotalCount != 2 {
		t.Errorf("unexpected summary: %+v", page2.Items[0])
	}
}

func TestSeriesEpisodesHandler(t *testing.T) {
	db := testDB(t)
	seedSeries(t, db, "Show A", 3)
	seedSeries(t, db, "Show B", 2)

	summary := getSeriesSummaryPage(t, db, "/api/v1/items?type=series&limit=10&offset=0")
	var showAKey string
	for _, it := range summary.Items {
		if it.SeriesName == "Show A" {
			showAKey = it.SeriesKey
		}
	}
	if showAKey == "" {
		t.Fatalf("Show A not found in summary: %+v", summary.Items)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/items/series/episodes?key="+url.QueryEscape(showAKey), nil)
	rec := httptest.NewRecorder()
	SeriesEpisodesHandler(db)(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var episodes []itemDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &episodes); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(episodes) != 3 {
		t.Fatalf("got %d episodes, want 3", len(episodes))
	}
	for _, ep := range episodes {
		if ep.SeriesName != "Show A" {
			t.Errorf("leaked episode from series %q", ep.SeriesName)
		}
	}
}

func TestSeriesEpisodesHandlerMissingKey(t *testing.T) {
	db := testDB(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/items/series/episodes", nil)
	rec := httptest.NewRecorder()
	SeriesEpisodesHandler(db)(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status %d, want 400", rec.Code)
	}
}

func TestItemsHandlerInvalidParams(t *testing.T) {
	db := testDB(t)

	for _, url := range []string{
		"/api/v1/items?type=bogus",
		"/api/v1/items?type=movie&limit=0",
		"/api/v1/items?type=movie&limit=abc",
		"/api/v1/items?type=movie&offset=-1",
	} {
		req := httptest.NewRequest(http.MethodGet, url, nil)
		rec := httptest.NewRecorder()
		ItemsHandler(db)(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("GET %s: status %d, want 400", url, rec.Code)
		}
	}
}

func TestItemsHandlerMoviesLimitCappedAtMax(t *testing.T) {
	db := testDB(t)
	seedMovies(t, db, 3)

	page := getItemsPage(t, db, fmt.Sprintf("/api/v1/items?type=movie&limit=%d", maxItemsLimit+100))
	if page.Limit != maxItemsLimit {
		t.Errorf("limit = %d, want capped at %d", page.Limit, maxItemsLimit)
	}
}

func seedOwned(t *testing.T, db *sql.DB, globalID, mediaType string, local bool, peerID, seriesName string) {
	t.Helper()
	var peer any
	if peerID != "" {
		peer = peerID
	}
	_, err := db.Exec(`
		INSERT INTO catalog_items (global_id, name, media_type, local, primary_peer_id, series_global_id, series_name, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, 0)
	`, globalID, globalID, mediaType, local, peer, seriesName, seriesName)
	if err != nil {
		t.Fatalf("seed %s: %v", globalID, err)
	}
}

func TestItemsHandlerOwnersFilter(t *testing.T) {
	db := testDB(t)
	seedOwned(t, db, "m-local", "movie", true, "", "")
	seedOwned(t, db, "m-alice", "movie", false, "alice", "")
	seedOwned(t, db, "m-bob", "movie", false, "bob", "")
	// Show A: local + alice episodes; Show B: bob only.
	seedOwned(t, db, "a-e1", "episode", true, "", "Show A")
	seedOwned(t, db, "a-e2", "episode", false, "alice", "Show A")
	seedOwned(t, db, "a-e3", "episode", false, "alice", "Show A")
	seedOwned(t, db, "b-e1", "episode", false, "bob", "Show B")

	movieCases := map[string]int{
		"":                  3,
		"&owners=local":     1,
		"&owners=alice,bob": 2,
		"&owners=local,bob": 2,
		"&owners=":          0,
		"&owners=carol":     0,
	}
	for q, want := range movieCases {
		page := getItemsPage(t, db, "/api/v1/items?type=movie"+q)
		if page.Total != want || len(page.Items) != want {
			t.Errorf("movies %q: total=%d items=%d, want %d", q, page.Total, len(page.Items), want)
		}
	}

	page := getSeriesSummaryPage(t, db, "/api/v1/items?type=series&owners=bob")
	if page.Total != 1 || len(page.Items) != 1 || page.Items[0].SeriesName != "Show B" {
		t.Errorf("series owners=bob: %+v", page)
	}

	page = getSeriesSummaryPage(t, db, "/api/v1/items?type=series&owners=alice")
	if page.Total != 1 || page.Items[0].SeriesName != "Show A" || page.Items[0].TotalCount != 2 || page.Items[0].LocalCount != 0 {
		t.Errorf("series owners=alice: %+v", page)
	}

	page = getSeriesSummaryPage(t, db, "/api/v1/items?type=series&owners=")
	if page.Total != 0 || len(page.Items) != 0 {
		t.Errorf("series owners=: %+v", page)
	}
}

func TestItemsHandlerReportsHiddenAndPending(t *testing.T) {
	db := testDB(t)
	seedMovies(t, db, 2)
	seedSeries(t, db, "Show A", 2)

	for _, key := range []string{"movie-00", "series:Show A"} {
		if err := SetHidden(t.Context(), db, key, true); err != nil {
			t.Fatalf("hiding %s: %v", key, err)
		}
	}
	if err := SetHidden(t.Context(), db, "series:Show A-e00", true); err == nil {
		t.Error("hiding a single episode succeeded, want an error")
	}

	page := getItemsPage(t, db, "/api/v1/items?type=movie")
	got := map[string]itemDTO{}
	for _, it := range page.Items {
		got[it.GlobalID] = it
	}
	if m := got["movie-00"]; !m.Hidden || !m.HidePending {
		t.Errorf("movie-00 = %+v, want hidden and pending", m)
	}
	if m := got["movie-01"]; m.Hidden || m.HidePending {
		t.Errorf("movie-01 = %+v, want visible", m)
	}

	// Applied by a sync: no longer pending.
	if _, err := db.Exec(`UPDATE catalog_items SET hidden = 1 WHERE global_id LIKE 'series:%'`); err != nil {
		t.Fatal(err)
	}
	series := getSeriesSummaryPage(t, db, "/api/v1/items?type=series")
	if s := series.Items[0]; !s.Hidden || s.HidePending {
		t.Errorf("series = %+v, want hidden, not pending", s)
	}
}
