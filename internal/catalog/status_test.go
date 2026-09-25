package catalog

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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
			updated_at       INTEGER NOT NULL
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

func TestItemsHandlerSeriesPagination_KeepsEpisodesTogether(t *testing.T) {
	db := testDB(t)
	seedSeries(t, db, "Show A", 3)
	seedSeries(t, db, "Show B", 2)
	seedSeries(t, db, "Show C", 4)

	page := getItemsPage(t, db, "/api/v1/items?type=series&limit=1&offset=0")
	if page.Total != 3 {
		t.Errorf("total = %d, want 3", page.Total)
	}
	if len(page.Items) != 3 {
		t.Fatalf("Show A has 3 episodes, all must land on its page; got %d items", len(page.Items))
	}
	for _, it := range page.Items {
		if it.SeriesName != "Show A" {
			t.Errorf("page 0 leaked episode from series %q", it.SeriesName)
		}
	}

	page2 := getItemsPage(t, db, "/api/v1/items?type=series&limit=1&offset=1")
	if len(page2.Items) != 2 {
		t.Fatalf("Show B has 2 episodes; got %d items", len(page2.Items))
	}
	for _, it := range page2.Items {
		if it.SeriesName != "Show B" {
			t.Errorf("page 1 leaked episode from series %q", it.SeriesName)
		}
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
