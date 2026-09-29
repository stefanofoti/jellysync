// Package jellyfin is a thin client for a single local Jellyfin instance.
package jellyfin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

type Client struct {
	baseURL string
	apiKey  string
	nodeID  string
	http    *http.Client
}

func New(baseURL, apiKey, nodeID string, timeout time.Duration) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		nodeID:  nodeID,
		http: &http.Client{
			Timeout: timeout,
		},
	}
}

// authHeader builds the standard Authorization header. Jellyfin 12.x ignores
// the legacy X-Emby-Token header entirely, so every authenticated request
// must go through this instead.
func (c *Client) authHeader() string {
	return fmt.Sprintf(
		`MediaBrowser Client="jellysync", Device="jellysync", DeviceId="jellysync-%s", Version="1.0.0", Token="%s"`,
		c.nodeID, c.apiKey,
	)
}

// NewRequest builds an authenticated request against this Jellyfin instance.
// Callers that need to stream the response body (the proxy handler) should
// use this directly rather than one of the higher-level helpers below.
func (c *Client) NewRequest(ctx context.Context, method, path string, query url.Values) (*http.Request, error) {
	u := c.baseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", c.authHeader())
	return req, nil
}

// Ping verifies the local Jellyfin instance is reachable and responding.
func (c *Client) Ping(ctx context.Context) error {
	req, err := c.NewRequest(ctx, http.MethodGet, "/System/Info/Public", nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("jellyfin unreachable: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("jellyfin returned status %d", resp.StatusCode)
	}
	return nil
}

// NewDownloadRequest builds an authenticated request for an item's raw file.
// The caller is responsible for setting a Range header before sending it and
// for streaming the response body without buffering it fully in memory.
func (c *Client) NewDownloadRequest(ctx context.Context, itemID string) (*http.Request, error) {
	return c.NewRequest(ctx, http.MethodGet, "/Items/"+itemID+"/Download", nil)
}

type CatalogItem struct {
	ItemID        string
	Name          string
	Year          int
	MediaType     string // movie, episode, series
	Path          string
	TmdbID        string
	TvdbID        string
	ImdbID        string
	TitleYearHash string

	// Episode-only fields, populated by joining against this same
	// response's Series items (see buildCatalogItems). Zero-valued for
	// movies and series roots.
	SeriesGlobalID string
	SeriesName     string
	SeasonNumber   int
	EpisodeNumber  int
}

// GlobalID is the identifier used for cross-node dedup: the first available
// provider ID, falling back to a normalized title+year hash.
func (c CatalogItem) GlobalID() string {
	switch {
	case c.TmdbID != "":
		return "tmdb:" + c.TmdbID
	case c.TvdbID != "":
		return "tvdb:" + c.TvdbID
	case c.ImdbID != "":
		return "imdb:" + c.ImdbID
	default:
		return "titleyear:" + c.TitleYearHash
	}
}

type itemsResponse struct {
	Items []rawItem `json:"Items"`
}

type rawItem struct {
	Id                string            `json:"Id"`
	Name              string            `json:"Name"`
	ProductionYear    int               `json:"ProductionYear"`
	Type              string            `json:"Type"`
	Path              string            `json:"Path"`
	ProviderIds       map[string]string `json:"ProviderIds"`
	SeriesId          string            `json:"SeriesId"`
	SeriesName        string            `json:"SeriesName"`
	ParentIndexNumber int               `json:"ParentIndexNumber"`
	IndexNumber       int               `json:"IndexNumber"`

	// LocationType/IsVirtualItem exist to filter out placeholder items
	// injected by plugins like Jellyseerr's "request" integration (e.g. a
	// fake "favorite this to request it" movie). Those have no backing
	// file, so treating them as real media would produce a .strm that can
	// never stream. See isRealMedia.
	LocationType  string `json:"LocationType"`
	IsVirtualItem bool   `json:"IsVirtualItem"`
}

// isRealMedia reports whether an item is backed by an actual file rather
// than being a virtual placeholder (e.g. Jellyseerr's "request this" items,
// or unaired episodes). Movies and episodes are additionally required to
// have a Path; series roots are exempt since they're folders, not files.
//
// This deliberately doesn't also check Jellyfin's MediaSources field: that
// would catch a narrower case (a FileSystem-located item Jellyfin hasn't
// finished probing yet), but asking Jellyfin for MediaSources on every item
// in a recursive listing is dramatically more expensive than the fields
// here, which are plain columns Jellyfin already has indexed — and this
// runs on every sync cycle, not just occasionally.
func isRealMedia(ri rawItem) bool {
	if ri.IsVirtualItem {
		return false
	}
	if ri.LocationType != "" && ri.LocationType != "FileSystem" {
		return false
	}
	switch strings.ToLower(ri.Type) {
	case "movie", "episode":
		if ri.Path == "" {
			return false
		}
	}
	return true
}

var nonAlnum = regexp.MustCompile(`[^a-z0-9]+`)

// titleYearHash is the dedup fallback for items with no provider ID.
// Episodes fold in their series/season/episode position rather than just
// name+year: two different shows can both have a same-year episode titled
// "Pilot", and without the series/position in the hash they'd collide on
// the same global_id (catalog_items' primary key).
func titleYearHash(name string, year int) string {
	return hashParts(fmt.Sprintf("%s_%d", normalize(name), year))
}

func episodeHash(seriesGlobalID string, season, episode int) string {
	return hashParts(fmt.Sprintf("%s_%d_%d", seriesGlobalID, season, episode))
}

func normalize(name string) string {
	normalized := nonAlnum.ReplaceAllString(strings.ToLower(name), "-")
	return strings.Trim(normalized, "-")
}

func hashParts(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:16]
}

// ListItems fetches the local movie/episode/series catalog.
func (c *Client) ListItems(ctx context.Context) ([]CatalogItem, error) {
	started := time.Now()
	slog.Debug("listing jellyfin library")
	req, err := c.NewRequest(ctx, http.MethodGet, "/Items", url.Values{
		"Recursive":        {"true"},
		"IncludeItemTypes": {"Movie,Episode,Series"},
		"Fields":           {"Path,SeriesId,SeriesName,ParentIndexNumber,IndexNumber,ProviderIds"},
	})
	if err != nil {
		return nil, err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("listing items: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("listing items: status %d", resp.StatusCode)
	}

	var parsed itemsResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decoding items: %w", err)
	}

	items := buildCatalogItems(parsed.Items)
	slog.Debug("listed jellyfin library", "raw_items", len(parsed.Items), "real_media", len(items),
		"skipped_virtual", len(parsed.Items)-len(items), "duration", time.Since(started).Round(time.Millisecond))
	return items, nil
}

// buildCatalogItems converts raw Jellyfin items into CatalogItems, joining
// each Episode against its Series (from the same listing) by Jellyfin's
// local SeriesId to attach a cross-node-stable SeriesGlobalID plus
// season/episode numbers.
func buildCatalogItems(raw []rawItem) []CatalogItem {
	seriesGlobalIDs := make(map[string]string, len(raw)) // local SeriesId -> GlobalID
	for _, ri := range raw {
		if strings.ToLower(ri.Type) != "series" || !isRealMedia(ri) {
			continue
		}
		seriesGlobalIDs[ri.Id] = CatalogItem{
			Name:          ri.Name,
			Year:          ri.ProductionYear,
			TmdbID:        ri.ProviderIds["Tmdb"],
			TvdbID:        ri.ProviderIds["Tvdb"],
			ImdbID:        ri.ProviderIds["Imdb"],
			TitleYearHash: titleYearHash(ri.Name, ri.ProductionYear),
		}.GlobalID()
	}

	items := make([]CatalogItem, 0, len(raw))
	for _, ri := range raw {
		if !isRealMedia(ri) {
			continue
		}
		item := CatalogItem{
			ItemID:    ri.Id,
			Name:      ri.Name,
			Year:      ri.ProductionYear,
			MediaType: strings.ToLower(ri.Type),
			Path:      ri.Path,
			TmdbID:    ri.ProviderIds["Tmdb"],
			TvdbID:    ri.ProviderIds["Tvdb"],
			ImdbID:    ri.ProviderIds["Imdb"],
		}

		if item.MediaType == "episode" {
			item.SeriesGlobalID = seriesGlobalIDs[ri.SeriesId]
			item.SeriesName = ri.SeriesName
			item.SeasonNumber = ri.ParentIndexNumber
			item.EpisodeNumber = ri.IndexNumber
			item.TitleYearHash = episodeHash(item.SeriesGlobalID, item.SeasonNumber, item.EpisodeNumber)
		} else {
			item.TitleYearHash = titleYearHash(item.Name, item.Year)
		}

		items = append(items, item)
	}
	return items
}
