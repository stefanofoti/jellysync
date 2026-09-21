package config

import (
	"fmt"
	"os"
	"strings"
)

type Config struct {
	NodeID     string
	ListenAddr string
	DBPath     string
	Jellyfin   Jellyfin
	Strm       Strm
	Peers      []Peer
}

type Jellyfin struct {
	URL    string
	APIKey string
}

type Strm struct {
	OutputDir string
	// BaseURL is how the LOCAL Jellyfin instance reaches this jellysync
	// node, e.g. "http://jellysync:8080" when they're separate containers
	// on the same compose network. Written into every .strm file.
	BaseURL string
}

type Peer struct {
	ID  string
	URL string
}

// Load builds the Config from environment variables. jellysync always
// listens on :8080 inside its container — expose it on a different host
// port via docker-compose's port mapping instead of changing this.
func Load() (*Config, error) {
	cfg := &Config{
		NodeID:     os.Getenv("NODE_ID"),
		ListenAddr: ":8080",
		DBPath:     envOr("DB_PATH", "./data/jellysync.db"),
		Jellyfin: Jellyfin{
			URL:    os.Getenv("JELLYFIN_URL"),
			APIKey: os.Getenv("JELLYFIN_API_KEY"),
		},
		Strm: Strm{
			OutputDir: os.Getenv("OUTPUT_DIR"),
			BaseURL:   os.Getenv("BASE_URL"),
		},
	}

	peers, err := parsePeers(os.Getenv("PEERS"))
	if err != nil {
		return nil, err
	}
	cfg.Peers = peers

	if cfg.NodeID == "" {
		return nil, fmt.Errorf("NODE_ID is required")
	}
	if cfg.Jellyfin.URL == "" {
		return nil, fmt.Errorf("JELLYFIN_URL is required")
	}
	if cfg.Jellyfin.APIKey == "" {
		return nil, fmt.Errorf("JELLYFIN_API_KEY is required")
	}
	if cfg.Strm.OutputDir == "" {
		return nil, fmt.Errorf("OUTPUT_DIR is required")
	}
	if cfg.Strm.BaseURL == "" {
		return nil, fmt.Errorf("BASE_URL is required")
	}

	return cfg, nil
}

// parsePeers parses PEERS, a comma-separated "id=url,id=url" list of
// starting peers. The dashboard's Peers list is the same data either way,
// and peers added there persist across restarts even if never set here.
func parsePeers(raw string) ([]Peer, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	var peers []Peer
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		id, url, ok := strings.Cut(entry, "=")
		if !ok || id == "" || url == "" {
			return nil, fmt.Errorf("PEERS: invalid entry %q, expected id=url", entry)
		}
		peers = append(peers, Peer{ID: id, URL: url})
	}
	return peers, nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
