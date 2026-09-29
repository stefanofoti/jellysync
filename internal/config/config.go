package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"jellysync/internal/logging"
)

// defaultStreamBufferKB is the read/write buffer size used to relay
// proxied stream bytes (see internal/proxy). Both sides of that relay are
// network sockets, so Go can't use sendfile/splice — a bigger buffer means
// fewer, larger syscalls per second for a given bitrate.
const defaultStreamBufferKB = 256

// defaultJellyfinTimeoutSec bounds every individual request this node makes
// to its own local Jellyfin, including catalog.Sync's recursive /Items
// listing, which runs every sync cycle and is the slowest of these on a
// large library or constrained hardware. Generous rather than tight so
// a large library on constrained hardware still fits.
const defaultJellyfinTimeoutSec = 240

// defaultPeerFetchTimeoutSec bounds how long this node waits for a peer's
// GET /api/v1/catalog to respond. That handler runs the peer's own
// (possibly slow, same as defaultJellyfinTimeoutSec above) Jellyfin
// listing, so this needs its own separate budget rather than reusing the
// local Jellyfin timeout.
const defaultPeerFetchTimeoutSec = 60

type Config struct {
	NodeID     string
	ListenAddr string
	DBPath     string
	Jellyfin   Jellyfin
	Strm       Strm
	Peers      []Peer
	// StreamBufferKB is the proxy relay's copy buffer size, in KiB.
	StreamBufferKB int
	// JellyfinTimeout bounds every request to the local Jellyfin instance.
	JellyfinTimeout time.Duration
	// PeerFetchTimeout bounds each peer's GET /api/v1/catalog request during sync.
	PeerFetchTimeout time.Duration
	// LogLevel is the minimum level logged (LOG_LEVEL, default DEBUG).
	LogLevel slog.Level
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

	streamBufferKB, err := envIntOr("STREAM_BUFFER_KB", defaultStreamBufferKB)
	if err != nil {
		return nil, err
	}
	if streamBufferKB <= 0 {
		return nil, fmt.Errorf("STREAM_BUFFER_KB must be positive, got %d", streamBufferKB)
	}
	cfg.StreamBufferKB = streamBufferKB

	jellyfinTimeoutSec, err := envIntOr("JELLYFIN_TIMEOUT_SEC", defaultJellyfinTimeoutSec)
	if err != nil {
		return nil, err
	}
	if jellyfinTimeoutSec <= 0 {
		return nil, fmt.Errorf("JELLYFIN_TIMEOUT_SEC must be positive, got %d", jellyfinTimeoutSec)
	}
	cfg.JellyfinTimeout = time.Duration(jellyfinTimeoutSec) * time.Second

	peerFetchTimeoutSec, err := envIntOr("PEER_FETCH_TIMEOUT_SEC", defaultPeerFetchTimeoutSec)
	if err != nil {
		return nil, err
	}
	if peerFetchTimeoutSec <= 0 {
		return nil, fmt.Errorf("PEER_FETCH_TIMEOUT_SEC must be positive, got %d", peerFetchTimeoutSec)
	}
	cfg.PeerFetchTimeout = time.Duration(peerFetchTimeoutSec) * time.Second

	logLevel, err := logging.ParseLevel(os.Getenv("LOG_LEVEL"))
	if err != nil {
		return nil, err
	}
	cfg.LogLevel = logLevel

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

func envIntOr(k string, def int) (int, error) {
	v := os.Getenv(k)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: invalid integer %q", k, v)
	}
	return n, nil
}
