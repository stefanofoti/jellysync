package config

import "testing"

func setRequired(t *testing.T) {
	t.Helper()
	for k, v := range map[string]string{
		"NODE_ID": "n", "JELLYFIN_URL": "http://jf", "JELLYFIN_API_KEY": "k",
		"OUTPUT_DIR": "/out", "BASE_URL": "http://js:8080",
	} {
		t.Setenv(k, v)
	}
}

// Without the new variables a node behaves like earlier versions: legacy
// peers on, mTLS off.
func TestTransportDefaults(t *testing.T) {
	setRequired(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.HTTPPeers || cfg.MTLSPeers || cfg.PublicURL != "" {
		t.Fatalf("defaults: http_peers=%v mtls_peers=%v public_url=%q", cfg.HTTPPeers, cfg.MTLSPeers, cfg.PublicURL)
	}
}

func TestTransportFlags(t *testing.T) {
	setRequired(t)
	t.Setenv("HTTP_PEERS", "false")
	t.Setenv("MTLS_PEERS", "true")
	t.Setenv("PUBLIC_URL", "https://Alice.example.org")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPPeers || !cfg.MTLSPeers || cfg.PublicURL != "https://alice.example.org:443" {
		t.Fatalf("got http_peers=%v mtls_peers=%v public_url=%q", cfg.HTTPPeers, cfg.MTLSPeers, cfg.PublicURL)
	}

	t.Setenv("MTLS_PEERS", "yes please")
	if _, err := Load(); err == nil {
		t.Fatal("invalid boolean accepted")
	}
}

// https:// peers in PEERS are legacy peers (e.g. behind a reverse proxy),
// accepted as before.
func TestPeersAcceptsHTTPS(t *testing.T) {
	setRequired(t)
	t.Setenv("PEERS", "bob=https://bob.example.org,carol=http://10.0.0.3:8098")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Peers) != 2 || cfg.Peers[0].URL != "https://bob.example.org" {
		t.Fatalf("peers = %+v", cfg.Peers)
	}
}
