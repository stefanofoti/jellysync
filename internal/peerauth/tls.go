package peerauth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var (
	// ErrPinMismatch means the server presented a key other than the one
	// pinned for it: not the node we paired with (or meant to pair with).
	ErrPinMismatch = errors.New("peer key does not match its pin")
	// ErrUnknownKey is the server-side handshake rejection of a client key
	// that isn't pinned as a client, peer or pending invite.
	ErrUnknownKey = errors.New("unknown client key")
)

// ServerTLSConfig is the TLS config for the peer listener: TLS 1.3 only,
// and a client certificate is mandatory and must carry a key known
// reports as pinned. Anything else fails the handshake, so no HTTP from an
// unpaired caller is ever read. Callers must still check the caller on
// every request (see Authenticate): a key revoked after the handshake can
// keep a connection alive.
func (id *Identity) ServerTLSConfig(known func(pin string) bool) *tls.Config {
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{id.cert},
		ClientAuth:   tls.RequireAnyClientCert,
		NextProtos:   []string{"http/1.1"},
		VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
			pin, err := pinOfRaw(raw)
			if err != nil {
				return err
			}
			if !known(pin) {
				return ErrUnknownKey
			}
			return nil
		},
	}
}

func pinOfRaw(raw [][]byte) (string, error) {
	if len(raw) == 0 {
		return "", errors.New("no certificate presented")
	}
	cert, err := x509.ParseCertificate(raw[0])
	if err != nil {
		return "", fmt.Errorf("parsing presented certificate: %w", err)
	}
	return PinOf(cert), nil
}

// NewTransport returns the transport for every request this node makes to
// a peer. A paired peer's address (one pinFor knows) gets mTLS with id's
// certificate, and the server's key must match the pin. Anything else is
// a legacy peer and is dialed as before: http:// in the clear, https://
// (e.g. behind a reverse proxy) with ordinary CA-verified TLS. HTTP/2 is
// off on purpose: one TCP connection per stream means no head-of-line
// blocking and no shared flow control window between concurrent video
// streams.
func NewTransport(id *Identity, pinFor func(addr string) (string, bool)) *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	// An HTTP proxy would do its own TLS (without our pin check) after
	// CONNECT, so paired peers are always dialed directly.
	t.Proxy = func(r *http.Request) (*url.URL, error) {
		if r.URL.Scheme == "https" {
			if _, pinned := pinFor(canonicalAddr(r.URL)); pinned {
				return nil, nil
			}
		}
		return http.ProxyFromEnvironment(r)
	}
	t.ForceAttemptHTTP2 = false
	t.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	t.DialTLSContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		pin, ok := pinFor(addr)
		if !ok {
			return dialCAVerified(ctx, network, addr)
		}
		if id == nil {
			return nil, errors.New("this node has no identity to authenticate with")
		}
		return dialPinned(ctx, network, addr, id.cert, pin)
	}
	return t
}

// canonicalAddr is the host:port http.Transport dials for u.
func canonicalAddr(u *url.URL) string {
	port := u.Port()
	if port == "" {
		port = "443"
	}
	return net.JoinHostPort(strings.ToLower(u.Hostname()), port)
}

// dialCAVerified is plain TLS with the system's CA roots, as Go's default
// transport does: how legacy https:// peers were always reached.
func dialCAVerified(ctx context.Context, network, addr string) (net.Conn, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	d := &tls.Dialer{
		NetDialer: &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second},
		Config:    &tls.Config{ServerName: host, NextProtos: []string{"http/1.1"}},
	}
	return d.DialContext(ctx, network, addr)
}

func dialPinned(ctx context.Context, network, addr string, cert tls.Certificate, pin string) (net.Conn, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	d := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	raw, err := d.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	conn := tls.Client(raw, &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		ServerName:   host,
		NextProtos:   []string{"http/1.1"},
		// There is no CA to verify against: the pin check below replaces
		// chain and hostname verification entirely.
		InsecureSkipVerify: true,
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 || PinOf(cs.PeerCertificates[0]) != pin {
				return ErrPinMismatch
			}
			return nil
		},
	})
	if err := conn.HandshakeContext(ctx); err != nil {
		raw.Close()
		return nil, fmt.Errorf("tls handshake with %s: %w", addr, err)
	}
	return conn, nil
}

// CanonicalURL normalizes a peer port URL to "https://host:port" (443 if
// no port is given), rejecting anything else: an http:// URL, a path, or
// credentials. Pins are looked up by host:port, so the stored form must be
// canonical.
func CanonicalURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("invalid URL %q: %w", raw, err)
	}
	if u.Scheme != "https" {
		return "", fmt.Errorf("URL %q must be https://", raw)
	}
	if u.Hostname() == "" {
		return "", fmt.Errorf("URL %q has no host", raw)
	}
	if u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("URL %q must be just https://host:port", raw)
	}
	port := u.Port()
	if port == "" {
		port = "443"
	}
	return "https://" + net.JoinHostPort(strings.ToLower(u.Hostname()), port), nil
}

// AddrOf returns the host:port an https:// URL dials, as http.Transport
// passes it to DialTLSContext.
func AddrOf(rawURL string) (string, bool) {
	canon, err := CanonicalURL(rawURL)
	if err != nil {
		return "", false
	}
	return canon[len("https://"):], true
}
