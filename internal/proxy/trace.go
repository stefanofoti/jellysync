package proxy

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync"
	"time"

	"jellysync/internal/metrics"
)

// timings records where a stream request's startup time goes: the
// upstream request's phases from httptrace, then the first body byte.
// httptrace hooks may fire from dialing goroutines, so fields are guarded
// by mu.
type timings struct {
	start time.Time

	mu                        sync.Mutex
	getConn, gotConn          time.Time
	dnsStart, dnsDone         time.Time
	connectStart, connectDone time.Time
	tlsStart, tlsDone         time.Time
	wroteRequest, firstByte   time.Time
	headers, firstBody        time.Time
	reused                    bool
}

func newTimings() *timings { return &timings{start: time.Now()} }

// trace returns the httptrace hooks that fill t in.
func (t *timings) trace() *httptrace.ClientTrace {
	set := func(f *time.Time) {
		t.mu.Lock()
		*f = time.Now()
		t.mu.Unlock()
	}
	return &httptrace.ClientTrace{
		GetConn:      func(string) { set(&t.getConn) },
		DNSStart:     func(httptrace.DNSStartInfo) { set(&t.dnsStart) },
		DNSDone:      func(httptrace.DNSDoneInfo) { set(&t.dnsDone) },
		ConnectStart: func(string, string) { set(&t.connectStart) },
		ConnectDone:  func(string, string, error) { set(&t.connectDone) },
		// Not fired for paired peers: dialPinned does its own handshake,
		// which then shows up only in conn_wait.
		TLSHandshakeStart: func() { set(&t.tlsStart) },
		TLSHandshakeDone:  func(tls.ConnectionState, error) { set(&t.tlsDone) },
		GotConn: func(info httptrace.GotConnInfo) {
			t.mu.Lock()
			t.gotConn = time.Now()
			t.reused = info.Reused
			t.mu.Unlock()
		},
		WroteRequest:         func(httptrace.WroteRequestInfo) { set(&t.wroteRequest) },
		GotFirstResponseByte: func() { set(&t.firstByte) },
	}
}

func (t *timings) gotHeaders()   { t.mu.Lock(); t.headers = time.Now(); t.mu.Unlock() }
func (t *timings) gotFirstBody() { t.mu.Lock(); t.firstBody = time.Now(); t.mu.Unlock() }

func between(from, to time.Time) time.Duration {
	if from.IsZero() || to.IsZero() || to.Before(from) {
		return 0
	}
	return to.Sub(from)
}

// phases is t as durations; zero for a phase that didn't happen.
func (t *timings) phases() metrics.StreamTimings {
	t.mu.Lock()
	defer t.mu.Unlock()
	return metrics.StreamTimings{
		Connect:   between(t.connectStart, t.connectDone),
		TTFB:      between(t.wroteRequest, t.firstByte),
		FirstBody: between(t.headers, t.firstBody),
		Startup:   between(t.start, t.firstBody),
	}
}

// attrs is t as slog attributes, for the phases that happened.
func (t *timings) attrs() []any {
	p := t.phases()
	t.mu.Lock()
	defer t.mu.Unlock()
	ms := func(d time.Duration) time.Duration { return d.Round(time.Millisecond) }
	a := []any{"conn_reused", t.reused}
	for _, ph := range []struct {
		key string
		d   time.Duration
	}{
		{"conn_wait", between(t.getConn, t.gotConn)},
		{"dns", between(t.dnsStart, t.dnsDone)},
		{"connect", p.Connect},
		{"tls", between(t.tlsStart, t.tlsDone)},
		{"ttfb", p.TTFB},
		{"first_body", p.FirstBody},
		{"startup", p.Startup},
	} {
		if ph.d > 0 {
			a = append(a, ph.key, ms(ph.d))
		}
	}
	return a
}

// timedReader wraps an upstream body to time it from the relay's side:
// when the first byte arrives, and the longest single Read. A Read only
// blocks while upstream has nothing to give, so maxWait isolates upstream
// stalls (network, disk) from the player throttling its own reads, which
// shows up between Reads instead. Used from one goroutine only.
type timedReader struct {
	r       io.Reader
	maxWait time.Duration
	first   bool
	onFirst func()
}

func (tr *timedReader) Read(p []byte) (int, error) {
	start := time.Now()
	n, err := tr.r.Read(p)
	if wait := time.Since(start); wait > tr.maxWait {
		tr.maxWait = wait
	}
	if n > 0 && !tr.first {
		tr.first = true
		if tr.onFirst != nil {
			tr.onFirst()
		}
	}
	return n, err
}

// streamID returns the request's X-Jellysync-Stream-Id if the caller sent
// a sane one, so both nodes log the same id for one stream, else a new one.
func streamID(r *http.Request) string {
	if id := r.Header.Get(streamIDHeader); id != "" && len(id) <= 32 && strings.Trim(id, "0123456789abcdef") == "" {
		return id
	}
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// totalSize is the full size of the file behind a stream response: the
// total in Content-Range on a 206, else Content-Length. -1 if unknown.
func totalSize(resp *http.Response) int64 {
	if cr := resp.Header.Get("Content-Range"); cr != "" {
		if i := strings.LastIndexByte(cr, '/'); i >= 0 {
			if n, err := strconv.ParseInt(cr[i+1:], 10, 64); err == nil {
				return n
			}
		}
		return -1
	}
	return resp.ContentLength
}

// burstWindow is how long requests for one item count as one playback
// start: a player probing a file opens several ranges in a row.
const burstWindow = 30 * time.Second

// burstTracker counts stream requests per item within burstWindow of the
// first one, i.e. how many round trips a playback start costs.
type burstTracker struct {
	mu     sync.Mutex
	bursts map[string]*burst
}

type burst struct {
	start time.Time
	n     int
}

// hit records a request for key and returns its position in the current
// burst, 1 for the first.
func (b *burstTracker) hit(key string) int {
	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.bursts == nil {
		b.bursts = make(map[string]*burst)
	}
	for k, v := range b.bursts {
		if now.Sub(v.start) > burstWindow {
			delete(b.bursts, k)
		}
	}
	v, ok := b.bursts[key]
	if !ok {
		v = &burst{start: now}
		b.bursts[key] = v
	}
	v.n++
	return v.n
}
