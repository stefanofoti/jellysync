package metrics

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Outcome classifies how a proxied stream request ended.
type Outcome string

const (
	OutcomeOK           Outcome = "ok"
	OutcomeClientClosed Outcome = "client_closed"  // the player went away (seek, stop, probe done)
	OutcomeUpstreamErr  Outcome = "upstream_error" // upstream answered with a status >= 400
	OutcomeTimeout      Outcome = "timeout"        // dial or response headers timed out
	OutcomeUnreachable  Outcome = "unreachable"    // upstream couldn't be reached at all
	OutcomeInterrupted  Outcome = "interrupted"    // upstream broke off mid-transfer
)

// StreamTimings is where one stream request's startup time went. A zero
// field is a phase that didn't happen (Connect on a reused connection) or
// wasn't reached.
type StreamTimings struct {
	Connect   time.Duration // TCP connect to upstream
	TTFB      time.Duration // request written to first response byte
	FirstBody time.Duration // response headers to first body byte (disk read, on the serving side)
	Startup   time.Duration // request received to first body byte available to relay
}

// latencyBuckets are the histogram upper bounds, in seconds: fine at the
// LAN end, coarse up to the proxy's 10s response header timeout and past
// it for disk spin-ups.
var latencyBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}

type histogram struct {
	counts []uint64 // per bucket, not cumulative
	count  uint64
	sum    float64
}

func (h *histogram) observe(d time.Duration) {
	if h.counts == nil {
		h.counts = make([]uint64, len(latencyBuckets))
	}
	s := d.Seconds()
	for i, le := range latencyBuckets {
		if s <= le {
			h.counts[i]++
			break
		}
	}
	h.count++
	h.sum += s
}

type streamKey struct {
	peerID string
	dir    Direction
}

type streamStats struct {
	active                            int64
	outcomes                          map[Outcome]uint64
	connect, ttfb, firstBody, startup histogram
}

// streams holds stream request counters and latency histograms. Unlike
// traffic totals they're in memory only and survive DELETE /api/v1/traffic,
// as Prometheus counters must only go up between restarts.
type streams struct {
	mu    sync.Mutex
	stats map[streamKey]*streamStats
}

func (s *streams) get(peerID string, dir Direction) *streamStats {
	if s.stats == nil {
		s.stats = make(map[streamKey]*streamStats)
	}
	k := streamKey{peerID, dir}
	st, ok := s.stats[k]
	if !ok {
		st = &streamStats{outcomes: make(map[Outcome]uint64)}
		s.stats[k] = st
	}
	return st
}

// StreamStarted counts a proxied stream request as in flight. Every call
// must be paired with StreamEnded.
func (c *Collector) StreamStarted(peerID string, dir Direction) {
	c.streams.mu.Lock()
	defer c.streams.mu.Unlock()
	c.streams.get(peerID, dir).active++
}

// StreamEnded counts a stream request as finished with outcome o.
func (c *Collector) StreamEnded(peerID string, dir Direction, o Outcome) {
	c.streams.mu.Lock()
	defer c.streams.mu.Unlock()
	st := c.streams.get(peerID, dir)
	st.active--
	st.outcomes[o]++
}

// ObserveStartup records a stream request's startup phases in the latency
// histograms; zero phases are skipped.
func (c *Collector) ObserveStartup(peerID string, dir Direction, t StreamTimings) {
	c.streams.mu.Lock()
	defer c.streams.mu.Unlock()
	st := c.streams.get(peerID, dir)
	for _, p := range []struct {
		h *histogram
		d time.Duration
	}{{&st.connect, t.Connect}, {&st.ttfb, t.TTFB}, {&st.firstBody, t.FirstBody}, {&st.startup, t.Startup}} {
		if p.d > 0 {
			p.h.observe(p.d)
		}
	}
}

// writeStreamMetrics appends the stream request metrics to b in
// OpenMetrics text format, in a stable order.
func (c *Collector) writeStreamMetrics(b *strings.Builder) {
	c.streams.mu.Lock()
	defer c.streams.mu.Unlock()

	keys := make([]streamKey, 0, len(c.streams.stats))
	for k := range c.streams.stats {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].peerID != keys[j].peerID {
			return keys[i].peerID < keys[j].peerID
		}
		return keys[i].dir < keys[j].dir
	})
	labels := func(k streamKey) string {
		return fmt.Sprintf("peer_id=%q,direction=%q", k.peerID, k.dir)
	}

	b.WriteString("# TYPE jellysync_streams_active gauge\n")
	b.WriteString("# HELP jellysync_streams_active Stream requests currently being relayed, per peer and direction.\n")
	for _, k := range keys {
		fmt.Fprintf(b, "jellysync_streams_active{%s} %d\n", labels(k), c.streams.stats[k].active)
	}

	b.WriteString("# TYPE jellysync_stream_requests counter\n")
	b.WriteString("# HELP jellysync_stream_requests Finished stream requests per peer, direction and outcome.\n")
	for _, k := range keys {
		outcomes := c.streams.stats[k].outcomes
		names := make([]string, 0, len(outcomes))
		for o := range outcomes {
			names = append(names, string(o))
		}
		sort.Strings(names)
		for _, o := range names {
			fmt.Fprintf(b, "jellysync_stream_requests_total{%s,outcome=%q} %d\n", labels(k), o, outcomes[Outcome(o)])
		}
	}

	for _, h := range []struct {
		name, help string
		get        func(*streamStats) *histogram
	}{
		{"jellysync_stream_connect_seconds", "TCP connect time to the stream upstream, new connections only.", func(s *streamStats) *histogram { return &s.connect }},
		{"jellysync_stream_ttfb_seconds", "Time from the upstream request being written to its first response byte.", func(s *streamStats) *histogram { return &s.ttfb }},
		{"jellysync_stream_first_body_seconds", "Time from upstream response headers to the first body byte.", func(s *streamStats) *histogram { return &s.firstBody }},
		{"jellysync_stream_startup_seconds", "Time from a stream request arriving to its first body byte being ready to relay.", func(s *streamStats) *histogram { return &s.startup }},
	} {
		fmt.Fprintf(b, "# TYPE %s histogram\n", h.name)
		fmt.Fprintf(b, "# HELP %s %s\n", h.name, h.help)
		for _, k := range keys {
			hist := h.get(c.streams.stats[k])
			if hist.count == 0 {
				continue
			}
			var cum uint64
			for i, le := range latencyBuckets {
				cum += hist.counts[i]
				fmt.Fprintf(b, "%s_bucket{%s,le=\"%s\"} %d\n", h.name, labels(k), strconv.FormatFloat(le, 'g', -1, 64), cum)
			}
			fmt.Fprintf(b, "%s_bucket{%s,le=\"+Inf\"} %d\n", h.name, labels(k), hist.count)
			fmt.Fprintf(b, "%s_count{%s} %d\n", h.name, labels(k), hist.count)
			fmt.Fprintf(b, "%s_sum{%s} %f\n", h.name, labels(k), hist.sum)
		}
	}
}
