package metrics

import (
	"strings"
	"testing"
	"time"
)

func TestWriteStreamMetrics(t *testing.T) {
	var c Collector
	c.StreamStarted("peer-a", In)
	c.StreamStarted("peer-a", In)
	c.ObserveStartup("peer-a", In, StreamTimings{ConnWait: 400 * time.Millisecond, TTFB: 20 * time.Millisecond, FirstBody: 7 * time.Second, Startup: 8 * time.Second})
	c.ObserveMaxReadWait("peer-a", In, 3*time.Second)
	c.ObserveMaxReadWait("peer-a", In, 0)
	c.StreamEnded("peer-a", In, OutcomeClientClosed)

	var b strings.Builder
	c.writeStreamMetrics(&b)
	out := b.String()
	for _, want := range []string{
		`jellysync_streams_active{peer_id="peer-a",direction="in"} 1`,
		`jellysync_stream_requests_total{peer_id="peer-a",direction="in",outcome="client_closed"} 1`,
		`jellysync_stream_ttfb_seconds_bucket{peer_id="peer-a",direction="in",le="0.01"} 0`,
		`jellysync_stream_ttfb_seconds_bucket{peer_id="peer-a",direction="in",le="0.025"} 1`,
		`jellysync_stream_first_body_seconds_bucket{peer_id="peer-a",direction="in",le="5"} 0`,
		`jellysync_stream_first_body_seconds_bucket{peer_id="peer-a",direction="in",le="10"} 1`,
		`jellysync_stream_first_body_seconds_bucket{peer_id="peer-a",direction="in",le="+Inf"} 1`,
		`jellysync_stream_startup_seconds_count{peer_id="peer-a",direction="in"} 1`,
		`jellysync_stream_conn_wait_seconds_bucket{peer_id="peer-a",direction="in",le="0.25"} 0`,
		`jellysync_stream_conn_wait_seconds_bucket{peer_id="peer-a",direction="in",le="0.5"} 1`,
		`jellysync_stream_max_read_wait_seconds_bucket{peer_id="peer-a",direction="in",le="2.5"} 0`,
		`jellysync_stream_max_read_wait_seconds_bucket{peer_id="peer-a",direction="in",le="5"} 1`,
		// A zero stall isn't observed.
		`jellysync_stream_max_read_wait_seconds_count{peer_id="peer-a",direction="in"} 1`,
	} {
		if !strings.Contains(out, want+"\n") {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	// Connect was zero (a reused connection): no samples, only the header.
	if strings.Contains(out, "jellysync_stream_connect_seconds_count") {
		t.Errorf("connect histogram has samples, want none:\n%s", out)
	}
}
