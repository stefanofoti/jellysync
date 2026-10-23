package proxy

import (
	"net/http"
	"testing"
)

func TestStreamID(t *testing.T) {
	r, _ := http.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set(streamIDHeader, "0123abcd")
	if got := streamID(r); got != "0123abcd" {
		t.Errorf("streamID with a valid header = %q, want it kept", got)
	}
	r.Header.Set(streamIDHeader, "bad id\n")
	if got := streamID(r); got == "bad id\n" || len(got) != 16 {
		t.Errorf("streamID with an invalid header = %q, want a fresh 16-char id", got)
	}
}

func TestTotalSize(t *testing.T) {
	for _, tc := range []struct {
		contentRange string
		length       int64
		want         int64
	}{
		{"bytes 100-199/5000", 100, 5000},
		{"bytes 0-99/*", 100, -1},
		{"", 5000, 5000},
	} {
		resp := &http.Response{Header: http.Header{}, ContentLength: tc.length}
		if tc.contentRange != "" {
			resp.Header.Set("Content-Range", tc.contentRange)
		}
		if got := totalSize(resp); got != tc.want {
			t.Errorf("totalSize(%q, %d) = %d, want %d", tc.contentRange, tc.length, got, tc.want)
		}
	}
}

func TestBurstTracker(t *testing.T) {
	var b burstTracker
	if n := b.hit("p/a"); n != 1 {
		t.Errorf("first hit = %d, want 1", n)
	}
	if n := b.hit("p/a"); n != 2 {
		t.Errorf("second hit = %d, want 2", n)
	}
	if n := b.hit("p/b"); n != 1 {
		t.Errorf("other item's first hit = %d, want 1", n)
	}
}
