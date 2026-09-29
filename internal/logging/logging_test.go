package logging

import (
	"log/slog"
	"testing"
)

func TestParseLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"":        slog.LevelDebug,
		"debug":   slog.LevelDebug,
		" INFO ":  slog.LevelInfo,
		"Warn":    slog.LevelWarn,
		"warning": slog.LevelWarn,
		"ERROR":   slog.LevelError,
	}
	for in, want := range cases {
		got, err := ParseLevel(in)
		if err != nil || got != want {
			t.Errorf("ParseLevel(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	if _, err := ParseLevel("verbose"); err == nil {
		t.Error("ParseLevel(\"verbose\") succeeded, want error")
	}
}
