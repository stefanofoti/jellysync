// Package logging configures the process-wide structured logger (log/slog).
// Every package logs through slog's default logger with key=value
// attributes, so a line like
//
//	level=INFO msg="peer pull finished" peer=alice changes=12 duration=340ms
//
// can be grepped by message or filtered by attribute. The standard library
// "log" package is routed through the same handler at INFO, so nothing
// bypasses the configured level or format.
package logging

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// DefaultLevel is used when LOG_LEVEL is unset: verbose by default, since
// jellysync is a small, quiet background service whose logs are mostly
// read when something needs troubleshooting.
const DefaultLevel = slog.LevelDebug

// ParseLevel accepts DEBUG, INFO, WARN/WARNING or ERROR, case-insensitively.
// An empty string yields DefaultLevel.
func ParseLevel(s string) (slog.Level, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "":
		return DefaultLevel, nil
	case "DEBUG":
		return slog.LevelDebug, nil
	case "INFO":
		return slog.LevelInfo, nil
	case "WARN", "WARNING":
		return slog.LevelWarn, nil
	case "ERROR":
		return slog.LevelError, nil
	}
	return 0, fmt.Errorf("LOG_LEVEL: unknown level %q (want DEBUG, INFO, WARN or ERROR)", s)
}

// Setup installs a text handler on stderr at level as slog's default logger.
func Setup(level slog.Level) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
}

// Err is the conventional attribute for an error value.
func Err(err error) slog.Attr {
	return slog.Any("err", err)
}
