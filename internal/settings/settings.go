// Package settings persists small user-configurable node settings (currently
// just the sync interval) in the settings key-value table, so they survive
// restarts and can be changed at runtime from the dashboard.
package settings

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"
)

const syncIntervalKey = "sync_interval_seconds"

// DefaultSyncInterval is used when no value has been saved yet.
const DefaultSyncInterval = 2 * time.Hour

// GetSyncInterval reads the configured sync interval, falling back to
// DefaultSyncInterval if unset.
func GetSyncInterval(ctx context.Context, db *sql.DB) (time.Duration, error) {
	var value string
	err := db.QueryRowContext(ctx, `SELECT value FROM settings WHERE key = ?`, syncIntervalKey).Scan(&value)
	if err == sql.ErrNoRows {
		return DefaultSyncInterval, nil
	}
	if err != nil {
		return 0, fmt.Errorf("reading sync interval: %w", err)
	}
	seconds, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("parsing sync interval %q: %w", value, err)
	}
	return time.Duration(seconds) * time.Second, nil
}

// SetSyncInterval persists the sync interval.
func SetSyncInterval(ctx context.Context, db *sql.DB, d time.Duration) error {
	if d <= 0 {
		return fmt.Errorf("sync interval must be positive")
	}
	_, err := db.ExecContext(ctx, `
		INSERT INTO settings (key, value) VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value
	`, syncIntervalKey, strconv.Itoa(int(d.Seconds())))
	if err != nil {
		return fmt.Errorf("saving sync interval: %w", err)
	}
	return nil
}
