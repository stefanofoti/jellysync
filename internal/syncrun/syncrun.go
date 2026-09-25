// Package syncrun runs this node's own sync (Jellyfin library refresh,
// catalog.Sync, strm.Reconcile) and reports its progress to a
// syncstatus.Tracker under the "local" key. It's used by both the scheduled
// loop and the manual "sync now" trigger so the two stay identical.
package syncrun

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"jellysync/internal/catalog"
	"jellysync/internal/config"
	"jellysync/internal/jellyfin"
	"jellysync/internal/peers"
	"jellysync/internal/strm"
	"jellysync/internal/syncstatus"
)

const (
	scanPollInterval = 2 * time.Second

	// scanStartGrace bounds how long we wait for Jellyfin to report the
	// scan as "Running" before giving up and treating it as already
	// finished: some servers complete near-instant scans of small
	// libraries before the first poll ever observes the Running state.
	scanStartGrace = 10 * time.Second
)

// RunLocal refreshes this node's own Jellyfin library, then re-runs
// catalog.Sync and strm.Reconcile so the refreshed library is reflected and
// re-offered to peers. tracker's "local" entry is updated throughout.
func RunLocal(ctx context.Context, db *sql.DB, jf *jellyfin.Client, registry *peers.Registry, strmCfg config.Strm, tracker *syncstatus.Tracker) error {
	started := time.Now()
	tracker.Set("local", syncstatus.Status{State: syncstatus.StateRunning, StartedAt: started})

	if err := jf.RefreshLibrary(ctx); err != nil {
		err = fmt.Errorf("refreshing jellyfin library: %w", err)
		fail(tracker, started, err)
		return err
	}

	if err := pollScan(ctx, jf, tracker, started); err != nil {
		fail(tracker, started, err)
		return err
	}

	if err := catalog.Sync(ctx, db, jf, registry, strmCfg.OutputDir); err != nil {
		err = fmt.Errorf("catalog sync: %w", err)
		fail(tracker, started, err)
		return err
	}
	if err := strm.Reconcile(ctx, db, jf, strmCfg); err != nil {
		err = fmt.Errorf("strm reconcile: %w", err)
		fail(tracker, started, err)
		return err
	}

	tracker.Set("local", syncstatus.Status{
		State: syncstatus.StateSuccess, Percent: 100,
		StartedAt: started, FinishedAt: time.Now(),
	})
	return nil
}

// pollScan watches Jellyfin's own scheduled-tasks API for the library scan
// task's progress, updating tracker as it goes, until the scan is no longer
// running.
func pollScan(ctx context.Context, jf *jellyfin.Client, tracker *syncstatus.Tracker, started time.Time) error {
	seenRunning := false
	for {
		percent, running, err := jf.LibraryScanProgress(ctx)
		switch {
		case err != nil:
			// Best-effort: a flaky scheduled-tasks call shouldn't abort
			// the sync, just leave progress stale until the next poll.
			log.Printf("syncrun: checking scan progress: %v", err)
		case running:
			seenRunning = true
			tracker.Set("local", syncstatus.Status{State: syncstatus.StateRunning, Percent: percent, StartedAt: started})
		case seenRunning, time.Since(started) > scanStartGrace:
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(scanPollInterval):
		}
	}
}

func fail(tracker *syncstatus.Tracker, started time.Time, err error) {
	tracker.Set("local", syncstatus.Status{
		State: syncstatus.StateError, StartedAt: started, FinishedAt: time.Now(), Error: err.Error(),
	})
}
