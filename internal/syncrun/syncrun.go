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
	"sync"
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

// Request describes one sync run. Requests queued while a run is pending
// merge (see Queue), so the zero value is the cheapest possible run.
type Request struct {
	// ForceScan makes Jellyfin rescan its library on disk first.
	ForceScan bool
	// RefreshLocal re-reads this node's own Jellyfin index. Without it the
	// run only pulls peer changes (e.g. after a peer's change notification)
	// and keeps the local catalog as of the last refresh.
	RefreshLocal bool
}

// Merge returns a request doing everything either one asks for.
func (r Request) Merge(o Request) Request {
	return Request{ForceScan: r.ForceScan || o.ForceScan, RefreshLocal: r.RefreshLocal || o.RefreshLocal}
}

// Queue coalesces sync requests that arrive while one is already pending
// into a single run that does the union of what each asked for — so, e.g.,
// a burst of peer notifications costs one pull, and a manual force-sync
// isn't lost just because a notification got queued first.
type Queue struct {
	mu      sync.Mutex
	pending *Request
	wake    chan struct{}
}

func NewQueue() *Queue {
	return &Queue{wake: make(chan struct{}, 1)}
}

// Push queues r without blocking.
func (q *Queue) Push(r Request) {
	q.mu.Lock()
	if q.pending != nil {
		r = q.pending.Merge(r)
	}
	q.pending = &r
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// C fires when a request is pending; Take then returns it.
func (q *Queue) C() <-chan struct{} { return q.wake }

// Take returns and clears the pending request, if any.
func (q *Queue) Take() (Request, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.pending == nil {
		return Request{}, false
	}
	r := *q.pending
	q.pending = nil
	return r, true
}

// RunLocal re-runs catalog.Sync and strm.Reconcile so this node's catalog
// and .strm files reflect Jellyfin's current index. If req.ForceScan is set, it
// first tells Jellyfin to rescan its library on disk and waits for that to
// finish — otherwise it just reads whatever Jellyfin already has indexed,
// which is far cheaper and is what the scheduled loop uses by default; a
// full rescan is reserved for an explicit "force" sync-now request, since
// forcing Jellyfin to re-walk its whole library every cycle is what made
// routine syncs slow (and prone to the request timing out) even though the
// library itself hadn't changed. tracker's "local" entry is updated
// throughout. If the local catalog changed, peers are notified so they pull
// the delta right away.
func RunLocal(ctx context.Context, db *sql.DB, jf *jellyfin.Client, ix *catalog.Index, registry *peers.Registry, strmCfg config.Strm, peerFetchTimeout time.Duration, req Request, selfID string, tracker *syncstatus.Tracker) error {
	started := time.Now()

	if req.ForceScan {
		tracker.Set("local", syncstatus.Status{State: syncstatus.StateRunning, Stage: syncstatus.StageScanning, StartedAt: started})

		if err := jf.RefreshLibrary(ctx); err != nil {
			err = fmt.Errorf("refreshing jellyfin library: %w", err)
			fail(tracker, started, err)
			return err
		}

		if err := pollScan(ctx, jf, tracker, started); err != nil {
			fail(tracker, started, err)
			return err
		}
	}

	tracker.Set("local", syncstatus.Status{State: syncstatus.StateRunning, Stage: syncstatus.StageCatalog, StartedAt: started})
	localChanges, err := catalog.Sync(ctx, db, ix, registry, req.RefreshLocal || req.ForceScan, peerFetchTimeout)
	if localChanges > 0 {
		catalog.NotifyPeers(registry, selfID)
	}
	if err != nil {
		err = fmt.Errorf("catalog sync: %w", err)
		fail(tracker, started, err)
		return err
	}

	tracker.Set("local", syncstatus.Status{State: syncstatus.StateRunning, Stage: syncstatus.StageWriting, StartedAt: started})
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
			tracker.Set("local", syncstatus.Status{State: syncstatus.StateRunning, Stage: syncstatus.StageScanning, Percent: percent, StartedAt: started})
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
