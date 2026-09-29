// Package syncrun runs this node's own sync (Jellyfin library refresh,
// catalog.Sync, strm.Reconcile) and reports its progress to a
// syncstatus.Tracker under the "local" key. It's used by both the scheduled
// loop and the manual "sync now" trigger so the two stay identical.
package syncrun

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"jellysync/internal/catalog"
	"jellysync/internal/config"
	"jellysync/internal/jellyfin"
	"jellysync/internal/logging"
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
	// Reason says what asked for the run, for logs: one of the Reason*
	// constants, or several joined by "+" once merged.
	Reason string
}

// Reasons a sync runs.
const (
	ReasonStartup     = "startup"
	ReasonScheduled   = "scheduled"
	ReasonManual      = "manual"
	ReasonManualForce = "manual-force"
	ReasonPeerNotify  = "peer-notify"
)

// Merge returns a request doing everything either one asks for.
func (r Request) Merge(o Request) Request {
	reason := r.Reason
	switch {
	case reason == "":
		reason = o.Reason
	case o.Reason != "" && !slices.Contains(strings.Split(reason, "+"), o.Reason):
		reason += "+" + o.Reason
	}
	return Request{ForceScan: r.ForceScan || o.ForceScan, RefreshLocal: r.RefreshLocal || o.RefreshLocal, Reason: reason}
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
		slog.Debug("sync already queued, merging request", "queued", q.pending.Reason, "new", r.Reason)
		r = q.pending.Merge(r)
	} else {
		slog.Debug("sync queued", "reason", r.Reason, "force_scan", r.ForceScan, "refresh_local", r.RefreshLocal)
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
	refreshLocal := req.RefreshLocal || req.ForceScan
	log := slog.With("reason", req.Reason)
	log.Info("sync started", "force_jellyfin_rescan", req.ForceScan, "refresh_local", refreshLocal)

	if req.ForceScan {
		tracker.Set("local", syncstatus.Status{State: syncstatus.StateRunning, Stage: syncstatus.StageScanning, StartedAt: started})

		log.Info("jellyfin library rescan started")
		if err := jf.RefreshLibrary(ctx); err != nil {
			return fail(log, tracker, started, "jellyfin rescan", fmt.Errorf("refreshing jellyfin library: %w", err))
		}
		if err := pollScan(ctx, jf, tracker, started); err != nil {
			return fail(log, tracker, started, "jellyfin rescan", err)
		}
		log.Info("jellyfin library rescan finished", "duration", time.Since(started).Round(time.Millisecond))
	}

	tracker.Set("local", syncstatus.Status{State: syncstatus.StateRunning, Stage: syncstatus.StageCatalog, StartedAt: started})
	catalogStarted := time.Now()
	st, err := catalog.Sync(ctx, db, ix, registry, refreshLocal, peerFetchTimeout, selfID)
	if st.LocalRefreshed {
		log.Info("local catalog refreshed",
			"items", st.Local.Total, "added", st.Local.Added, "updated", st.Local.Updated, "removed", st.Local.Removed,
			"jellyfin_listing", st.Local.ListDuration.Round(time.Millisecond))
	}
	if st.Local.Changed() > 0 {
		log.Info("local catalog changed, notifying peers", "changes", st.Local.Changed())
		catalog.NotifyPeers(registry, selfID)
	}
	if err != nil {
		return fail(log, tracker, started, "catalog sync", fmt.Errorf("catalog sync: %w", err))
	}
	peersOK, peersFailed, remoteChanges := 0, 0, 0
	for _, p := range st.Peers {
		if p.Err != nil {
			peersFailed++
			continue
		}
		peersOK++
		remoteChanges += p.Changes()
	}
	log.Info("catalog synced",
		"peers_ok", peersOK, "peers_failed", peersFailed, "peers_skipped", st.PeersSkipped, "remote_changes", remoteChanges,
		"items_local", st.Election.Local, "items_remote", st.Election.Remote, "items_changed", st.Election.Upserted,
		"orphans_cleared", st.Election.OrphansCleared, "duration", time.Since(catalogStarted).Round(time.Millisecond))

	tracker.Set("local", syncstatus.Status{State: syncstatus.StateRunning, Stage: syncstatus.StageWriting, StartedAt: started})
	strmStarted := time.Now()
	files, err := strm.Reconcile(ctx, db, jf, strmCfg)
	if err != nil {
		return fail(log, tracker, started, "writing .strm files", fmt.Errorf("strm reconcile: %w", err))
	}
	log.Info(".strm files reconciled",
		"written", files.Written, "moved", files.Moved, "removed", files.Removed, "unchanged", files.Unchanged,
		"failed", files.Failed, "jellyfin_rescan_requested", files.LibraryRefreshed, "duration", time.Since(strmStarted).Round(time.Millisecond))

	tracker.Set("local", syncstatus.Status{
		State: syncstatus.StateSuccess, Percent: 100,
		StartedAt: started, FinishedAt: time.Now(),
	})
	finished := log.Info
	if peersFailed > 0 || files.Failed > 0 {
		finished = log.Warn
	}
	finished("sync finished", "duration", time.Since(started).Round(time.Millisecond),
		"peers_failed", peersFailed, "strm_failed", files.Failed)
	return nil
}

// pollScan watches Jellyfin's own scheduled-tasks API for the library scan
// task's progress, updating tracker as it goes, until the scan is no longer
// running.
func pollScan(ctx context.Context, jf *jellyfin.Client, tracker *syncstatus.Tracker, started time.Time) error {
	seenRunning := false
	lastPercent := -1
	for {
		percent, running, err := jf.LibraryScanProgress(ctx)
		switch {
		case err != nil:
			// Best-effort: a flaky scheduled-tasks call shouldn't abort
			// the sync, just leave progress stale until the next poll.
			slog.Warn("checking jellyfin rescan progress", logging.Err(err))
		case running:
			seenRunning = true
			if percent != lastPercent {
				slog.Debug("jellyfin library rescan progress", "percent", percent)
				lastPercent = percent
			}
			tracker.Set("local", syncstatus.Status{State: syncstatus.StateRunning, Stage: syncstatus.StageScanning, Percent: percent, StartedAt: started})
		case seenRunning:
			return nil
		case time.Since(started) > scanStartGrace:
			slog.Debug("jellyfin never reported the rescan as running; assuming it already finished", "grace", scanStartGrace)
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(scanPollInterval):
		}
	}
}

// fail records err as the run's outcome, logs it, and returns it.
func fail(log *slog.Logger, tracker *syncstatus.Tracker, started time.Time, stage string, err error) error {
	tracker.Set("local", syncstatus.Status{
		State: syncstatus.StateError, StartedAt: started, FinishedAt: time.Now(), Error: err.Error(),
	})
	log.Error("sync failed", "stage", stage, "duration", time.Since(started).Round(time.Millisecond), logging.Err(err))
	return err
}
