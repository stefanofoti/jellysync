// Package syncrun runs this node's own sync (local catalog refresh,
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

// Request describes one sync run. Requests queued while a run is pending
// merge (see Queue), so the zero value is the cheapest possible run.
type Request struct {
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
	ReasonStartup    = "startup"
	ReasonScheduled  = "scheduled"
	ReasonManual     = "manual"
	ReasonPeerNotify = "peer-notify"
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
	return Request{RefreshLocal: r.RefreshLocal || o.RefreshLocal, Reason: reason}
}

// Queue coalesces sync requests that arrive while one is already pending
// into a single run that does the union of what each asked for — so, e.g.,
// a burst of peer notifications costs one pull, and a manual sync
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
		slog.Debug("sync queued", "reason", r.Reason, "refresh_local", r.RefreshLocal)
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
// and .strm files reflect Jellyfin's current index. It only ever reads
// Jellyfin's index (GET /Items); it never asks Jellyfin to scan its files on
// disk — that's Jellyfin's own business. tracker's "local" entry is updated
// throughout. If the local catalog changed, peers are notified so they pull
// the delta right away.
func RunLocal(ctx context.Context, db *sql.DB, jf *jellyfin.Client, ix *catalog.Index, registry *peers.Registry, strmCfg config.Strm, peerFetchTimeout time.Duration, req Request, selfID string, tracker *syncstatus.Tracker) error {
	started := time.Now()
	log := slog.With("reason", req.Reason)
	log.Info("sync started", "refresh_local", req.RefreshLocal)

	tracker.Set("local", syncstatus.Status{State: syncstatus.StateRunning, Stage: syncstatus.StageCatalog, StartedAt: started})
	catalogStarted := time.Now()
	st, err := catalog.Sync(ctx, db, ix, registry, req.RefreshLocal, peerFetchTimeout, selfID)
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
	files, err := strm.Reconcile(ctx, db, strmCfg)
	if err != nil {
		return fail(log, tracker, started, "writing .strm files", fmt.Errorf("strm reconcile: %w", err))
	}
	log.Info(".strm files reconciled",
		"written", files.Written, "moved", files.Moved, "removed", files.Removed, "unchanged", files.Unchanged,
		"failed", files.Failed, "duration", time.Since(strmStarted).Round(time.Millisecond))

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

// fail records err as the run's outcome, logs it, and returns it.
func fail(log *slog.Logger, tracker *syncstatus.Tracker, started time.Time, stage string, err error) error {
	tracker.Set("local", syncstatus.Status{
		State: syncstatus.StateError, StartedAt: started, FinishedAt: time.Now(), Error: err.Error(),
	})
	log.Error("sync failed", "stage", stage, "duration", time.Since(started).Round(time.Millisecond), logging.Err(err))
	return err
}
