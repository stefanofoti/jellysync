// Package synctrigger serves the manual "sync now" endpoint: POST
// /api/v1/sync/trigger/{peerID}. peerID "local" (the only form this node's
// own dashboard needs) re-reads this node's Jellyfin index and syncs
// (scan 2), mirroring internal/proxy's local/forward split. Any other peerID
// is a remote sync: by default just a pull of the peers' cached catalogs
// (scan 3), leaving that peer untouched; with force=true it first asks that
// peer's node to refresh its own catalog from its Jellyfin, waits for it to
// finish, then pulls. With resync=true the pull re-downloads that peer's
// whole catalog instead of only the changes since the last pull (see
// catalog.RequestResync). No path ever asks a Jellyfin to scan its files on
// disk.
package synctrigger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"jellysync/internal/logging"
	"jellysync/internal/peers"
	"jellysync/internal/syncstatus"
)

const (
	remoteHTTPTimeout  = 10 * time.Second
	remotePollInterval = 3 * time.Second
	remotePollTimeout  = 30 * time.Minute
)

// Handler serves POST /api/v1/sync/trigger/{peerID}?force=true&resync=true.
// triggerLocal queues a sync that refreshes this node's own catalog from its
// Jellyfin; triggerPull queues a pull-only sync (peer catalogs, no local
// refresh). Both should be non-blocking. requestResync marks a peer for a
// full resync on its next pull. force and resync only matter for a remote
// peerID.
func Handler(registry *peers.Registry, tracker *syncstatus.Tracker, triggerLocal, triggerPull func(), requestResync func(ctx context.Context, peerID string) error) http.HandlerFunc {
	client := &http.Client{Timeout: remoteHTTPTimeout, Transport: registry.Transport()}

	return func(w http.ResponseWriter, r *http.Request) {
		peerID := r.PathValue("peerID")
		force := r.URL.Query().Get("force") == "true"
		resync := r.URL.Query().Get("resync") == "true"

		if peerID == "" || peerID == "local" {
			slog.Info("local sync requested", "from", r.RemoteAddr)
			triggerLocal()
			w.WriteHeader(http.StatusAccepted)
			return
		}

		p, found := registry.Get(peerID)
		if !found {
			slog.Warn("remote sync requested for unknown peer", "peer", peerID)
			http.Error(w, "unknown peer", http.StatusNotFound)
			return
		}
		if p.State == peers.StateDisabled {
			http.Error(w, "peer disabled: its transport is turned off on this node", http.StatusConflict)
			return
		}

		if resync {
			if err := requestResync(r.Context(), p.ID); err != nil {
				slog.Error("requesting full resync", "peer", p.ID, logging.Err(err))
				http.Error(w, "requesting resync failed", http.StatusInternalServerError)
				return
			}
			slog.Info("full resync of peer catalog requested", "peer", p.ID)
		}
		go triggerRemote(client, tracker, p, force, triggerPull)
		w.WriteHeader(http.StatusAccepted)
	}
}

func triggerRemote(client *http.Client, tracker *syncstatus.Tracker, p peers.Peer, force bool, triggerPull func()) {
	started := time.Now()
	tracker.Set(p.ID, syncstatus.Status{State: syncstatus.StateRunning, StartedAt: started})
	slog.Info("remote sync started", "peer", p.ID, "url", p.URL, "force", force)

	if !force {
		triggerPull()
		waitForLocalRun(tracker, p.ID, started)
		return
	}

	req, err := http.NewRequest(http.MethodPost, p.URL+"/api/v1/sync/trigger/local", nil)
	if err == nil {
		var resp *http.Response
		resp, err = client.Do(req)
		if err == nil {
			resp.Body.Close()
			switch resp.StatusCode {
			case http.StatusAccepted:
			case http.StatusTooManyRequests:
				// A paired peer limits how often each client may make it
				// re-read its library (see app.forceRefreshEvery).
				err = errors.New("peer refused: a refresh was requested too recently, try again later")
			default:
				err = fmt.Errorf("peer returned status %d", resp.StatusCode)
			}
		}
	}
	if err != nil {
		unreachable(tracker, p.ID, started, err)
		return
	}

	if !pollRemoteStatus(client, tracker, p, started) {
		return
	}
	// The peer notifies us if its catalog changed, but pull regardless so a
	// forced sync always ends with a fresh copy of its catalog.
	triggerPull()
}

// waitForLocalRun mirrors this node's own "local" sync status onto peerID
// once a run that started after `started` has finished, so a pull-only remote
// sync shows progress and outcome on the dashboard.
func waitForLocalRun(tracker *syncstatus.Tracker, peerID string, started time.Time) {
	deadline := time.Now().Add(remotePollTimeout)
	for time.Now().Before(deadline) {
		time.Sleep(time.Second)
		st, ok := tracker.Get("local")
		if !ok || st.StartedAt.Before(started) {
			continue
		}
		if st.State == syncstatus.StateSuccess || st.State == syncstatus.StateError {
			st.StartedAt = started
			tracker.Set(peerID, st)
			slog.Info("remote sync finished", "peer", peerID, "state", st.State, "duration", time.Since(started).Round(time.Millisecond))
			return
		}
		st.StartedAt = started
		tracker.Set(peerID, st)
	}
	unreachable(tracker, peerID, started, fmt.Errorf("timed out waiting for the pull to finish"))
}

// pollRemoteStatus repeatedly reads the peer's own /api/v1/sync/status
// until its "local" entry reports success or error, mirroring that status
// (re-stamped with our own StartedAt) onto our tracker under p.ID. It returns
// true if the peer's sync succeeded.
func pollRemoteStatus(client *http.Client, tracker *syncstatus.Tracker, p peers.Peer, started time.Time) bool {
	deadline := time.Now().Add(remotePollTimeout)
	var lastStage syncstatus.Stage
	for time.Now().Before(deadline) {
		time.Sleep(remotePollInterval)

		req, err := http.NewRequest(http.MethodGet, p.URL+"/api/v1/sync/status", nil)
		if err != nil {
			slog.Error("building remote sync status request", "peer", p.ID, logging.Err(err))
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			unreachable(tracker, p.ID, started, err)
			return false
		}
		var all map[string]syncstatus.Status
		decErr := json.NewDecoder(resp.Body).Decode(&all)
		resp.Body.Close()
		if decErr != nil {
			slog.Warn("decoding remote sync status", "peer", p.ID, logging.Err(decErr))
			continue
		}

		remoteLocal, ok := all["local"]
		if !ok {
			continue
		}
		remoteLocal.StartedAt = started
		tracker.Set(p.ID, remoteLocal)
		if remoteLocal.Stage != lastStage {
			slog.Debug("remote sync progress", "peer", p.ID, "stage", remoteLocal.Stage, "percent", remoteLocal.Percent)
			lastStage = remoteLocal.Stage
		}
		switch remoteLocal.State {
		case syncstatus.StateSuccess:
			slog.Info("remote sync finished", "peer", p.ID, "duration", time.Since(started).Round(time.Millisecond))
			return true
		case syncstatus.StateError:
			slog.Warn("remote sync failed on the peer", "peer", p.ID, "duration", time.Since(started).Round(time.Millisecond), "peer_error", remoteLocal.Error)
			return false
		}
	}
	unreachable(tracker, p.ID, started, fmt.Errorf("timed out waiting for peer sync status"))
	return false
}

func unreachable(tracker *syncstatus.Tracker, peerID string, started time.Time, err error) {
	slog.Warn("remote sync: peer unreachable", "peer", peerID, "duration", time.Since(started).Round(time.Millisecond), logging.Err(err))
	tracker.Set(peerID, syncstatus.Status{
		State: syncstatus.StateUnreachable, StartedAt: started, FinishedAt: time.Now(), Error: err.Error(),
	})
}
