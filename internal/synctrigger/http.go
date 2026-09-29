// Package synctrigger serves the manual "sync now" endpoint: POST
// /api/v1/sync/trigger/{peerID}. peerID "local" (the only form this node's
// own dashboard needs) wakes the scheduled sync loop immediately, mirroring
// internal/proxy's local/forward split; any other peerID asks that peer's
// own node to run its local sync, then polls that peer for status until it
// finishes so its progress bar can be shown here too.
package synctrigger

import (
	"encoding/json"
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

// Handler serves POST /api/v1/sync/trigger/{peerID}?force=true. triggerLocal
// is called to wake the scheduled sync loop for peerID "local"; it should be
// a non-blocking send (e.g. on a buffered size-1 channel). force controls
// whether that sync also tells Jellyfin to rescan its library on disk
// first, rather than just reading its current index — see syncrun.RunLocal.
func Handler(registry *peers.Registry, tracker *syncstatus.Tracker, triggerLocal func(force bool)) http.HandlerFunc {
	client := &http.Client{Timeout: remoteHTTPTimeout}

	return func(w http.ResponseWriter, r *http.Request) {
		peerID := r.PathValue("peerID")
		force := r.URL.Query().Get("force") == "true"

		if peerID == "" || peerID == "local" {
			slog.Info("local sync requested", "force_jellyfin_rescan", force, "from", r.RemoteAddr)
			triggerLocal(force)
			w.WriteHeader(http.StatusAccepted)
			return
		}

		p, found := registry.Get(peerID)
		if !found {
			slog.Warn("remote sync requested for unknown peer", "peer", peerID)
			http.Error(w, "unknown peer", http.StatusNotFound)
			return
		}

		go triggerRemote(client, tracker, p, force)
		w.WriteHeader(http.StatusAccepted)
	}
}

func triggerRemote(client *http.Client, tracker *syncstatus.Tracker, p peers.Peer, force bool) {
	started := time.Now()
	tracker.Set(p.ID, syncstatus.Status{State: syncstatus.StateRunning, StartedAt: started})
	slog.Info("remote sync started", "peer", p.ID, "url", p.URL, "force_jellyfin_rescan", force)

	url := p.URL + "/api/v1/sync/trigger/local"
	if force {
		url += "?force=true"
	}
	req, err := http.NewRequest(http.MethodPost, url, nil)
	if err == nil {
		var resp *http.Response
		resp, err = client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode != http.StatusAccepted {
				err = fmt.Errorf("peer returned status %d", resp.StatusCode)
			}
		}
	}
	if err != nil {
		unreachable(tracker, p.ID, started, err)
		return
	}

	pollRemoteStatus(client, tracker, p, started)
}

// pollRemoteStatus repeatedly reads the peer's own /api/v1/sync/status
// until its "local" entry reports success or error, mirroring that status
// (re-stamped with our own StartedAt) onto our tracker under p.ID.
func pollRemoteStatus(client *http.Client, tracker *syncstatus.Tracker, p peers.Peer, started time.Time) {
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
			return
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
			return
		case syncstatus.StateError:
			slog.Warn("remote sync failed on the peer", "peer", p.ID, "duration", time.Since(started).Round(time.Millisecond), "peer_error", remoteLocal.Error)
			return
		}
	}
	unreachable(tracker, p.ID, started, fmt.Errorf("timed out waiting for peer sync status"))
}

func unreachable(tracker *syncstatus.Tracker, peerID string, started time.Time, err error) {
	slog.Warn("remote sync: peer unreachable", "peer", peerID, "duration", time.Since(started).Round(time.Millisecond), logging.Err(err))
	tracker.Set(peerID, syncstatus.Status{
		State: syncstatus.StateUnreachable, StartedAt: started, FinishedAt: time.Now(), Error: err.Error(),
	})
}
