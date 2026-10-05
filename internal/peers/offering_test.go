package peers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"jellysync/internal/db"
)

func TestOffering(t *testing.T) {
	now := time.Now()
	recent, stale := now.Add(-time.Minute), now.Add(-2*WithdrawAfter)
	cases := []struct {
		state    State
		lastSeen time.Time
		want     bool
	}{
		{StateOnline, time.Time{}, true},
		{StateDegraded, recent, true},
		{StateOffline, recent, true},
		{StateDegraded, stale, false},
		{StateOffline, stale, false},
		{StateOffline, time.Time{}, false},
		{StateDisabled, recent, false},
	}
	for _, c := range cases {
		p := Peer{State: c.state, LastSeen: c.lastSeen}
		if got := p.Offering(now); got != c.want {
			t.Errorf("Offering(%s, last seen %v ago) = %v, want %v", c.state, now.Sub(c.lastSeen), got, c.want)
		}
	}
}

// TestHeartbeatReportsOfferingChanges checks that a heartbeat calls the
// OnOfferingChange hook when a peer unreachable for too long starts or
// stops offering, and not for a mere missed heartbeat.
func TestHeartbeatReportsOfferingChanges(t *testing.T) {
	ctx := context.Background()
	store, err := db.Open(filepath.Join(t.TempDir(), "jellysync.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	var healthy atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !healthy.Load() {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte(`{"node_id":"alice"}`))
	}))
	t.Cleanup(srv.Close)

	r, err := NewRegistry(ctx, store, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var changes atomic.Int32
	r.OnOfferingChange(func() { changes.Add(1) })
	if err := r.AddPeer(ctx, "alice", srv.URL, "alice", "", ""); err != nil {
		t.Fatal(err)
	}

	// A missed heartbeat right after being seen: DEGRADED, still offering.
	r.checkOnce(ctx, "alice")
	if p, _ := r.Get("alice"); p.State != StateDegraded || changes.Load() != 0 {
		t.Fatalf("after one miss: state %s, %d changes; want DEGRADED, 0", p.State, changes.Load())
	}

	// Unreachable for longer than WithdrawAfter: withdrawn.
	r.mu.Lock()
	r.peers["alice"].LastSeen = time.Now().Add(-2 * WithdrawAfter)
	r.mu.Unlock()
	r.checkOnce(ctx, "alice")
	if changes.Load() != 1 {
		t.Fatalf("after grace expired: %d changes, want 1", changes.Load())
	}

	// Back: restored.
	healthy.Store(true)
	r.checkOnce(ctx, "alice")
	if p, _ := r.Get("alice"); p.State != StateOnline || changes.Load() != 2 {
		t.Fatalf("after recovery: state %s, %d changes; want ONLINE, 2", p.State, changes.Load())
	}
}
