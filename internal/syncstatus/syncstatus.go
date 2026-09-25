// Package syncstatus tracks the in-progress/last-result state of manual and
// scheduled syncs, per peer ("local" for this node's own sync). It is
// transient, in-memory only: a sync's progress isn't meaningful across a
// restart, unlike catalog_items or peers.
package syncstatus

import (
	"sync"
	"time"
)

type State string

const (
	StateIdle        State = "idle"
	StateRunning     State = "running"
	StateSuccess     State = "success"
	StateError       State = "error"
	StateUnreachable State = "unreachable"
)

type Status struct {
	State      State     `json:"state"`
	Percent    int       `json:"percent"`
	StartedAt  time.Time `json:"started_at,omitempty"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
	Error      string    `json:"error,omitempty"`
}

// Tracker holds the last known Status per peer ID ("local" included).
type Tracker struct {
	mu       sync.RWMutex
	statuses map[string]Status
}

func NewTracker() *Tracker {
	return &Tracker{statuses: make(map[string]Status)}
}

func (t *Tracker) Set(id string, s Status) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.statuses[id] = s
}

func (t *Tracker) Get(id string) (Status, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	s, ok := t.statuses[id]
	return s, ok
}

// All returns a snapshot copy of every tracked status.
func (t *Tracker) All() map[string]Status {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[string]Status, len(t.statuses))
	for k, v := range t.statuses {
		out[k] = v
	}
	return out
}
