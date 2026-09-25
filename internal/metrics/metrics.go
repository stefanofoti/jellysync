// Package metrics tracks per-peer traffic (bytes transferred, current
// bitrate) and exposes it to the dashboard and as OpenMetrics text.
package metrics

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// sampleInterval is how often current bitrate is recomputed and a
	// history point is recorded.
	sampleInterval = 5 * time.Second
	// historyLen * sampleInterval is how far back the chart data goes.
	historyLen = 120 // 10 minutes at 5s

	// flushInterval is how often accumulated totals are persisted to the
	// peer_traffic table. Persisting on every byte would thrash SQLite, so
	// totals live in memory between flushes and are only ever behind by
	// at most flushInterval worth of traffic if the process dies.
	flushInterval = 10 * time.Second
)

type Direction string

const (
	In  Direction = "in"
	Out Direction = "out"
)

// Sample is one point in a peer's bitrate history.
type Sample struct {
	T      int64   `json:"t"`
	InBps  float64 `json:"in_bps"`
	OutBps float64 `json:"out_bps"`
}

// PeerTraffic is the traffic snapshot for a single peer.
type PeerTraffic struct {
	TotalIn     int64    `json:"total_in_bytes"`
	TotalOut    int64    `json:"total_out_bytes"`
	CurrentIn   float64  `json:"current_in_bps"`
	CurrentOut  float64  `json:"current_out_bps"`
	History     []Sample `json:"history"`
}

type counters struct {
	totalIn   int64 // persisted, atomic
	totalOut  int64 // persisted, atomic
	windowIn  int64 // bytes since last sample tick, atomic
	windowOut int64 // bytes since last sample tick, atomic

	mu         sync.Mutex
	currentIn  float64
	currentOut float64
	history    []Sample
}

// Collector accumulates per-peer transfer byte counts, derives a current
// bitrate from them on a fixed tick, and periodically persists cumulative
// totals so they survive a restart.
type Collector struct {
	db *sql.DB

	mu    sync.RWMutex
	peers map[string]*counters
}

func NewCollector(ctx context.Context, db *sql.DB) (*Collector, error) {
	c := &Collector{db: db, peers: make(map[string]*counters)}

	rows, err := db.QueryContext(ctx, `SELECT peer_id, direction, bytes FROM peer_traffic`)
	if err != nil {
		return nil, fmt.Errorf("loading peer traffic: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var peerID, direction string
		var n int64
		if err := rows.Scan(&peerID, &direction, &n); err != nil {
			return nil, fmt.Errorf("scanning peer traffic: %w", err)
		}
		cnt := c.peerCounters(peerID)
		if Direction(direction) == In {
			atomic.StoreInt64(&cnt.totalIn, n)
		} else {
			atomic.StoreInt64(&cnt.totalOut, n)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("loading peer traffic: %w", err)
	}

	return c, nil
}

func (c *Collector) peerCounters(peerID string) *counters {
	c.mu.RLock()
	cnt, ok := c.peers[peerID]
	c.mu.RUnlock()
	if ok {
		return cnt
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if cnt, ok = c.peers[peerID]; ok {
		return cnt
	}
	cnt = &counters{}
	c.peers[peerID] = cnt
	return cnt
}

// RecordBytes attributes n transferred bytes to peerID in the given
// direction. Safe for concurrent use by every in-flight proxy stream.
func (c *Collector) RecordBytes(peerID string, dir Direction, n int64) {
	if peerID == "" || n <= 0 {
		return
	}
	cnt := c.peerCounters(peerID)
	if dir == In {
		atomic.AddInt64(&cnt.totalIn, n)
		atomic.AddInt64(&cnt.windowIn, n)
	} else {
		atomic.AddInt64(&cnt.totalOut, n)
		atomic.AddInt64(&cnt.windowOut, n)
	}
}

// Run ticks every sampleInterval to compute current bitrate and append to
// history, and every flushInterval to persist cumulative totals. Blocks
// until ctx is cancelled.
func (c *Collector) Run(ctx context.Context) {
	sampleTicker := time.NewTicker(sampleInterval)
	defer sampleTicker.Stop()
	flushTicker := time.NewTicker(flushInterval)
	defer flushTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.flush(context.Background())
			return
		case <-sampleTicker.C:
			c.sample()
		case <-flushTicker.C:
			c.flush(ctx)
		}
	}
}

func (c *Collector) sample() {
	now := time.Now().Unix()
	secs := sampleInterval.Seconds()

	c.mu.RLock()
	snapshot := make(map[string]*counters, len(c.peers))
	for id, cnt := range c.peers {
		snapshot[id] = cnt
	}
	c.mu.RUnlock()

	for _, cnt := range snapshot {
		in := atomic.SwapInt64(&cnt.windowIn, 0)
		out := atomic.SwapInt64(&cnt.windowOut, 0)
		inBps := float64(in) / secs
		outBps := float64(out) / secs

		cnt.mu.Lock()
		cnt.currentIn = inBps
		cnt.currentOut = outBps
		cnt.history = append(cnt.history, Sample{T: now, InBps: inBps, OutBps: outBps})
		if len(cnt.history) > historyLen {
			cnt.history = cnt.history[len(cnt.history)-historyLen:]
		}
		cnt.mu.Unlock()
	}
}

func (c *Collector) flush(ctx context.Context) {
	c.mu.RLock()
	snapshot := make(map[string]*counters, len(c.peers))
	for id, cnt := range c.peers {
		snapshot[id] = cnt
	}
	c.mu.RUnlock()

	for peerID, cnt := range snapshot {
		in := atomic.LoadInt64(&cnt.totalIn)
		out := atomic.LoadInt64(&cnt.totalOut)
		if _, err := c.db.ExecContext(ctx, `
			INSERT INTO peer_traffic (peer_id, direction, bytes) VALUES (?, 'in', ?)
			ON CONFLICT(peer_id, direction) DO UPDATE SET bytes = excluded.bytes
		`, peerID, in); err != nil {
			continue
		}
		c.db.ExecContext(ctx, `
			INSERT INTO peer_traffic (peer_id, direction, bytes) VALUES (?, 'out', ?)
			ON CONFLICT(peer_id, direction) DO UPDATE SET bytes = excluded.bytes
		`, peerID, out)
	}
}

// Snapshot returns the current traffic view for every peer that has ever
// transferred bytes.
func (c *Collector) Snapshot() map[string]PeerTraffic {
	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make(map[string]PeerTraffic, len(c.peers))
	for id, cnt := range c.peers {
		cnt.mu.Lock()
		history := make([]Sample, len(cnt.history))
		copy(history, cnt.history)
		pt := PeerTraffic{
			TotalIn:    atomic.LoadInt64(&cnt.totalIn),
			TotalOut:   atomic.LoadInt64(&cnt.totalOut),
			CurrentIn:  cnt.currentIn,
			CurrentOut: cnt.currentOut,
			History:    history,
		}
		cnt.mu.Unlock()
		out[id] = pt
	}
	return out
}
