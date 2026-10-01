package peers

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"github.com/google/uuid"
)

// reservedIDs can never be a peer id: "local" means "this node" in the
// stream proxy and sync-trigger routes, and "unknown" is the owner key
// catalog stats use for remote items with no elected peer.
var reservedIDs = map[string]bool{"local": true, "unknown": true}

var nonSlugChars = regexp.MustCompile(`[^a-z0-9_-]+`)

// slug turns a peer's self-reported node name into something safe to use
// both as a URL path segment and as a directory name under OUTPUT_DIR.
func slug(name string) string {
	s := nonSlugChars.ReplaceAllString(strings.ToLower(strings.TrimSpace(name)), "-")
	return strings.Trim(s, "-_")
}

// idFor picks the registry id for a peer reporting name: its slug, or
// slug_01, slug_02, ... if that is reserved or already in taken. A peer
// reporting no usable name (a build too old to send node_id) gets a UUID.
// Ids are assigned once and kept: a peer that later changes its NODE_ID
// keeps its id, so its .strm files don't move again.
func idFor(name string, taken func(string) bool) string {
	base := slug(name)
	if base == "" {
		return uuid.NewString()
	}
	if !taken(base) && !reservedIDs[base] {
		return base
	}
	for n := 1; ; n++ {
		id := fmt.Sprintf("%s_%02d", base, n)
		if !taken(id) {
			return id
		}
	}
}

// OtherIDs is another set of node ids sharing the registry's namespace:
// the clients that redeemed this node's invites (peerauth.Store). Sharing
// it means traffic, keyed by id, never mixes up two different nodes, and a
// node that is both a client and a peer gets one id for both.
type OtherIDs interface {
	IDTaken(id string) bool
	IDForPin(pin string) (string, bool)
}

// SetOtherIDs makes new ids avoid (or, for the same key, reuse) o's ids.
func (r *Registry) SetOtherIDs(o OtherIDs) {
	r.mu.Lock()
	r.others = o
	r.mu.Unlock()
}

// NewID returns an unused registry id for a peer reporting name. It only
// picks the id; callers then AddPeer with it.
func (r *Registry) NewID(name string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return idFor(name, r.takenLocked)
}

// NewIDForPin is NewID for a peer with key pin: if that key is already
// one of our clients, the peer takes the client's id.
func (r *Registry) NewIDForPin(name, pin string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.others != nil {
		if id, ok := r.others.IDForPin(pin); ok {
			if _, peerTaken := r.peers[id]; !peerTaken {
				return id
			}
		}
	}
	return idFor(name, r.takenLocked)
}

// ClientID picks the id for a new client with key pin and name: the id of
// the peer with that key if there is one, else a fresh one unused by any
// peer or client.
func (r *Registry) ClientID(name, pin string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for id, p := range r.peers {
		if p.Pin != "" && p.Pin == pin {
			return id
		}
	}
	return idFor(name, r.takenLocked)
}

func (r *Registry) takenLocked(id string) bool {
	if _, ok := r.peers[id]; ok {
		return true
	}
	return r.others != nil && r.others.IDTaken(id)
}

// renamePeerID rewrites every column holding peer id oldID to newID.
// newID is not a registered peer, but rows may already exist under it:
// older builds persisted outgoing traffic under the caller's name (which
// is what newID is derived from), so traffic is summed into those rows;
// mirror/cursor rows under newID can only be leftovers of a removed peer,
// so they're dropped before taking over the name.
func renamePeerID(ctx context.Context, tx *sql.Tx, oldID, newID string) error {
	stmts := []string{
		`UPDATE peers SET id = ?2 WHERE id = ?1`,
		`DELETE FROM peer_catalog WHERE peer_id = ?2`,
		`UPDATE peer_catalog SET peer_id = ?2 WHERE peer_id = ?1`,
		`DELETE FROM peer_sync_state WHERE peer_id = ?2`,
		`UPDATE peer_sync_state SET peer_id = ?2 WHERE peer_id = ?1`,
		`INSERT INTO peer_traffic (peer_id, direction, bytes)
			SELECT ?2, direction, bytes FROM peer_traffic WHERE peer_id = ?1 AND true
			ON CONFLICT(peer_id, direction) DO UPDATE SET bytes = bytes + excluded.bytes`,
		`DELETE FROM peer_traffic WHERE peer_id = ?1`,
		`UPDATE catalog_items SET primary_peer_id = ?2 WHERE primary_peer_id = ?1`,
	}
	for _, q := range stmts {
		if _, err := tx.ExecContext(ctx, q, oldID, newID); err != nil {
			return fmt.Errorf("renaming peer %s to %s: %w", oldID, newID, err)
		}
	}
	return nil
}

// migrateUUIDIDs renames peers that still carry a generated UUID id (from
// builds that assigned those) to a name-based id, once their name is
// known. Peers are handled in the order they were added (rowid), so the
// first of several same-named peers gets the plain name. The next
// strm.Reconcile moves their .strm files to the new peer folder. A UUID
// peer whose name isn't known yet is left alone and retried next boot.
func migrateUUIDIDs(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `SELECT id, name FROM peers ORDER BY rowid`)
	if err != nil {
		return fmt.Errorf("loading peer ids: %w", err)
	}
	type peerRow struct{ id, name string }
	var all []peerRow
	taken := make(map[string]bool)
	for rows.Next() {
		var p peerRow
		if err := rows.Scan(&p.id, &p.name); err != nil {
			rows.Close()
			return fmt.Errorf("scanning peer id: %w", err)
		}
		all = append(all, p)
		taken[p.id] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("loading peer ids: %w", err)
	}

	renames := make(map[string]string)
	for _, p := range all {
		if _, err := uuid.Parse(p.id); err != nil || slug(p.name) == "" {
			continue
		}
		newID := idFor(p.name, func(id string) bool { return taken[id] })
		taken[newID] = true
		renames[p.id] = newID
	}
	if len(renames) == 0 {
		return nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for oldID, newID := range renames {
		if err := renamePeerID(ctx, tx, oldID, newID); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("renaming peers: %w", err)
	}
	for oldID, newID := range renames {
		slog.Info("peer id renamed; its .strm files move at the next sync", "from", oldID, "to", newID)
	}
	return nil
}
