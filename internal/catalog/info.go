package catalog

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"time"

	"jellysync/internal/peers"
)

// itemInfo is GET /api/v1/items/info: one catalog item as the dashboard
// lists it, plus the media details catalog_items doesn't keep and every
// copy any source offers, so the user can see why a copy was elected.
type itemInfo struct {
	Item itemDTO `json:"item"`
	// Year and provider IDs, from the elected copy's entry (else the first
	// offer's).
	Year   int        `json:"year,omitempty"`
	TmdbID string     `json:"tmdb_id,omitempty"`
	TvdbID string     `json:"tvdb_id,omitempty"`
	ImdbID string     `json:"imdb_id,omitempty"`
	Offers []offerDTO `json:"offers"`
}

// offerDTO is one source's copy of an item: this node's own library
// ("local") or a peer's mirrored catalog.
type offerDTO struct {
	Source string `json:"source"`
	ItemID string `json:"item_id"`
	// Path is the file on Source's own filesystem; empty from older peers.
	Path string `json:"path,omitempty"`
	// Elected: this is the copy catalog_items points at.
	Elected bool `json:"elected"`
	// Hidden: a local copy the user hid, so peers aren't offered it.
	Hidden bool `json:"hidden,omitempty"`
	// ExcludedFolder: Path lies under a folder excluded for this peer, so
	// the copy only counts if nothing else offers the item.
	ExcludedFolder bool `json:"excluded_folder,omitempty"`
	// PeerState and Offering describe a peer source as of now (see
	// peers.Peer.Offering): a copy of a peer that isn't offering doesn't
	// take part in the election.
	PeerState string `json:"peer_state,omitempty"`
	Offering  bool   `json:"offering"`
}

// ItemInfoHandler serves GET /api/v1/items/info?global_id=...: 404 if the
// item isn't in catalog_items. list returns the registered peers.
func ItemInfoHandler(db *sql.DB, list func() []peers.Peer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("global_id")
		if id == "" {
			http.Error(w, "missing global_id", http.StatusBadRequest)
			return
		}
		ctx := r.Context()

		rows, err := db.QueryContext(ctx, `SELECT `+itemColumns+` FROM catalog_items WHERE global_id = ?`, id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		items, err := scanItemRows(rows)
		rows.Close()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if len(items) == 0 {
			http.Error(w, "no such item", http.StatusNotFound)
			return
		}
		info := itemInfo{Item: items[0], Offers: []offerDTO{}}

		var details *Entry
		var raw string
		var hidden bool
		err = db.QueryRowContext(ctx, `SELECT entry, hidden FROM local_catalog WHERE global_id = ? AND deleted = 0`, id).Scan(&raw, &hidden)
		switch {
		case errors.Is(err, sql.ErrNoRows):
		case err != nil:
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		default:
			var e Entry
			if err := json.Unmarshal([]byte(raw), &e); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			info.Offers = append(info.Offers, offerDTO{
				Source: "local", ItemID: e.ItemID, Path: e.Path,
				Elected: info.Item.Local, Hidden: hidden, Offering: true,
			})
			details = &e
		}

		excluded, err := excludedFolders(ctx, db)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		known := make(map[string]peers.Peer)
		for _, p := range list() {
			known[p.ID] = p
		}
		now := time.Now()
		rows, err = db.QueryContext(ctx, `SELECT peer_id, entry FROM peer_catalog WHERE global_id = ? ORDER BY peer_id`, id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()
		for rows.Next() {
			var peerID string
			if err := rows.Scan(&peerID, &raw); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			var e Entry
			if err := json.Unmarshal([]byte(raw), &e); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			o := offerDTO{
				Source: peerID, ItemID: e.ItemID, Path: e.Path,
				Elected:        !info.Item.Local && info.Item.PrimaryPeerID == peerID,
				ExcludedFolder: underAnyFolder(e.Path, excluded[peerID]),
			}
			if p, ok := known[peerID]; ok {
				o.PeerState, o.Offering = string(p.State), p.Offering(now)
			}
			if o.Elected || details == nil {
				details = &e
			}
			info.Offers = append(info.Offers, o)
		}
		if err := rows.Err(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// Elected first, then local, then peers by id.
		sort.SliceStable(info.Offers, func(i, j int) bool { return info.Offers[i].Elected && !info.Offers[j].Elected })

		if details != nil {
			info.Year, info.TmdbID, info.TvdbID, info.ImdbID = details.Year, details.TmdbID, details.TvdbID, details.ImdbID
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(info)
	}
}
