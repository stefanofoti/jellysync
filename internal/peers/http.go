package peers

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"jellysync/internal/logging"
	"jellysync/internal/peerauth"
)

type peerDTO struct {
	ID      string `json:"id"`
	Name    string `json:"name,omitempty"`
	URL     string `json:"url"`
	State   string `json:"state"`
	Version string `json:"version,omitempty"`
	IP      string `json:"ip,omitempty"`
	// MTLS is true for a peer paired by invite (reached with mTLS on its
	// peer port), false for a legacy peer added by URL.
	MTLS bool `json:"mtls"`
	// LastSyncAt is when this node last pulled the peer's catalog
	// successfully; absent if it never has.
	LastSyncAt *time.Time `json:"last_sync_at,omitempty"`
}

// ListHandler serves GET /api/v1/peers. lastSync supplies each peer's
// last successful catalog sync (catalog.LastSyncTimes); it's injected
// because the catalog package depends on this one, not the other way round.
func ListHandler(registry *Registry, lastSync func(context.Context) (map[string]time.Time, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		synced, err := lastSync(r.Context())
		if err != nil {
			// Best-effort: the peer list is still useful without it.
			slog.Warn("reading last sync times for peer list", logging.Err(err))
		}
		list := registry.List()
		out := make([]peerDTO, 0, len(list))
		for _, p := range list {
			dto := peerDTO{ID: p.ID, Name: p.Name, URL: p.URL, State: string(p.State), Version: p.Version, IP: p.IP, MTLS: p.Pin != ""}
			if t, ok := synced[p.ID]; ok {
				dto.LastSyncAt = &t
			}
			out = append(out, dto)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
	}
}

// AddHandler serves POST /api/v1/peers with a {"url"} JSON body. The peer's
// display name is read from its own /health "node_id", and its id is
// derived from that name (see idFor) — nothing about identity comes from
// client input.
func AddHandler(registry *Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !registry.HTTPPeers() {
			http.Error(w, "peers by URL are turned off on this node (HTTP_PEERS=false): pair by invite instead", http.StatusConflict)
			return
		}
		var body struct {
			URL string `json:"url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		if body.URL == "" {
			http.Error(w, "url is required", http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		name, version, err := registry.Probe(ctx, strings.TrimRight(body.URL, "/"))
		if err != nil {
			slog.Warn("adding peer: unreachable", "url", body.URL, logging.Err(err))
			http.Error(w, "peer unreachable: "+err.Error(), http.StatusUnprocessableEntity)
			return
		}
		id := registry.NewID(name)
		if err := registry.AddPeer(ctx, id, body.URL, name, version, ""); err != nil {
			slog.Error("adding peer", "url", body.URL, logging.Err(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// RedeemHandler serves POST /api/v1/peers/redeem with an {"invite"} JSON
// body: pairs with the node that issued the invite (see peerauth.Pair) and
// adds it as an mTLS peer. Pairing is one-way: the issuer authorizes this
// node as a client and never dials back. A legacy http:// peer with the
// issuer's node name is upgraded in place, keeping its id (which names its
// .strm folder) and its catalog mirror; re-pairing with a node already
// paired keeps its id too.
func RedeemHandler(registry *Registry, selfNodeID string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !registry.MTLSPeers() {
			http.Error(w, "pairing by invite is turned off on this node: set MTLS_PEERS=true", http.StatusConflict)
			return
		}
		if registry.identity == nil {
			http.Error(w, "this node has no identity key", http.StatusInternalServerError)
			return
		}
		var body struct {
			Invite string `json:"invite"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		inv, err := peerauth.ParseInvite(body.Invite)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if inv.Expired(time.Now()) {
			http.Error(w, "invite expired: ask for a new one", http.StatusUnprocessableEntity)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()
		res, err := peerauth.Pair(ctx, inv, registry.identity.Pin(), selfNodeID)
		if err != nil {
			slog.Warn("redeeming invite failed", "url", inv.URL, "issuer", inv.NodeID, logging.Err(err))
			status := http.StatusBadGateway
			if errors.Is(err, peerauth.ErrPinMismatch) || errors.Is(err, peerauth.ErrInviteRejected) {
				status = http.StatusUnprocessableEntity
			}
			http.Error(w, err.Error(), status)
			return
		}

		id, how := registry.idForRedeemed(res.NodeID, inv.Pin)
		if err := registry.AddPeer(ctx, id, inv.URL, res.NodeID, res.Version, inv.Pin); err != nil {
			slog.Error("adding paired peer", "peer", id, logging.Err(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		slog.Info("paired with peer by invite", "peer", id, "url", inv.URL, "name", res.NodeID, "as", how)
		p, _ := registry.Get(id)
		writePeer(w, p)
	}
}

// idForRedeemed picks the registry id for a peer just paired with: the
// id of a peer already holding that key (re-pairing), else of a legacy
// http:// peer reporting the same node name (upgrade), else a new one.
func (r *Registry) idForRedeemed(name, pin string) (id, how string) {
	if p, ok := r.PeerByPin(pin); ok {
		return p.ID, "re-pair"
	}
	r.mu.RLock()
	for id, p := range r.peers {
		if p.Pin == "" && name != "" && (p.Name == name || id == name) {
			r.mu.RUnlock()
			return id, "upgrade"
		}
	}
	r.mu.RUnlock()
	return r.NewIDForPin(name, pin), "new"
}

func writePeer(w http.ResponseWriter, p Peer) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(peerDTO{ID: p.ID, Name: p.Name, URL: p.URL, State: string(p.State), Version: p.Version, IP: p.IP, MTLS: p.Pin != ""})
}

// RemoveHandler serves DELETE /api/v1/peers/{peerID}.
func RemoveHandler(registry *Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("peerID")
		if id == "" {
			http.Error(w, "missing peer id", http.StatusBadRequest)
			return
		}
		if err := registry.RemovePeer(r.Context(), id); err != nil {
			slog.Error("removing peer", "peer", id, logging.Err(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
