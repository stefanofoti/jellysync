package peerauth

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"jellysync/internal/logging"
)

// InviteConfig is what the invite endpoints need to know about this node.
type InviteConfig struct {
	Identity  *Identity
	PublicURL string // canonical https://host:port peers reach this node at; "" disables invites
	NodeID    string
	TTL       time.Duration
}

type inviteDTO struct {
	Invite    string    `json:"invite"`
	Pin       string    `json:"pin"`
	ExpiresAt time.Time `json:"expires_at"`
}

// CreateInviteHandler serves POST /api/v1/invites (dashboard only): issues
// a single-use invite string for a consumer to redeem.
func CreateInviteHandler(store *Store, cfg InviteConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if cfg.PublicURL == "" {
			http.Error(w, "set PUBLIC_URL to the address peers reach this node's peer port at to create invites", http.StatusConflict)
			return
		}
		seed, pending, err := store.CreateInvite(r.Context(), cfg.TTL)
		if err != nil {
			slog.Error("creating invite", logging.Err(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		inv := Invite{URL: cfg.PublicURL, NodeID: cfg.NodeID, Pin: cfg.Identity.Pin(), Seed: seed, ExpiresAt: pending.ExpiresAt.Unix()}
		slog.Info("invite created", "invite", pending.Pin, "expires_at", pending.ExpiresAt.Format(time.RFC3339))
		writeJSON(w, http.StatusCreated, inviteDTO{Invite: inv.Encode(), Pin: pending.Pin, ExpiresAt: pending.ExpiresAt})
	}
}

// ListInvitesHandler serves GET /api/v1/invites: pending invites, without
// their strings (the private key was never stored).
func ListInvitesHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, store.Invites(r.Context()))
	}
}

// CancelInviteHandler serves DELETE /api/v1/invites/{pin}.
func CancelInviteHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pin := r.PathValue("pin")
		switch err := store.CancelInvite(r.Context(), pin); {
		case errors.Is(err, ErrNotFound):
			http.Error(w, "no such invite", http.StatusNotFound)
		case err != nil:
			slog.Error("cancelling invite", "invite", pin, logging.Err(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
		default:
			slog.Info("invite cancelled", "invite", pin)
			w.WriteHeader(http.StatusNoContent)
		}
	}
}

// ListClientsHandler serves GET /api/v1/clients: the nodes allowed to read
// this node's library.
func ListClientsHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, store.Clients())
	}
}

// RevokeClientHandler serves DELETE /api/v1/clients/{clientID}.
func RevokeClientHandler(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("clientID")
		switch err := store.Revoke(r.Context(), id); {
		case errors.Is(err, ErrNotFound):
			http.Error(w, "no such client", http.StatusNotFound)
		case err != nil:
			slog.Error("revoking client", "client", id, logging.Err(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
		default:
			slog.Info("client access revoked", "client", id)
			w.WriteHeader(http.StatusNoContent)
		}
	}
}

// PairHandler serves POST /api/v1/pair on the peer port. Only a pending
// invite's temporary key gets here (RequirePairing): it consumes the
// invite and authorizes the caller's permanent key as a client. assignID
// picks the new client's id from its name and pin.
func PairHandler(store *Store, assignID func(name, pin string) string, nodeID, version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c, _ := CallerFrom(r.Context())
		var body PairRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&body); err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		if !ValidPin(body.Pin) || body.Pin == c.Pin {
			http.Error(w, "invalid pin", http.StatusBadRequest)
			return
		}
		client, err := store.Redeem(r.Context(), c.Pin, body.Pin, body.NodeID, assignID(body.NodeID, body.Pin))
		switch {
		case errors.Is(err, ErrInviteInvalid):
			http.Error(w, "invite no longer valid", http.StatusGone)
			return
		case err != nil:
			slog.Error("pairing client", "name", body.NodeID, logging.Err(err))
			http.Error(w, "pairing failed", http.StatusInternalServerError)
			return
		}
		slog.Info("client paired: it can now read this node's library", "client", client.ID, "name", client.Name, "invite", c.Pin, "remote", r.RemoteAddr)
		writeJSON(w, http.StatusOK, PairResponse{NodeID: nodeID, Version: version})
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
