package catalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"

	"jellysync/internal/logging"
)

// Folder exclusions: per peer, folders on that peer's own filesystem whose
// items this node doesn't take from it. An offer whose Entry.Path lies under
// one of them doesn't count in the election; if no other source offers the
// item, it's still listed (catalog_items.hidden_folder) but gets no .strm.
// Unlike a hide, it can't be undone per item, only by editing the folders.
// Items from peers too old to report a path are never excluded.

// normalizeFolder turns a user-entered folder into the form underFolder
// compares against: backslashes as slashes, no trailing slash (except the
// root itself).
func normalizeFolder(f string) string {
	f = strings.ReplaceAll(strings.TrimSpace(f), `\`, "/")
	for len(f) > 1 && strings.HasSuffix(f, "/") {
		f = strings.TrimSuffix(f, "/")
	}
	return f
}

// underFolder reports whether path is folder itself or lies inside it,
// comparing whole path components, case-sensitively. folder must be
// normalized; an empty path never matches.
func underFolder(path, folder string) bool {
	if path == "" || folder == "" {
		return false
	}
	path = strings.ReplaceAll(path, `\`, "/")
	if folder == "/" {
		return strings.HasPrefix(path, "/")
	}
	return path == folder || strings.HasPrefix(path, folder+"/")
}

func underAnyFolder(path string, folders []string) bool {
	for _, f := range folders {
		if underFolder(path, f) {
			return true
		}
	}
	return false
}

// excludedFolders returns every peer's excluded folders, by peer id.
func excludedFolders(ctx context.Context, q rowsQueryer) (map[string][]string, error) {
	rows, err := q.QueryContext(ctx, `SELECT peer_id, folder FROM peer_excluded_folders ORDER BY peer_id, folder`)
	if err != nil {
		return nil, fmt.Errorf("reading excluded folders: %w", err)
	}
	defer rows.Close()
	out := make(map[string][]string)
	for rows.Next() {
		var peerID, folder string
		if err := rows.Scan(&peerID, &folder); err != nil {
			return nil, fmt.Errorf("reading excluded folders: %w", err)
		}
		out[peerID] = append(out[peerID], folder)
	}
	return out, rows.Err()
}

// SetExcludedFolders replaces peerID's excluded folders. Each is normalized
// and must be absolute (starting with "/", or a drive like "D:/" for a
// Windows peer); duplicates are dropped. It returns the stored list.
func SetExcludedFolders(ctx context.Context, db *sql.DB, peerID string, folders []string) ([]string, error) {
	clean := make([]string, 0, len(folders))
	for _, f := range folders {
		n := normalizeFolder(f)
		if !isAbsFolder(n) {
			return nil, errBadRequest(fmt.Sprintf("not an absolute folder: %q", f))
		}
		if !slices.Contains(clean, n) {
			clean = append(clean, n)
		}
	}
	slices.Sort(clean)
	err := withTx(ctx, db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM peer_excluded_folders WHERE peer_id = ?`, peerID); err != nil {
			return err
		}
		for _, f := range clean {
			if _, err := tx.ExecContext(ctx, `INSERT INTO peer_excluded_folders (peer_id, folder) VALUES (?, ?)`, peerID, f); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("saving excluded folders of %s: %w", peerID, err)
	}
	return clean, nil
}

func isAbsFolder(f string) bool {
	if strings.HasPrefix(f, "/") {
		return true
	}
	// A Windows peer's path: drive letter, colon, slash.
	return len(f) >= 3 && f[1] == ':' && f[2] == '/' &&
		(('a' <= f[0] && f[0] <= 'z') || ('A' <= f[0] && f[0] <= 'Z'))
}

type excludedFoldersBody struct {
	Folders []string `json:"folders"`
}

// ExcludedFoldersHandler serves GET and PUT
// /api/v1/peers/{peerID}/excluded-folders: {"folders": [...]}. PUT replaces
// the whole list, then calls changed so the next sync applies it right
// away. known reports whether peerID is a registered peer.
func ExcludedFoldersHandler(db *sql.DB, known func(peerID string) bool, changed func()) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		peerID := r.PathValue("peerID")
		if !known(peerID) {
			http.Error(w, "no such peer", http.StatusNotFound)
			return
		}
		var folders []string
		switch r.Method {
		case http.MethodGet:
			all, err := excludedFolders(r.Context(), db)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			folders = all[peerID]
		case http.MethodPut:
			var body excludedFoldersBody
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, `body must be {"folders": [string]}`, http.StatusBadRequest)
				return
			}
			var err error
			folders, err = SetExcludedFolders(r.Context(), db, peerID, body.Folders)
			if _, bad := err.(errBadRequest); bad {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if err != nil {
				slog.Error("saving excluded folders", "peer", peerID, logging.Err(err))
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			slog.Info("excluded folders changed, applying now", "peer", peerID, "folders", folders)
			changed()
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if folders == nil {
			folders = []string{}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(excludedFoldersBody{Folders: folders})
	}
}
