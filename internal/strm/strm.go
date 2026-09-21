// Package strm writes and reconciles the .strm files that point Jellyfin
// at items owned by a remote peer.
package strm

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"jellysync/internal/config"
	"jellysync/internal/jellyfin"
)

var illegalPathChars = regexp.MustCompile(`[/\x00]`)

func sanitize(name string) string {
	name = illegalPathChars.ReplaceAllString(name, "-")
	name = strings.TrimSpace(name)
	if name == "" {
		name = "untitled"
	}
	return name
}

// mediaRoot returns the top-level folder an item's .strm lives under, so
// movies and series sit in entirely separate trees (outputDir/movies and
// outputDir/series). This lets each side point a Jellyfin library root at
// just one of the two instead of scanning a single mixed folder.
func mediaRoot(mediaType string) string {
	if mediaType == "movie" {
		return "movies"
	}
	return "series"
}

// pathFor derives a filesystem path from a catalog row, grouping each
// peer's items under their own subfolder so they're easy to browse or
// bulk-remove on disk. The global_id suffix on the item (or episode) folder
// is load-bearing, not cosmetic: two distinct items can share a display
// name (a movie and a series both called "Alice", or two titles that
// collide on the title+year fallback hash), and without a unique suffix
// they'd both resolve to the same .strm file and silently overwrite each
// other. Provider IDs are rendered in the bracket syntax Jellyfin itself
// recognizes for metadata matching, which is a bonus, not just
// disambiguation.
//
// Episodes get the Series Name/Season NN/... layout Jellyfin expects for
// TV libraries instead of the flat one-folder-per-item layout movies use.
func pathFor(outputDir string, r row) string {
	peerDir := filepath.Join(outputDir, mediaRoot(r.mediaType), sanitize(r.peerID))

	if r.mediaType == "episode" && r.seriesGlobalID != "" {
		seriesDir := sanitize(r.seriesName) + " [" + disambiguator(r.seriesGlobalID) + "]"
		filename := fmt.Sprintf("%s - S%02dE%02d - %s [%s].strm",
			sanitize(r.seriesName), r.seasonNumber, r.episodeNumber, sanitize(r.name), disambiguator(r.globalID))
		return filepath.Join(peerDir, seriesDir, seasonDirName(r.seasonNumber), filename)
	}

	dir := sanitize(r.name) + " [" + disambiguator(r.globalID) + "]"
	return filepath.Join(peerDir, dir, dir+".strm")
}

// seasonDirName follows Jellyfin's own convention: zero-padded "Season NN",
// except season 0 which Jellyfin treats as specials.
func seasonDirName(season int) string {
	if season == 0 {
		return "Specials"
	}
	return fmt.Sprintf("Season %02d", season)
}

func disambiguator(globalID string) string {
	kind, id, _ := strings.Cut(globalID, ":")
	switch kind {
	case "tmdb":
		return "tmdbid-" + id
	case "tvdb":
		return "tvdbid-" + id
	case "imdb":
		return "imdbid-" + id
	default:
		if len(id) > 8 {
			id = id[:8]
		}
		return "id-" + id
	}
}

func urlFor(baseURL, peerID, itemID string) string {
	return fmt.Sprintf("%s/api/v1/proxy/stream/%s/%s", strings.TrimRight(baseURL, "/"), peerID, itemID)
}

type row struct {
	globalID, name, mediaType, peerID, itemID string
	local                                     bool
	oldPath                                   sql.NullString

	seriesGlobalID, seriesName  string
	seasonNumber, episodeNumber int
}

// Reconcile writes a .strm file for every remote-elected catalog item that
// doesn't already have one, removes .strm files for items that are now
// local or no longer elected to any peer, and triggers a local Jellyfin
// library refresh if anything changed.
func Reconcile(ctx context.Context, db *sql.DB, jf *jellyfin.Client, cfg config.Strm) error {
	for _, root := range []string{"movies", "series"} {
		if err := os.MkdirAll(filepath.Join(cfg.OutputDir, root), 0o755); err != nil {
			return fmt.Errorf("creating %s dir: %w", root, err)
		}
	}

	rows, err := db.QueryContext(ctx, `
		SELECT global_id, name, media_type, local, primary_peer_id, primary_item_id, strm_path,
		       series_global_id, series_name, season_number, episode_number
		FROM catalog_items
	`)
	if err != nil {
		return fmt.Errorf("querying catalog_items: %w", err)
	}

	var all []row
	for rows.Next() {
		var r row
		var localInt int
		var peerID, itemID sql.NullString
		if err := rows.Scan(&r.globalID, &r.name, &r.mediaType, &localInt, &peerID, &itemID, &r.oldPath,
			&r.seriesGlobalID, &r.seriesName, &r.seasonNumber, &r.episodeNumber); err != nil {
			rows.Close()
			return fmt.Errorf("scanning catalog_items: %w", err)
		}
		r.local = localInt != 0
		r.peerID = peerID.String
		r.itemID = itemID.String
		all = append(all, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	changed := false

	for _, r := range all {
		// Series-root items have no downloadable file — only their
		// episodes are playable — so they never get a .strm of their own.
		wantStrm := !r.local && r.peerID != "" && r.itemID != "" && r.mediaType != "series"

		if !wantStrm {
			if r.oldPath.Valid {
				if err := removeStrm(r.oldPath.String, cfg.OutputDir); err != nil {
					log.Printf("strm: removing %s: %v", r.oldPath.String, err)
				}
				if err := clearPath(ctx, db, r.globalID); err != nil {
					return err
				}
				changed = true
			}
			continue
		}

		desiredPath := pathFor(cfg.OutputDir, r)
		desiredContent := urlFor(cfg.BaseURL, r.peerID, r.itemID)

		if r.oldPath.Valid && r.oldPath.String != desiredPath {
			if err := removeStrm(r.oldPath.String, cfg.OutputDir); err != nil {
				log.Printf("strm: removing stale %s: %v", r.oldPath.String, err)
			}
		}

		wrote, err := writeIfChanged(desiredPath, desiredContent)
		if err != nil {
			log.Printf("strm: writing %s: %v", desiredPath, err)
			continue
		}
		if wrote {
			changed = true
		}

		if !r.oldPath.Valid || r.oldPath.String != desiredPath {
			if err := setPath(ctx, db, r.globalID, desiredPath); err != nil {
				return err
			}
		}
	}

	if changed {
		if err := jf.RefreshLibrary(ctx); err != nil {
			log.Printf("strm: triggering library refresh: %v", err)
		}
	}

	return nil
}

// writeIfChanged writes content to path (creating parent directories as
// needed) unless a file with identical content is already there. Returns
// whether it actually wrote.
func writeIfChanged(path, content string) (bool, error) {
	existing, err := os.ReadFile(path)
	if err == nil && string(existing) == content {
		return false, nil
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return false, fmt.Errorf("creating dir: %w", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return false, fmt.Errorf("writing file: %w", err)
	}
	return true, nil
}

// removeStrm removes the .strm file at path, then best-effort cleans up any
// now-empty parent directories above it — the item's own folder, and for
// an episode the season and series folders above that too — stopping at
// outputDir. The boundary is always outputDir itself, never a path derived
// from the (possibly stale, pre-movies/series-split) file being removed, so
// a leftover .strm from an older on-disk layout can never walk the cleanup
// above outputDir. The movies/ and series/ folders directly under outputDir
// are additionally never removed even when empty, so they stay valid as
// fixed Jellyfin library roots. os.Remove on a non-empty directory just
// errors, which is fine to ignore: it means a sibling item/season/series
// still exists there.
func removeStrm(path, outputDir string) error {
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	boundary := filepath.Clean(outputDir)
	protected := map[string]bool{
		filepath.Join(boundary, "movies"): true,
		filepath.Join(boundary, "series"): true,
	}
	for dir := filepath.Dir(path); dir != boundary && dir != "." && dir != string(filepath.Separator); dir = filepath.Dir(dir) {
		if protected[dir] {
			break
		}
		if err := os.Remove(dir); err != nil {
			break
		}
	}
	return nil
}

func setPath(ctx context.Context, db *sql.DB, globalID, path string) error {
	_, err := db.ExecContext(ctx, `UPDATE catalog_items SET strm_path = ? WHERE global_id = ?`, path, globalID)
	if err != nil {
		return fmt.Errorf("setting strm_path for %s: %w", globalID, err)
	}
	return nil
}

func clearPath(ctx context.Context, db *sql.DB, globalID string) error {
	_, err := db.ExecContext(ctx, `UPDATE catalog_items SET strm_path = NULL WHERE global_id = ?`, globalID)
	if err != nil {
		return fmt.Errorf("clearing strm_path for %s: %w", globalID, err)
	}
	return nil
}
