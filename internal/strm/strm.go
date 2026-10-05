// Package strm writes and reconciles the .strm files that point Jellyfin
// at items owned by a remote peer.
package strm

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"unicode/utf8"

	"jellysync/internal/config"
	"jellysync/internal/logging"
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
//
// Display names are shortened as needed so no path element exceeds
// maxNameBytes; the suffix is never shortened.
func pathFor(outputDir string, r row) string {
	peerDir := filepath.Join(outputDir, mediaRoot(r.mediaType), sanitize(r.peerID))

	if r.mediaType == "episode" && r.seriesGlobalID != "" {
		seriesSuffix := " [" + disambiguator(r.seriesGlobalID) + "]"
		seriesDir := truncateUTF8(sanitize(r.seriesName), maxNameBytes-len(seriesSuffix)) + seriesSuffix

		fixed := fmt.Sprintf(" - S%02dE%02d - ", r.seasonNumber, r.episodeNumber) + " [" + disambiguator(r.globalID) + "].strm"
		avail := maxNameBytes - len(fixed)
		series, name := sanitize(r.seriesName), sanitize(r.name)
		if len(series)+len(name) > avail {
			series = truncateUTF8(series, max(avail/2, avail-len(name)))
			name = truncateUTF8(name, avail-len(series))
		}
		filename := fmt.Sprintf("%s - S%02dE%02d - %s [%s].strm",
			series, r.seasonNumber, r.episodeNumber, name, disambiguator(r.globalID))
		return filepath.Join(peerDir, seriesDir, seasonDirName(r.seasonNumber), filename)
	}

	suffix := " [" + disambiguator(r.globalID) + "]"
	dir := truncateUTF8(sanitize(r.name), maxNameBytes-len(suffix)-len(".strm")) + suffix
	return filepath.Join(peerDir, dir, dir+".strm")
}

// maxNameBytes is the longest file or directory name most filesystems
// accept (NAME_MAX on Linux).
const maxNameBytes = 255

// truncateUTF8 shortens s to at most n bytes without splitting a rune.
func truncateUTF8(s string, n int) string {
	if n <= 0 {
		return ""
	}
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// seasonDirName follows Jellyfin's own convention: zero-padded "Season NN",
// except season 0 which Jellyfin treats as specials.
func seasonDirName(season int) string {
	if season == 0 {
		return "Specials"
	}
	return fmt.Sprintf("Season %02d", season)
}

func providerDisambiguator(globalID string) string {
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

// maxDisambiguatorBytes bounds a provider ID rendered into a path, so an
// absurdly long one (IDs come from peers) can't push a name past
// maxNameBytes; longer ones fall back to a hash of the whole global ID.
const maxDisambiguatorBytes = 64

func disambiguator(globalID string) string {
	d := providerDisambiguator(globalID)
	if len(d) > maxDisambiguatorBytes {
		sum := sha256.Sum256([]byte(globalID))
		return "id-" + hex.EncodeToString(sum[:4])
	}
	return d
}

func urlFor(baseURL, peerID, itemID string) string {
	return fmt.Sprintf("%s/api/v1/proxy/stream/%s/%s", strings.TrimRight(baseURL, "/"), peerID, itemID)
}

type row struct {
	globalID, name, mediaType, peerID, itemID string
	local, hidden, hiddenFolder               bool
	oldPath                                   sql.NullString

	seriesGlobalID, seriesName  string
	seasonNumber, episodeNumber int
}

// Stats describes what one Reconcile changed on disk.
type Stats struct {
	Written   int // .strm files created or rewritten
	Moved     int // of Written, how many replaced a file at an old path
	Removed   int // .strm files deleted (item now local, hidden or no longer offered)
	Unchanged int // wanted .strm files already up to date
	Swept     int // untracked .strm files deleted (left behind by an older build, a lost DB, a changed layout)
	Failed    int // files that couldn't be written or removed; retried next cycle
}

// Reconcile writes a .strm file for every remote-elected catalog item that
// doesn't already have one, removes .strm files for items that are now
// local, hidden by the user or by a folder exclusion, or no longer elected to any peer, then sweeps
// any other .strm file under the movies/ and series/ folders. It never asks
// Jellyfin to rescan.
//
// A row's strm_path only changes once the file it names is gone, so a
// removal that fails is retried next cycle instead of being forgotten. The
// strm_path updates are written in one transaction at the end; if the
// process dies before that, the next cycle finds the same files (paths are
// a pure function of the row) and records them then.
func Reconcile(ctx context.Context, db *sql.DB, cfg config.Strm) (Stats, error) {
	var st Stats
	for _, root := range strmRoots {
		if err := os.MkdirAll(filepath.Join(cfg.OutputDir, root), 0o755); err != nil {
			return st, fmt.Errorf("creating %s dir: %w", root, err)
		}
	}

	// Ordered so that, should two rows ever map to the same path, the same
	// one wins every cycle.
	rows, err := db.QueryContext(ctx, `
		SELECT global_id, name, media_type, local, primary_peer_id, primary_item_id, strm_path,
		       series_global_id, series_name, season_number, episode_number, hidden, hidden_folder
		FROM catalog_items
		ORDER BY global_id
	`)
	if err != nil {
		return st, fmt.Errorf("querying catalog_items: %w", err)
	}

	var all []row
	for rows.Next() {
		var r row
		var localInt int
		var peerID, itemID sql.NullString
		if err := rows.Scan(&r.globalID, &r.name, &r.mediaType, &localInt, &peerID, &itemID, &r.oldPath,
			&r.seriesGlobalID, &r.seriesName, &r.seasonNumber, &r.episodeNumber, &r.hidden, &r.hiddenFolder); err != nil {
			rows.Close()
			return st, fmt.Errorf("scanning catalog_items: %w", err)
		}
		r.local = localInt != 0
		r.peerID = peerID.String
		r.itemID = itemID.String
		all = append(all, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return st, err
	}

	var updates []pathUpdate
	keep := make(map[string]bool)      // every .strm file the sweep must leave alone
	claimed := make(map[string]string) // desired path -> global_id that owns it
	for _, r := range all {
		// Series-root items have no downloadable file — only their
		// episodes are playable — so they never get a .strm of their own.
		wantStrm := !r.local && !r.hidden && !r.hiddenFolder && r.peerID != "" && r.itemID != "" && r.mediaType != "series"

		if !wantStrm {
			if r.oldPath.Valid {
				if err := removeStrm(r.oldPath.String, cfg.OutputDir); err != nil {
					slog.Warn("removing .strm file", "path", r.oldPath.String, logging.Err(err))
					st.Failed++
					keep[r.oldPath.String] = true
					continue
				}
				slog.Debug("removed .strm file", "path", r.oldPath.String, "item", r.globalID, "now_local", r.local, "hidden", r.hidden, "hidden_folder", r.hiddenFolder)
				st.Removed++
				updates = append(updates, pathUpdate{globalID: r.globalID})
			}
			continue
		}

		desiredPath := pathFor(cfg.OutputDir, r)
		desiredContent := urlFor(cfg.BaseURL, r.peerID, r.itemID)
		moving := r.oldPath.Valid && r.oldPath.String != desiredPath
		if moving {
			// Until the old file is gone, it's still this row's.
			keep[r.oldPath.String] = true
		}

		if owner, taken := claimed[desiredPath]; taken {
			slog.Warn("two items map to the same .strm path; skipping the second", "path", desiredPath, "item", r.globalID, "owner", owner)
			st.Failed++
			continue
		}
		claimed[desiredPath] = r.globalID

		// Kept even if the write below fails: whatever is there is still
		// this row's best file.
		keep[desiredPath] = true

		// Write the new file before removing the old one, so a failure in
		// between never leaves the item without any .strm.
		wrote, err := writeIfChanged(desiredPath, desiredContent)
		if err != nil {
			slog.Warn("writing .strm file", "path", desiredPath, logging.Err(err))
			st.Failed++
			continue
		}
		if wrote {
			slog.Debug("wrote .strm file", "path", desiredPath, "peer", r.peerID, "item", r.globalID)
			st.Written++
		} else {
			st.Unchanged++
		}

		if moving {
			if err := removeStrm(r.oldPath.String, cfg.OutputDir); err != nil {
				// Keep the old path recorded so the next cycle retries;
				// the new file is rewritten (unchanged) and recorded then.
				slog.Warn("removing stale .strm file", "path", r.oldPath.String, logging.Err(err))
				st.Failed++
				continue
			}
			delete(keep, r.oldPath.String)
			st.Moved++
		}
		if !r.oldPath.Valid || moving {
			updates = append(updates, pathUpdate{globalID: r.globalID, path: desiredPath})
		}
	}

	if err := savePaths(ctx, db, updates); err != nil {
		return st, err
	}

	swept, failed := sweep(cfg.OutputDir, keep)
	st.Swept += swept
	st.Failed += failed
	return st, nil
}

// strmRoots are the folders under OutputDir that hold .strm files, and the
// only ones the sweep looks in.
var strmRoots = []string{"movies", "series"}

// tempPrefix starts the name of every file writeIfChanged is still
// writing; the sweep deletes any it finds (left by a crash mid-write).
const tempPrefix = ".jellysync-"

// sweep deletes every .strm file (and leftover temp file) under the
// movies/ and series/ folders that isn't in keep, cleaning up the folders
// that leaves empty. Other files — artwork or .nfo files Jellyfin may save
// next to a .strm — are never touched. It returns how many files it
// removed and how many it failed to.
func sweep(outputDir string, keep map[string]bool) (swept, failed int) {
	for _, root := range strmRoots {
		err := filepath.WalkDir(filepath.Join(outputDir, root), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				slog.Warn("sweeping .strm files", "path", path, logging.Err(err))
				failed++
				return nil
			}
			if d.IsDir() || keep[path] {
				return nil
			}
			name := d.Name()
			if !strings.HasSuffix(name, ".strm") && !strings.HasPrefix(name, tempPrefix) {
				return nil
			}
			if err := removeStrm(path, outputDir); err != nil {
				slog.Warn("removing untracked .strm file", "path", path, logging.Err(err))
				failed++
				return nil
			}
			slog.Debug("removed untracked .strm file", "path", path)
			swept++
			return nil
		})
		if err != nil {
			slog.Warn("sweeping .strm files", "root", root, logging.Err(err))
			failed++
		}
	}
	return swept, failed
}

// writeIfChanged writes content to path (creating parent directories as
// needed) unless a file with identical content is already there. Returns
// whether it actually wrote. The file is replaced atomically (temp file and
// rename), so a Jellyfin scan never reads a half-written one.
func writeIfChanged(path, content string) (bool, error) {
	existing, err := os.ReadFile(path)
	if err == nil && string(existing) == content {
		return false, nil
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return false, fmt.Errorf("creating dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, tempPrefix+"*.tmp")
	if err != nil {
		return false, fmt.Errorf("creating temp file: %w", err)
	}
	_, err = tmp.WriteString(content)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Chmod(tmp.Name(), 0o644)
	}
	if err == nil {
		err = os.Rename(tmp.Name(), path)
	}
	if err != nil {
		os.Remove(tmp.Name())
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
// above outputDir; a file outside outputDir altogether (recorded before
// OUTPUT_DIR changed) is removed without touching any directory. The
// movies/ and series/ folders directly under outputDir are additionally
// never removed even when empty, so they stay valid as fixed Jellyfin
// library roots. os.Remove on a non-empty directory just errors, which is
// fine to ignore: it means a sibling item/season/series still exists there.
func removeStrm(path, outputDir string) error {
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	boundary := filepath.Clean(outputDir)
	if !within(path, boundary) {
		return nil
	}
	protected := map[string]bool{}
	for _, root := range strmRoots {
		protected[filepath.Join(boundary, root)] = true
	}
	for dir := filepath.Dir(path); dir != boundary && within(dir, boundary); dir = filepath.Dir(dir) {
		if protected[dir] {
			break
		}
		if err := os.Remove(dir); err != nil {
			break
		}
	}
	return nil
}

// within reports whether path lies strictly inside dir.
func within(path, dir string) bool {
	rel, err := filepath.Rel(dir, filepath.Clean(path))
	return err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}

// pathUpdate sets (path != "") or clears a row's strm_path.
type pathUpdate struct {
	globalID, path string
}

func savePaths(ctx context.Context, db *sql.DB, updates []pathUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("saving strm paths: %w", err)
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `UPDATE catalog_items SET strm_path = ? WHERE global_id = ?`)
	if err != nil {
		return fmt.Errorf("saving strm paths: %w", err)
	}
	defer stmt.Close()
	for _, u := range updates {
		var path any
		if u.path != "" {
			path = u.path
		}
		if _, err := stmt.ExecContext(ctx, path, u.globalID); err != nil {
			return fmt.Errorf("saving strm_path for %s: %w", u.globalID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("saving strm paths: %w", err)
	}
	return nil
}
