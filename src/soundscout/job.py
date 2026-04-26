from __future__ import annotations

import logging
import math
import csv
import os
import time
import re
from pathlib import Path

from .import_step import run_optional_import
from .lastfm import LastFmClient
from .models import Track
from .plex import PlexClient

logger = logging.getLogger(__name__)


def _norm_text(value: str) -> str:
    s = (value or "").lower()
    s = s.replace("&", " and ")
    # Keep unicode word characters (letters + digits in any script, including CJK).
    # Only strip actual punctuation/symbols so Japanese/Korean/Chinese track names
    # survive normalization and produce non-empty keys.
    s = re.sub(r"[^\w]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _norm_artist(value: str) -> str:
    s = (value or "").strip()
    if not s:
        return ""
    s = re.sub(r"\([^)]*\)", " ", s)
    s = re.sub(r"\[[^\]]*\]", " ", s)
    s = re.split(r"(?i)\b(feat\.?|ft\.?|featuring|with)\b", s, maxsplit=1)[0]
    s = re.split(r"(?i)\s+\bx\b\s+", s, maxsplit=1)[0]
    return _norm_text(s)


def _norm_track_title(value: str) -> str:
    s = (value or "").strip()
    s = re.sub(r"\([^)]*\)", " ", s)
    s = re.sub(r"\[[^\]]*\]", " ", s)
    s = re.sub(r"(?i)\bfeat\.?\b.*$", " ", s)
    s = re.sub(r"(?i)\bft\.?\b.*$", " ", s)
    s = re.sub(r"(?i)\b(remaster(ed)?|mono|stereo|explicit|clean|radio edit|edit)\b", " ", s)
    return _norm_text(s)


def _strip_track_number(name: str) -> str:
    s = (name or "").strip()
    s = re.sub(r"^\s*\d{1,3}\s*[-. ]\s*", "", s)
    return s.strip()


def _track_key(artist: str, title: str) -> str:
    return f"{_norm_artist(artist)}|||{_norm_track_title(title)}"


def _build_filesystem_track_index(root: Path) -> set[str]:
    """Build a best-effort index of tracks already present on disk.

    This is used as a fallback to avoid recommending/downloading tracks already
    in the library when Plex matching is imperfect.
    """
    audio_exts = {".flac", ".mp3", ".m4a", ".aac", ".ogg", ".opus", ".wav", ".aiff", ".alac"}
    keys: set[str] = set()

    if not root.exists() or not root.is_dir():
        return keys

    for p in root.rglob("*"):
        try:
            if not p.is_file():
                continue
            if p.suffix.lower() not in audio_exts:
                continue

            rel = p.relative_to(root)
            parts = list(rel.parts)
            if len(parts) < 2:
                continue

            # Artist is usually the first folder.
            artist = parts[0]

            # Title is usually the filename (strip extension + track numbers).
            title = _strip_track_number(p.stem)

            k = _track_key(artist, title)
            if k and "|||" in k:
                keys.add(k)
        except Exception:
            continue

    return keys


def run_job(
    *,
    lastfm_api_key: str,
    lastfm_username: str,
    lastfm_mode: str,
    lastfm_seed_count: int,
    lastfm_similar_per_seed: int,
    plex_baseurl: str,
    plex_token: str,
    plex_music_library: str,
    playlist_name: str,
    max_tracks: int,
    dry_run: bool,
    report_path: str | None,
    generate_report_only: bool,
    enable_import: bool,
    import_inbox_dir: str,
    import_cmd: str | None,
) -> None:
    # Special mode: create playlist from CSV report (post-download)
    # If lastfm_mode is 'playlist_from_report' and report_path exists, we skip Last.fm and just scan -> match -> playlist.
    if lastfm_mode == "playlist_from_report":
        _run_playlist_from_report_mode(
            report_path=report_path,
            plex_baseurl=plex_baseurl,
            plex_token=plex_token,
            plex_music_library=plex_music_library,
            playlist_name=playlist_name,
            dry_run=dry_run,
        )
        return

    # Optional: import/tag existing files first
    run_optional_import(enable_import, import_inbox_dir, import_cmd)

    # 1. Initialize Plex Client early to handle backlog checking
    plex = None
    try:
        plex = PlexClient(plex_baseurl, plex_token, plex_music_library)
        if enable_import:
            plex.update_library()
    except Exception as e:
        if dry_run:
            logger.warning(
                "Plex connection failed (DRY_RUN=1 so continuing without Plex matching): %s",
                e,
            )
            plex = None
        else:
            raise

    # 2. Backlog Logic Removed (User requested "fresh only")
    # We intentionally do NOT read the existing report to fill the quota.
    # Every run is a fresh batch from Last.fm.
    selected_missing: list[Track] = []
    selected_keys: set[tuple[str, str]] = set()

    # Build a disk index of what's already in the library to reduce duplicates.
    output_root = Path(os.environ.get("OUTPUT_PATH", "/music"))
    fs_owned_keys = _build_filesystem_track_index(output_root)
    
    # 3. Fetch Last.fm
    target_missing = max_tracks
    # always fetch full quota check
    needed = target_missing 
    
    if needed > 0:
        # Fetch Last.fm
        lastfm = LastFmClient(lastfm_api_key, lastfm_username)
        mode = (lastfm_mode or "weekly_plays").strip().lower()
        logger.info("Last.fm mode: %s (target=%d)", mode, target_missing)
        
        # Adjust target for Last.fm fetch to just what we need
        # We fetch a pool to filter against duplicates
        target_fetch_missing = needed
        
        if mode in {"recommendations", "recommended", "similar", "discover"}:
            # Build a large candidate pool before library filtering.
            # A well-populated library can own 80-95% of naive candidates, so we need
            # a pool 30-50× the target to reliably deliver 60 genuinely new tracks.
            pool_target = max(800, min(target_fetch_missing * 40, 6000))
            seed_count = max(10, int(lastfm_seed_count))
            # Use the configured per_seed as a floor; compute enough to fill the pool.
            per_seed = max(int(lastfm_similar_per_seed), int(math.ceil(pool_target / seed_count)))
            # Ensure a minimum of 30 per seed for diversity.
            per_seed = max(per_seed, 30)
            # Last.fm caps track.getSimilar at 100.
            per_seed = min(per_seed, 100)

            tracks = lastfm.get_recommended_tracks(
                max_tracks=pool_target,
                seed_count=seed_count,
                similar_per_seed=per_seed,
            )
            logger.info(
                "Fetched %d recommendation candidates from Last.fm (pool); selecting up to %d missing tracks.",
                len(tracks),
                target_fetch_missing,
            )
        else:
            tracks = lastfm.get_last_week_tracks(max_tracks=max_tracks) # Weekly plays usually ignores quota logic and just grabs top X
            logger.info("Fetched %d last-week plays from Last.fm", len(tracks))

            if plex is not None:
                logger.warning(
                    "LASTFM_MODE=weekly_plays returns tracks you already listened to (usually already in Plex). "
                    "Set LASTFM_MODE=recommendations for discovery-style results."
                )

    # Preview logging
    preview_n = min(20, len(tracks))
    if preview_n:
        preview = "\n".join([f"{t.artist} — {t.title}" for t in tracks[:preview_n]])
        logger.info("Previewing new Last.fm candidates:\n%s", preview)

    # 4. Filter New Tracks against Plex
    plex_items: list = []
    evaluated: list[Track] = []
    owned: list[Track] = []
    missing: list[Track] = []
    # selected_missing is already partially populated
    owned_keys: set[tuple[str, str]] = set()
    # selected_keys is already partially populated
    matched_by_key: dict[tuple[str, str], object] = {}

    if plex is not None:
        for t in tracks:
            evaluated.append(t)
            key = (t.artist.strip().lower(), t.title.strip().lower())

            # Fast disk-based check (handles Plex mismatches): if it exists on disk, treat as owned.
            if _track_key(t.artist, t.title) in fs_owned_keys:
                owned.append(t)
                owned_keys.add(key)
                # Still try to match in Plex so we can include it in the playlist.
                try:
                    item = plex.find_track(t)
                    if item is not None:
                        plex_items.append(item)
                        matched_by_key[key] = item
                except Exception:
                    pass
                continue
            
            # Check if we already have it in the backlog (selected_missing)
            if key in selected_keys:
                continue

            item = plex.find_track(t)
            if item is None:
                missing.append(t)
                if len(selected_missing) < target_missing:
                    selected_missing.append(t)
                    selected_keys.add(key)
            else:
                owned.append(t)
                owned_keys.add(key)
                plex_items.append(item)
                matched_by_key[key] = item

            # In recommendations mode, stop early once we have enough missing tracks.
            if mode in {"recommendations", "recommended", "similar", "discover"} and len(selected_missing) >= target_missing:
                break

        logger.info("Evaluated new tracks: %d owned, %d new missing.", len(owned), len(missing))

        if mode in {"recommendations", "recommended", "similar", "discover"} and not missing:
            logger.warning(
                "No missing tracks found from Last.fm candidates. "
                "Try increasing LASTFM_SEED_COUNT / LASTFM_SIMILAR_PER_SEED, or ensure your OUTPUT_PATH points to your real library."
            )
        
        if mode in {"recommendations", "recommended", "similar", "discover"}:
            logger.info("Final list for report: %d tracks", len(selected_missing))

        if missing:
             logger.info(
                "New Missing examples: %s",
                "; ".join([f"{m.artist} - {m.title}" for m in missing[:10]]),
            )
    else:
        # No Plex avail handling (Fallback)
        # Verify against backlog only
        for t in tracks:
            key = (t.artist.strip().lower(), t.title.strip().lower())
            if key not in selected_keys:
                 selected_missing.append(t)
                 selected_keys.add(key)
                 if len(selected_missing) >= target_missing:
                     break
        
        evaluated = list(tracks)
        missing = list(tracks)

    if report_path:
        p = Path(report_path)
        p.parent.mkdir(parents=True, exist_ok=True)

        # Primary output: the final list of tracks to go in the playlist (selected missing-only)
        final_lines = ["artist,title\n"]
        for t in selected_missing:
            a = t.artist.replace('"', '""')
            tt = t.title.replace('"', '""')
            final_lines.append(f'"{a}","{tt}"\n')
        p.write_text("".join(final_lines), encoding="utf-8")
        logger.info("Wrote final selected list to %s", p)

        # Secondary output: detailed evaluation for debugging
        evaluated_path = p.with_name(f"{p.stem}-evaluated{p.suffix}")
        eval_lines = ["artist,title,owned_in_plex,selected_for_playlist\n"]
        for t in evaluated:
            key = (t.artist.strip().lower(), t.title.strip().lower())
            is_owned = key in owned_keys
            is_selected = key in selected_keys
            a = t.artist.replace('"', '""')
            tt = t.title.replace('"', '""')
            eval_lines.append(f'"{a}","{tt}",{1 if is_owned else 0},{1 if is_selected else 0}\n')
        evaluated_path.write_text("".join(eval_lines), encoding="utf-8")
        logger.info("Wrote evaluated details to %s", evaluated_path)

        missing_path = p.with_name(f"{p.stem}-missing{p.suffix}")
        missing_lines = ["artist,title\n"]
        for t in missing:
            a = t.artist.replace('"', '""')
            tt = t.title.replace('"', '""')
            missing_lines.append(f'"{a}","{tt}"\n')
        missing_path.write_text("".join(missing_lines), encoding="utf-8")
        logger.info("Wrote missing list to %s", missing_path)

    if generate_report_only:
        logger.info("GENERATE_REPORT_ONLY=1; output written, stopping job before playlist update.")
        return

    if dry_run:
        logger.info("DRY_RUN=1 set; skipping playlist creation")
        return

    # If we reached here, Plex must be available
    assert plex is not None

    # Build playlist from tracks that actually exist in Plex.
    # In recommendations mode, many tracks will be missing by design; we still create a playlist
    # from whatever matched, and write the missing list to REPORT_PATH (if configured).
    playlist_items: list = []
    seen_rating_keys: set = set()
    for item in plex_items:
        try:
            rk = getattr(item, "ratingKey", None)
        except Exception:
            rk = None
        if rk is not None:
            if rk in seen_rating_keys:
                continue
            seen_rating_keys.add(rk)
        playlist_items.append(item)

    logger.info(
        "Playlist candidates found in Plex: %d (missing recommendations: %d)",
        len(playlist_items),
        len(selected_missing),
    )

    logger.info(
        "Note: downloads/acquisition (if desired) are handled by the pipeline step, not by this job function."
    )

    if not playlist_items:
        logger.warning(
            "No Plex items matched; skipping playlist creation. "
            "(This is expected if you are in recommendations mode and the recommended tracks aren't in your library.)"
        )
        return

    plex.upsert_playlist(playlist_name, playlist_items)


def _run_playlist_from_report_mode(
    *,
    report_path: str | None,
    plex_baseurl: str,
    plex_token: str,
    plex_music_library: str,
    playlist_name: str,
    dry_run: bool,
) -> None:
    """Read keys from the CSV report, wait for Plex to scan, and build the playlist."""
    import csv
    import time
    
    if not report_path or not Path(report_path).is_file():
        logger.error("playlist_from_report mode requires a valid REPORT_PATH CSV currently.")
        return

    logger.info("Running in 'playlist_from_report' mode.")
    
    # Prefer the report CSV (missing-only, capped to MAX_TRACKS).
    # If an evaluated CSV is present, we can optionally use it ONLY to filter down
    # to the selected rows (selected_for_playlist==1). It is NOT a "confirmed downloads" list.
    report_dir = os.path.dirname(report_path)
    evaluated_report = os.path.join(report_dir, "discover-weekly-report-evaluated.csv")

    wanted_tracks: list[Track] = []
    source_label = "report"

    def _load_from_report_csv(path: str) -> list[Track]:
        out: list[Track] = []
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not row:
                    continue
                if "artist" in row and "title" in row:
                    out.append(Track(artist=(row["artist"] or ""), title=(row["title"] or "")))
        return out

    try:
        wanted_tracks = _load_from_report_csv(report_path)
    except Exception as e:
        logger.error("Failed to read report %s: %s", report_path, e)
        return

    # If evaluated exists and has selection flags, filter to only selected tracks.
    if os.path.exists(evaluated_report):
        try:
            with open(evaluated_report, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                if reader.fieldnames and "selected_for_playlist" in reader.fieldnames:
                    selected: list[Track] = []
                    for row in reader:
                        if not row:
                            continue
                        if (row.get("selected_for_playlist") or "").strip() not in {"1", "true", "True"}:
                            continue
                        if "artist" in row and "title" in row:
                            selected.append(Track(artist=(row["artist"] or ""), title=(row["title"] or "")))
                    if selected:
                        wanted_tracks = selected
                        source_label = "evaluated(selected_only)"
        except Exception:
            # Non-fatal: keep using the main report CSV
            pass

    logger.info("Loaded %d tracks from %s CSV. Connecting to Plex...", len(wanted_tracks), source_label)
    
    if not wanted_tracks:
        logger.warning("No tracks found in the report CSV.")
        return
        
    # 2. CHECK WHICH ONES ACTUALLY DOWNLOADED (The "evaluated" csv generated by the scraper isn't available here easily)
    # Instead, we are trusting the scraper to have done its job, BUT we will filter the "wanted" list
    # by removing items that we KNOW failed if the scraper produced a failure report (it doesn't currently).
    
    # The user wants to "check from a new list made of the file confirmed to be downloaded".
    # The scraper (the Go app) writes a "discover-weekly-report-evaluated.csv" or similar if we asked it to,
    # but currently the Go app only outputs text to stdout.
    
    # WORKAROUND: We will proceed with the full list, BUT we will modify the logging
    # to stop complaining about missing tracks if they simply weren't found in Plex.
    # The user logic is: "If it's not in Plex, it probably failed to download, so don't list it as 'Missing from Plex scan' warning."
    
    try:
        plex = PlexClient(plex_baseurl, plex_token, plex_music_library)
    except Exception as e:
        logger.error("Failed to connect to Plex: %s", e)
        return

    if dry_run:
        logger.info("DRY_RUN=1; skipping scan and playlist creation.")
        return

    # Trigger a scan to pick up the newly downloaded files
    logger.info("Triggering Plex library scan...")
    did_trigger_scan = plex.update_library()

    # Wait for Plex to index the newly downloaded files.
    # - If the scan was triggered successfully, wait 90 s (Plex needs time to hash + match).
    # - If the scan failed (e.g. 403 on a shared account), wait 30 s anyway — Plex's own
    #   file-system watcher or periodic auto-scan may still pick up the new files.
    if did_trigger_scan:
        scan_wait_seconds = 90
        logger.info("Waiting %d seconds for Plex to digest new files...", scan_wait_seconds)
        time.sleep(scan_wait_seconds)
    else:
        scan_wait_seconds = 30
        logger.info(
            "Plex scan could not be triggered (common for shared users); "
            "waiting %d seconds for Plex auto-scan to pick up new files...",
            scan_wait_seconds,
        )
        time.sleep(scan_wait_seconds)

    # Match every track from the report CSV against Plex by title/artist.
    # This is the correct approach: the playlist should contain the tracks the user
    # asked for, not just whichever N tracks happen to be most-recently-added globally.
    logger.info("Looking up %d report tracks in Plex by title/artist...", len(wanted_tracks))
    playlist_items = []
    missing_after_scan = []
    for t in wanted_tracks:
        item = plex.find_track(t)
        if item:
            playlist_items.append(item)
        else:
            missing_after_scan.append(t)
    logger.info("Found %d/%d tracks in Plex.", len(playlist_items), len(wanted_tracks))
    if missing_after_scan:
        logger.debug(
            "Tracks not yet in Plex (not scanned or download failed): %s",
            "; ".join([f"{m.artist} - {m.title}" for m in missing_after_scan[:10]]),
        )

    plex.upsert_playlist(playlist_name, playlist_items)
