from __future__ import annotations

import logging
import os
import re as _re
import subprocess
from pathlib import Path
import csv

from .config import load_settings
from .import_step import run_optional_import
from .job import run_job
from .logging_setup import setup_logging
from .spotify import SpotifyClient

logger = logging.getLogger(__name__)


def _is_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _run_acquire_command(*, report_path: Path) -> bool:
    """Run a user-provided acquisition command.

    This is intentionally generic: users can plug in their own workflow to obtain
    missing tracks and place them into OUTPUT_PATH.
    """

    def _enrich_report_with_spotify_ids(path: Path) -> None:
        """Best-effort: add spotify_id column to the report CSV.

        This avoids relying on the scraper's Spotify scraping/search (which can be rate-limited).
        Only runs if SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET are configured.
        """

        try:
            cid = (os.environ.get("SPOTIFY_CLIENT_ID") or "").strip()
            csec = (os.environ.get("SPOTIFY_CLIENT_SECRET") or "").strip()
            if not cid or not csec:
                logger.info("Spotify credentials not configured; skipping spotify_id enrichment")
                return

            if not path.exists():
                return

            rows: list[dict[str, str]] = []
            with path.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    return
                fieldnames = [str(x) for x in reader.fieldnames]

                # Normalize expected columns
                has_artist = any(n.lower() == "artist" for n in fieldnames)
                has_title = any(n.lower() == "title" for n in fieldnames)
                if not (has_artist and has_title):
                    logger.info("Report CSV missing artist/title headers; skipping spotify_id enrichment")
                    return

                for r in reader:
                    if not isinstance(r, dict):
                        continue
                    rows.append({k: (v or "") for k, v in r.items() if k is not None})

            if not rows:
                return

            # If spotify_id already present and mostly filled, do nothing.
            existing_ids = [((r.get("spotify_id") or "").strip()) for r in rows]
            filled = sum(1 for x in existing_ids if x)
            if filled >= max(3, int(len(rows) * 0.8)):
                return

            client = SpotifyClient(cid, csec)
            updated = 0

            for r in rows:
                if (r.get("spotify_id") or "").strip():
                    continue
                artist = (r.get("artist") or "").strip()
                title = (r.get("title") or "").strip()
                if not artist or not title:
                    continue

                q = f'track:"{title}" artist:"{artist}"'
                try:
                    results = client.search(q, "track", limit=5)
                except Exception:
                    results = []

                if results:
                    sid = (results[0].get("spotify_id") or "").strip()
                    if sid:
                        r["spotify_id"] = sid
                        updated += 1

            # Write back with spotify_id column
            out_fields = ["artist", "title", "spotify_id"]
            tmp = path.with_suffix(path.suffix + ".tmp")
            with tmp.open("w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=out_fields)
                w.writeheader()
                for r in rows:
                    w.writerow(
                        {
                            "artist": (r.get("artist") or ""),
                            "title": (r.get("title") or ""),
                            "spotify_id": (r.get("spotify_id") or ""),
                        }
                    )
            tmp.replace(path)

            logger.info("Enriched report with spotify_id for %d/%d tracks", updated, len(rows))
        except Exception as e:
            logger.warning("spotify_id enrichment failed (continuing): %s", e)

    # Try to enrich report with spotify IDs before acquisition.
    _enrich_report_with_spotify_ids(report_path)

    acquire_cmd = (os.environ.get("DISCOVERY_ACQUIRE_CMD") or "").strip()
    if not acquire_cmd:
        # Fallback for containers with built-in scraper
        scraper_bin = os.environ.get("SCRAPER_BIN", "scraper")
        # Check if predictable binary exists or is in PATH (simple check)
        is_container = os.path.exists("/usr/local/bin/scraper") or os.environ.get("IS_DOCKER")
        
        if is_container:
            logger.info("DISCOVERY_ACQUIRE_CMD not set, but running in container; defaulting to built-in scraper.")
            output_dir = os.environ.get("OUTPUT_PATH", "/music")
            # Construct command
            # Note: We use string command for shell=True
            acquire_cmd = f'{scraper_bin} --csv "{report_path}" --output "{output_dir}"'
        else:
            logger.warning(
                "DISCOVERY_ACQUIRE_CMD is not set; skipping acquisition step. "
                "Set DISCOVERY_ACQUIRE_CMD to a command that reads the report CSV and acquires the tracks into OUTPUT_PATH."
            )
            return -1

    env = os.environ.copy()
    env["DISCOVERY_REPORT_PATH"] = str(report_path)
    env["REPORT_PATH"] = str(report_path)

    logger.info("Running acquisition command")
    _OK_RE   = _re.compile(r'^\[TRACK_OK\] (.+?) \|\| (.+?)$')
    _FAIL_RE = _re.compile(r'^\[TRACK_FAIL\] (.+?) \|\| (.+?) \|\| (.*)$')
    _RES_RE  = _re.compile(r'^\[(\d+)/(\d+)\] Resolving platforms: (.+?) - (.+?)$')
    _DL_RE   = _re.compile(r'^\[(\d+)/(\d+)\] Downloading: (.+?) - (.+?)$')

    proc = subprocess.Popen(
        acquire_cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        env=env,
    )
    ok_count = 0
    fail_count = 0
    for raw in (proc.stdout or []):
        line = raw.rstrip()
        if not line:
            continue
        m_ok   = _OK_RE.match(line)
        m_fail = _FAIL_RE.match(line)
        m_res  = _RES_RE.match(line)
        m_dl   = _DL_RE.match(line)
        if m_ok:
            ok_count += 1
            logger.info("  \u2713 %s \u2013 %s", m_ok.group(1), m_ok.group(2))
        elif m_fail:
            fail_count += 1
            logger.warning("  \u2717 %s \u2013 %s: %s", m_fail.group(1), m_fail.group(2), m_fail.group(3))
        elif m_res:
            logger.debug("  [%s/%s] Resolving: %s \u2013 %s", m_res.group(1), m_res.group(2), m_res.group(3), m_res.group(4))
        elif m_dl:
            logger.debug("  [%s/%s] Downloading: %s \u2013 %s", m_dl.group(1), m_dl.group(2), m_dl.group(3), m_dl.group(4))
    proc.wait()
    if ok_count + fail_count > 0:
        logger.info("Acquisition complete: %d downloaded, %d failed", ok_count, fail_count)
    if proc.returncode != 0:
        raise RuntimeError(f"Acquisition command failed with exit code {proc.returncode}")
    return ok_count


def _default_report_path() -> Path:
    # Keep this stable and easy to find inside the container.
    return Path.cwd() / "soundscout-report.csv"


def run_full_pipeline(
    *,
    lastfm_username: str,
    plex_token: str | None = None,
    plex_baseurl: str | None = None,
    playlist_name: str | None = None,
) -> Path:
    """Run the full discovery pipeline:

    1) Generate recommendations report CSV (missing-only)
    2) Download missing tracks via scraper
    3) Trigger Plex scan + create/update the playlist from the report

    Returns the report CSV path.
    """

    s = load_settings()
    setup_logging(s.log_level)

    report_path = Path(s.report_path) if s.report_path else _default_report_path()
    report_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info("--- Starting SoundScout Pipeline ---")

    effective_lastfm_username = (lastfm_username or "").strip()
    if not effective_lastfm_username:
        raise RuntimeError("Missing Last.fm username")

    effective_plex_token = (plex_token or s.plex_token or "").strip()
    if not effective_plex_token:
        raise RuntimeError("Missing Plex token")

    effective_plex_baseurl = (plex_baseurl or str(s.plex_baseurl) or "").strip()
    if not effective_plex_baseurl:
        raise RuntimeError("Missing Plex base URL")

    effective_playlist_name = (playlist_name or s.playlist_name or "").strip() or "Discover Weekly"

    logger.info("[Step 1/3] Generating recommendations report")
    run_job(
        lastfm_api_key=s.lastfm_api_key,
        lastfm_username=effective_lastfm_username,
        lastfm_mode=s.lastfm_mode,
        lastfm_seed_count=s.lastfm_seed_count,
        lastfm_similar_per_seed=s.lastfm_similar_per_seed,
        plex_baseurl=effective_plex_baseurl,
        plex_token=effective_plex_token,
        plex_music_library=s.plex_music_library,
        playlist_name=effective_playlist_name,
        max_tracks=s.max_tracks,
        dry_run=True,  # report-only; no Plex mutations in this step
        report_path=str(report_path),
        generate_report_only=True,
        enable_import=False,  # import happens after download
        import_inbox_dir=s.import_inbox_dir,
        import_cmd=s.import_cmd,
    )

    if not report_path.exists():
        raise RuntimeError(f"Report CSV not found at {report_path}")

    acquire_enabled = _is_truthy(os.environ.get("DISCOVERY_ACQUIRE", "1"))

    if not acquire_enabled:
        logger.info("DISCOVERY_ACQUIRE=0; skipping acquisition + playlist build")
        return report_path

    if s.dry_run:
        logger.info("DRY_RUN=1; skipping download + playlist build")
        return report_path

    logger.info("[Step 2/3] Acquiring missing tracks")
    ok_count = _run_acquire_command(report_path=report_path)
    if ok_count < 0:
        logger.warning("Acquisition step was skipped; not scanning Plex or building playlist.")
        return report_path
    if ok_count == 0:
        logger.warning("No tracks downloaded successfully; skipping playlist build.")
        return report_path

    # Optional post-download import/tagging step (for users who route downloads into an inbox)
    run_optional_import(s.enable_import, s.import_inbox_dir, s.import_cmd)

    logger.info("[Step 3/3] Scanning Plex + building playlist (%d most recently added tracks)", ok_count)
    run_job(
        lastfm_api_key=s.lastfm_api_key,
        lastfm_username=effective_lastfm_username,
        lastfm_mode="playlist_from_report",
        lastfm_seed_count=s.lastfm_seed_count,
        lastfm_similar_per_seed=s.lastfm_similar_per_seed,
        plex_baseurl=effective_plex_baseurl,
        plex_token=effective_plex_token,
        plex_music_library=s.plex_music_library,
        playlist_name=effective_playlist_name,
        max_tracks=s.max_tracks,
        dry_run=False,
        report_path=str(report_path),
        generate_report_only=False,
        enable_import=False,
        import_inbox_dir=s.import_inbox_dir,
        import_cmd=s.import_cmd,
        recently_added_count=ok_count,
    )

    logger.info("--- Pipeline finished successfully ---")
    return report_path
