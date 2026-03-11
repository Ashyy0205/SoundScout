
import os
import sys
import logging
import time
import datetime as dt
from pathlib import Path
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger


# Allow running this launcher directly from a source checkout without installing the package.
_SRC_DIR = Path(__file__).resolve().parent / "src"
if _SRC_DIR.exists():
    sys.path.insert(0, str(_SRC_DIR))


from soundscout.pipeline import run_full_pipeline

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("pipeline")

# Throttle scheduler "nothing due" logs (once per minute).
_last_no_due_log_slot: str | None = None


def _is_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


# Web UI access logs (Werkzeug) are very noisy because the frontend polls endpoints like
# /api/downloads every second. Default to quiet unless explicitly enabled.
if not _is_truthy(os.environ.get("WEBUI_ACCESS_LOG", "0")):
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    logging.getLogger("werkzeug.serving").setLevel(logging.WARNING)
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)


def _webui_data_dir() -> Path:
    """Directory for WebUI state.

    Keep this in sync with the Web UI backend:
    - Prefer WEBUI_DATA_DIR if set
    - In Docker, prefer /config (mounted)
    - Locally, fall back to ./config
    """

    env_dir = (os.environ.get("WEBUI_DATA_DIR") or "").strip()
    if env_dir:
        p = Path(env_dir)
        p.mkdir(parents=True, exist_ok=True)
        return p

    docker_dir = Path("/config")
    if docker_dir.exists():
        return docker_dir

    local_dir = Path.cwd() / "config"
    local_dir.mkdir(parents=True, exist_ok=True)
    return local_dir


def _load_webui_user_store() -> dict:
    path = _webui_data_dir() / "webui_users.json"
    try:
        if not path.exists():
            return {}
        import json

        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_webui_user_store(store: dict) -> None:
    try:
        import json

        path = _webui_data_dir() / "webui_users.json"
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(store, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(path)
    except Exception:
        return


def _now_in_tz() -> dt.datetime:
    tz_name = (os.environ.get("TZ") or "UTC").strip() or "UTC"
    try:
        from zoneinfo import ZoneInfo

        tz = ZoneInfo(tz_name)
        return dt.datetime.now(tz)
    except Exception:
        return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)


def _slot_string(now: dt.datetime) -> str:
    return now.strftime("%Y-%m-%dT%H:%M")


def _tz_name_for_log() -> str:
    return (os.environ.get("TZ") or "UTC").strip() or "UTC"


def _count_store(store: dict, *, now: dt.datetime) -> dict[str, int]:
    """Best-effort counts for scheduler diagnostics."""
    total = 0
    opted_in = 0
    has_lastfm = 0
    has_plex_token = 0
    due_now = 0

    now_wd = int(now.weekday())
    now_hhmm = now.strftime("%H:%M")
    slot = _slot_string(now)

    for _key in store.keys():
        rec = store.get(_key)
        if not isinstance(rec, dict):
            continue
        total += 1

        ad = rec.get("auto_discovery")
        if not isinstance(ad, dict) or not bool(ad.get("enabled")):
            continue
        opted_in += 1

        lf = rec.get("lastfm")
        lastfm_username = ""
        if isinstance(lf, dict):
            lastfm_username = (lf.get("username") or "").strip()
        if lastfm_username:
            has_lastfm += 1

        plex = rec.get("plex")
        plex_token = ""
        if isinstance(plex, dict):
            plex_token = (plex.get("token") or "").strip()
        if plex_token:
            has_plex_token += 1

        # Due only if all required parts exist.
        if not (lastfm_username and plex_token):
            continue

        try:
            wd = int(ad.get("weekday"))
        except Exception:
            wd = -1
        t = str(ad.get("time") or "").strip()
        if wd != now_wd or t != now_hhmm:
            continue
        if str(ad.get("last_run_slot") or "").strip() == slot:
            continue
        due_now += 1

    return {
        "total": total,
        "opted_in": opted_in,
        "has_lastfm": has_lastfm,
        "has_plex_token": has_plex_token,
        "due_now": due_now,
    }


def _select_users_to_run(store: dict, *, force: bool) -> list[dict[str, str]]:
    """Return runnable user records, optionally filtering by per-user schedule.

    Each item: {"lastfm_username": str, "plex_token": str, "plex_username": str, "plex_baseurl": str}
    """

    if not store:
        return []

    now = _now_in_tz()
    slot = _slot_string(now)
    now_hhmm = now.strftime("%H:%M")
    now_wd = int(now.weekday())

    results: list[dict[str, str]] = []
    modified = False

    for _key in sorted(store.keys(), key=lambda x: str(x)):
        rec = store.get(_key)
        if not isinstance(rec, dict):
            continue

        ad = rec.get("auto_discovery")
        if not isinstance(ad, dict):
            ad = {}
        enabled = bool(ad.get("enabled"))
        if not enabled:
            continue

        if not force:
            try:
                wd = int(ad.get("weekday"))
            except Exception:
                wd = -1
            t = str(ad.get("time") or "").strip()
            if wd != now_wd:
                continue
            if t != now_hhmm:
                continue
            if str(ad.get("last_run_slot") or "").strip() == slot:
                continue

        lf = rec.get("lastfm")
        if not isinstance(lf, dict):
            continue
        lastfm_username = (lf.get("username") or "").strip()
        if not lastfm_username:
            continue

        plex = rec.get("plex")
        if not isinstance(plex, dict):
            continue
        plex_token = (plex.get("token") or "").strip()
        if not plex_token:
            continue
        plex_username = (plex.get("username") or plex.get("title") or "").strip()
        plex_baseurl = (plex.get("baseurl") or "").strip()

        # Claim the slot before running to prevent duplicate runs if the check fires twice.
        ad["last_run_slot"] = slot
        rec["auto_discovery"] = ad
        store[_key] = rec
        modified = True

        results.append(
            {
                "lastfm_username": lastfm_username,
                "plex_token": plex_token,
                "plex_username": plex_username,
                "plex_baseurl": plex_baseurl,
            }
        )

    if modified:
        _save_webui_user_store(store)

    # De-dupe by (plex_token,lastfm_username) to avoid accidental duplicate runs
    seen: set[tuple[str, str]] = set()
    deduped: list[dict[str, str]] = []
    for r in results:
        k = (r["plex_token"], r["lastfm_username"])
        if k in seen:
            continue
        seen.add(k)
        deduped.append(r)
    return deduped

def _run_pipeline_wrapper(*, force: bool = False) -> None:
    try:
        store = _load_webui_user_store()
        users = _select_users_to_run(store, force=force)
        if not users:
            if force:
                logger.warning("No opted-in users found (auto discovery enabled + Last.fm + Plex token).")
            else:
                global _last_no_due_log_slot
                now = _now_in_tz()
                slot = _slot_string(now)
                if _last_no_due_log_slot != slot:
                    _last_no_due_log_slot = slot
                    counts = _count_store(store, now=now) if isinstance(store, dict) else {}
                    logger.info(
                        "No auto-discovery runs due now (tz=%s now=%s). users=%s opted_in=%s lastfm=%s plex=%s due=%s",
                        _tz_name_for_log(),
                        now.strftime("%a %Y-%m-%d %H:%M"),
                        counts.get("total", 0),
                        counts.get("opted_in", 0),
                        counts.get("has_lastfm", 0),
                        counts.get("has_plex_token", 0),
                        counts.get("due_now", 0),
                    )
            return

        for i, u in enumerate(users, start=1):
            lf = u.get("lastfm_username", "")
            pt = u.get("plex_token", "")
            pu = u.get("plex_username", "")
            pb = u.get("plex_baseurl", "")
            label = f"{pu} / {lf}" if pu else lf
            logger.info(f"Running discovery for user {i}/{len(users)}: {label}")
            run_full_pipeline(lastfm_username=lf, plex_token=pt, plex_baseurl=pb or None)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")

def main():
    # Helper to check if we asked for a manual triggered run
    if len(sys.argv) > 1 and sys.argv[1] == "--now":
        _run_pipeline_wrapper(force=True)
        return

    # Check if Web UI is enabled
    enable_webui = os.environ.get("ENABLE_WEBUI", "true").lower() == "true"
    webui_port = int(os.environ.get("WEBUI_PORT", "5000"))
    
    # Back-compat: older setups used CRON_SCHEDULE; Settings() uses SCHEDULE_CRON.
    # These are now used only as a DEFAULT for per-user scheduling in the UI.
    cron_schedule = os.environ.get("CRON_SCHEDULE") or os.environ.get("SCHEDULE_CRON")

    if enable_webui:
        # Run scheduler (per-user) and web UI
        if cron_schedule:
            logger.info(
                f"Starting Scheduler (per-user schedules; default: {cron_schedule}) and Web UI (Port: {webui_port})"
            )
        else:
            logger.info(f"Starting Scheduler (per-user schedules) and Web UI (Port: {webui_port})")
        
        # Use BackgroundScheduler so it doesn't block the web UI
        tz_env = os.environ.get("TZ", "UTC")
        scheduler = BackgroundScheduler(timezone=tz_env)
        
        # Trigger immediately on startup?
        if os.environ.get("RUN_ON_STARTUP", "false").lower() == "true":
                        logger.info("RUN_ON_STARTUP is enabled: executing pipeline immediately...")
                        _run_pipeline_wrapper()
             
        scheduler.add_job(_run_pipeline_wrapper, IntervalTrigger(seconds=30))
        scheduler.start()
        logger.info("Scheduler started in background.")
        
        # Start Web UI in main thread (blocking)
        try:
            from soundscout.webui import run_webui
            run_webui(host="0.0.0.0", port=webui_port)
        except ImportError:
            logger.error("Web UI module not found. Install with: pip install flask flask-cors")
            sys.exit(1)
            
    elif cron_schedule:
        # Scheduler only, no web UI (per-user schedule checks still apply)
        logger.info(
            f"Starting Scheduler (per-user schedules; default: {cron_schedule}). Web UI disabled."
        )
        tz_env = os.environ.get("TZ", "UTC")
        scheduler = BackgroundScheduler(timezone=tz_env)

        if os.environ.get("RUN_ON_STARTUP", "false").lower() == "true":
            logger.info("RUN_ON_STARTUP is enabled: executing pipeline immediately...")
            _run_pipeline_wrapper(force=True)

        scheduler.add_job(_run_pipeline_wrapper, IntervalTrigger(seconds=30))
        scheduler.start()
        logger.info("Scheduler started. Waiting for next run...")
        while True:
            time.sleep(60)

    else:
        # No schedule and no web UI, just run once and exit
        _run_pipeline_wrapper(force=True)

if __name__ == "__main__":
    current_path = os.getcwd()
    src_path = os.path.join(current_path, "src")
    if os.path.isdir(src_path):
        sys.path.append(src_path)
        
    main()
