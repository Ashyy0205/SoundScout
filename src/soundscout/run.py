from __future__ import annotations

import logging
import time

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from .config import load_settings
from .job import run_job
from .logging_setup import setup_logging

logger = logging.getLogger(__name__)


def _run_once() -> None:
    s = load_settings()
    setup_logging(s.log_level)

    run_job(
        lastfm_api_key=s.lastfm_api_key,
        lastfm_username=s.lastfm_username,
        lastfm_mode=s.lastfm_mode,
        lastfm_seed_count=s.lastfm_seed_count,
        lastfm_similar_per_seed=s.lastfm_similar_per_seed,
        plex_baseurl=str(s.plex_baseurl),
        plex_token=s.plex_token,
        plex_music_library=s.plex_music_library,
        playlist_name=s.playlist_name,
        max_tracks=s.max_tracks,
        dry_run=s.dry_run,
        report_path=s.report_path,
        generate_report_only=s.generate_report_only,
        enable_import=s.enable_import,
        import_inbox_dir=s.import_inbox_dir,
        import_cmd=s.import_cmd,
    )


def main() -> None:
    s = load_settings()
    setup_logging(s.log_level)

    if s.run_once or not s.schedule_cron:
        _run_once()
        return

    logger.info("Starting scheduler with cron '%s' (TZ=%s)", s.schedule_cron, s.tz)

    scheduler = BackgroundScheduler(timezone=s.tz)
    trigger = CronTrigger.from_crontab(s.schedule_cron)
    scheduler.add_job(_run_once, trigger=trigger, id="soundscout", replace_existing=True)
    scheduler.start()

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Stopping scheduler")
        scheduler.shutdown()
