from __future__ import annotations

from pathlib import Path

from pydantic import AnyHttpUrl, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


_PROJECT_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=(str(_PROJECT_ROOT / ".env.example"), str(_PROJECT_ROOT / ".env")),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Last.fm — API key is bundled; override via LASTFM_API_KEY env var if needed
    lastfm_api_key: str = "5c9d9b9e8b5b545d516079408ef2a07c"
    # Optional: used for non-WebUI single-user runs only.
    # Preferred: link Last.fm per Plex user in the Web UI (Settings).
    lastfm_username: str = ""

    # Last.fm behavior
    # - weekly_plays: last week's listening chart (what you played)
    # - recommendations: generate recommendations using track.getSimilar seeded by your last week plays
    lastfm_mode: str = "recommendations"
    lastfm_seed_count: int = 25
    lastfm_similar_per_seed: int = 5

    # Plex
    plex_baseurl: AnyHttpUrl
    # Intentionally *not* sourced from the environment anymore.
    # Plex tokens are captured via Web UI login and stored in /config.
    plex_token: str = Field(default="", validation_alias="DISABLED_PLEX_AUTH_TOKEN")
    plex_music_library: str

    # Behavior
    playlist_name: str = "Discover Weekly"
    # Last.fm's recommendation UI commonly surfaces ~60 tracks.
    max_tracks: int = 60
    log_level: str = "INFO"

    # Safety / reporting
    dry_run: bool = False
    report_path: str | None = None
    generate_report_only: bool = False

    # Scheduling
    schedule_cron: str | None = None
    tz: str = "UTC"

    # Force a one-shot run even if SCHEDULE_CRON is set
    run_once: bool = False

    # Optional import step (for files you already have)
    enable_import: bool = False
    import_inbox_dir: str = "/inbox"
    import_cmd: str | None = None

    @field_validator("schedule_cron", mode="before")
    @classmethod
    def _blank_schedule_to_none(cls, v):
        if v is None:
            return None
        if isinstance(v, str) and not v.strip():
            return None
        return v


def load_settings() -> Settings:
    return Settings()
