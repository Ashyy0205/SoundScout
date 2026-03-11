from __future__ import annotations

import logging
import os
import base64
import subprocess
import json
import time
import copy
import datetime as dt
import re
import secrets
import threading
import shlex
import xml.etree.ElementTree as ET
from urllib.parse import urlparse
from urllib.parse import quote
from urllib.parse import urlencode
from pathlib import Path
from functools import lru_cache
from flask import Flask, jsonify, request, render_template, send_from_directory, Response, session
from flask_cors import CORS

import requests
import concurrent.futures

from .lastfm import LastFmClient
from .spotify import SpotifyClient, _pkce_verifier, _pkce_challenge
from .config import load_settings
from .pipeline import run_full_pipeline
from .logging_setup import setup_logging
from .plex import PlexClient

logger = logging.getLogger(__name__)

app = Flask(__name__, 
            template_folder=str(Path(__file__).parent / "templates"),
            static_folder=str(Path(__file__).parent / "static"))
CORS(app)

# Session configuration (required for Plex login)
_webui_secret = (os.environ.get("WEBUI_SECRET_KEY") or os.environ.get("SECRET_KEY") or "").strip()
if not _webui_secret:
    # Deterministic within-process fallback. For production, set WEBUI_SECRET_KEY.
    _webui_secret = secrets.token_hex(32)

app.secret_key = _webui_secret
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
)

# Configuration from environment
SCRAPER_BIN = os.environ.get("SCRAPER_BIN", "scraper")
MUSIC_LIBRARY_PATH = Path(os.environ.get("OUTPUT_PATH", "/music"))
PLEX_MUSIC_LIBRARY = (os.environ.get("PLEX_MUSIC_LIBRARY") or "Music").strip() or "Music"
LASTFM_API_KEY = os.environ.get("LASTFM_API_KEY", "5c9d9b9e8b5b545d516079408ef2a07c")
LASTFM_USERNAME = os.environ.get("LASTFM_USERNAME", "")
WEBUI_PUBLIC_URL = (os.environ.get("WEBUI_PUBLIC_URL") or "").rstrip("/")

# Obfuscated built-in Spotify Client ID (XOR-encoded, key=b'soundscout').
# This allows zero-config OAuth without exposing a plain-text credential.
_SP_CID_K = b'soundscout'
_SP_CID_D = (68,90,64,93,81,21,81,90,68,66,18,94,65,88,6,18,1,14,17,18,18,87,65,89,81,23,2,89,71,77,18,12)


def _builtin_spotify_client_id() -> str:
    return ''.join(chr(b ^ _SP_CID_K[i % len(_SP_CID_K)]) for i, b in enumerate(_SP_CID_D))


SEARCH_PROVIDER = os.environ.get("SEARCH_PROVIDER", "lastfm").strip().lower()

# Clamp displayed download speed (Mbps) to avoid absurd spikes from instantaneous estimates.
try:
    WEBUI_SPEED_MBPS_CAP = float((os.environ.get("WEBUI_SPEED_MBPS_CAP", "1000") or "1000").strip())
except Exception:
    WEBUI_SPEED_MBPS_CAP = 1000.0
WEBUI_SPEED_MBPS_CAP = max(1.0, WEBUI_SPEED_MBPS_CAP)

# Max simultaneous scraper processes when downloading a batch of tracks.
# Increase via DOWNLOAD_CONCURRENCY env var if your network/service allows it.
try:
    DOWNLOAD_CONCURRENCY = max(1, int((os.environ.get("DOWNLOAD_CONCURRENCY") or "3").strip()))
except Exception:
    DOWNLOAD_CONCURRENCY = 3

# Per-track subprocess timeout in seconds. If a scraper process doesn't finish
# within this time it is killed and the track is marked failed.
try:
    DOWNLOAD_TRACK_TIMEOUT_S = max(30, int((os.environ.get("DOWNLOAD_TRACK_TIMEOUT_S") or "300").strip()))
except Exception:
    DOWNLOAD_TRACK_TIMEOUT_S = 300

# Web UI Plex login gate
# Default ON so users must authenticate with Plex when using the Web UI.
WEBUI_REQUIRE_PLEX_LOGIN = os.environ.get("WEBUI_REQUIRE_PLEX_LOGIN", "1").strip().lower() in {"1", "true", "yes", "y", "on"}
PLEX_BASEURL = (os.environ.get("PLEX_BASEURL", "") or "").strip().rstrip("/")
PLEX_VERIFY_SSL = os.environ.get("PLEX_VERIFY_SSL", "1").strip().lower() not in {"0", "false", "no", "n", "off"}

# Plex OAuth client identifier
PLEX_OAUTH_CLIENT_ID = (
    os.environ.get("PLEX_OAUTH_CLIENT_ID")
    or os.environ.get("PLEX_CLIENT_ID")
    or os.environ.get("X_PLEX_CLIENT_IDENTIFIER")
    or ""
).strip()
if not PLEX_OAUTH_CLIENT_ID:
    # Stable-ish default: hostname-based
    PLEX_OAUTH_CLIENT_ID = f"soundscout-webui-{os.environ.get('HOSTNAME', 'local')}"


def _webui_data_dir() -> Path:
    """Directory for WebUI state.

    In Docker, we prefer /config (mounted in docker-compose.yml).
    For local runs, we fall back to ./config.
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


_USER_STORE_PATH = _webui_data_dir() / "webui_users.json"
_user_store_lock = threading.Lock()

_HISTORY_PATH = _webui_data_dir() / "download_history.json"
_history_lock = threading.Lock()
_HISTORY_MAX_ENTRIES = 500

try:
    ARTIST_MONITOR_INTERVAL_S = max(
        300,
        int(float((os.environ.get("ARTIST_MONITOR_INTERVAL_MINUTES") or "60").strip()) * 60),
    )
except Exception:
    ARTIST_MONITOR_INTERVAL_S = 3600

try:
    ARTIST_MONITOR_RELEASE_SCAN_LIMIT = max(
        1,
        min(12, int((os.environ.get("ARTIST_MONITOR_RELEASE_SCAN_LIMIT") or "6").strip())),
    )
except Exception:
    ARTIST_MONITOR_RELEASE_SCAN_LIMIT = 6

_ARTIST_MONITOR_MAX_SEEN_RELEASES = 25
_ARTIST_MONITOR_RETRY_FAILURE_LIMIT = 3
_ARTIST_MONITOR_RETRY_COOLDOWN_S = 7 * 24 * 60 * 60


def _parse_default_autodiscovery_from_cron() -> tuple[int, str]:
    """Return (weekday, time_str) default for the UI.

    Weekday uses Python's convention: 0=Mon .. 6=Sun.
    Time is HH:MM (24h).
    """

    # Try to keep this aligned with the docker template defaults.
    raw = (os.environ.get("CRON_SCHEDULE") or os.environ.get("SCHEDULE_CRON") or "").strip()
    # Default to Monday 05:00.
    default_dow = 0
    default_time = "05:00"

    if not raw:
        return default_dow, default_time

    parts = raw.split()
    if len(parts) != 5:
        return default_dow, default_time

    minute_s, hour_s, _dom, _mon, dow_s = parts
    try:
        minute = int(minute_s)
        hour = int(hour_s)
        if not (0 <= minute <= 59 and 0 <= hour <= 23):
            raise ValueError
        default_time = f"{hour:02d}:{minute:02d}"
    except Exception:
        default_time = "05:00"

    # Cron: 0-6 where 0=Sun. Accept 7 as Sun.
    try:
        if dow_s.isdigit():
            d = int(dow_s)
            if d == 7:
                d = 0
            if 0 <= d <= 6:
                # Convert to Python weekday (0=Mon..6=Sun)
                # Cron 0=Sun -> Python 6, Cron 1=Mon -> Python 0, ...
                default_dow = (d - 1) % 7
    except Exception:
        default_dow = 0

    return default_dow, default_time


def _get_autodiscovery_settings_for_key(user_key: str) -> dict:
    user_key = (user_key or "").strip()
    if not user_key:
        return {"enabled": False, "weekday": 0, "time": "05:00"}

    default_dow, default_time = _parse_default_autodiscovery_from_cron()

    with _user_store_lock:
        store = _load_user_store()
        user = store.get(user_key) or {}

    if not isinstance(user, dict):
        return {"enabled": False, "weekday": default_dow, "time": default_time}

    ad = user.get("auto_discovery")
    if not isinstance(ad, dict):
        return {"enabled": False, "weekday": default_dow, "time": default_time}

    enabled = bool(ad.get("enabled"))
    try:
        weekday = int(ad.get("weekday", default_dow))
    except Exception:
        weekday = default_dow
    weekday = max(0, min(6, weekday))

    t = str(ad.get("time") or default_time).strip()
    if not re.match(r"^\d{2}:\d{2}$", t):
        t = default_time
    try:
        hh = int(t.split(":", 1)[0])
        mm = int(t.split(":", 1)[1])
        if not (0 <= hh <= 23 and 0 <= mm <= 59):
            t = default_time
    except Exception:
        t = default_time

    return {"enabled": enabled, "weekday": weekday, "time": t}


def _set_autodiscovery_settings(enabled: bool, weekday: int, time_str: str) -> None:
    key = _plex_user_key()
    if not key:
        raise RuntimeError("Not authenticated")

    # Only require Last.fm when enabling.
    if enabled and not _get_linked_lastfm_username():
        raise ValueError("Last.fm must be linked to enable auto discovery")

    try:
        weekday_i = int(weekday)
    except Exception:
        weekday_i = 0
    weekday_i = max(0, min(6, weekday_i))

    t = (time_str or "").strip()
    if not re.match(r"^\d{2}:\d{2}$", t):
        raise ValueError("Invalid time format (expected HH:MM)")
    hh, mm = t.split(":", 1)
    try:
        hhi = int(hh)
        mmi = int(mm)
        if not (0 <= hhi <= 23 and 0 <= mmi <= 59):
            raise ValueError
    except Exception:
        raise ValueError("Invalid time value")

    with _user_store_lock:
        store = _load_user_store()
        user = store.get(key)
        if not isinstance(user, dict):
            user = {}
        ad = user.get("auto_discovery")
        if not isinstance(ad, dict):
            ad = {}

        ad["enabled"] = bool(enabled)
        ad["weekday"] = weekday_i
        ad["time"] = t
        # Clear run marker when settings change to avoid "stuck" schedule.
        ad.pop("last_run_slot", None)

        user["auto_discovery"] = ad
        store[key] = user
        _save_user_store(store)


def _load_user_store() -> dict:
    try:
        if not _USER_STORE_PATH.exists():
            return {}
        raw = _USER_STORE_PATH.read_text(encoding="utf-8")
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_user_store(data: dict) -> None:
    _USER_STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = _USER_STORE_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(_USER_STORE_PATH)


def _normalize_monitor_artist_name(artist: str) -> str:
    return re.sub(r"\s+", " ", str(artist or "").strip())


def _monitored_artist_key(artist: str) -> str:
    return _norm_artist(_normalize_monitor_artist_name(artist))


def _clean_seen_release_keys(values: object) -> list[str]:
    seen: list[str] = []
    if not isinstance(values, list):
        return seen
    for value in values:
        key = str(value or "").strip()
        if key and key not in seen:
            seen.append(key)
        if len(seen) >= _ARTIST_MONITOR_MAX_SEEN_RELEASES:
            break
    return seen


def _clean_release_retry_state(values: object) -> dict[str, dict[str, int | str]]:
    cleaned: dict[str, dict[str, int | str]] = {}
    if not isinstance(values, dict):
        return cleaned
    for release_key, raw in values.items():
        key = str(release_key or "").strip()
        if not key or not isinstance(raw, dict):
            continue
        try:
            consecutive_failures = max(0, int(raw.get("consecutive_failures") or 0))
        except Exception:
            consecutive_failures = 0
        try:
            cooldown_until = max(0, int(raw.get("cooldown_until") or 0))
        except Exception:
            cooldown_until = 0
        try:
            last_attempted_at = max(0, int(raw.get("last_attempted_at") or 0))
        except Exception:
            last_attempted_at = 0
        cleaned[key] = {
            "consecutive_failures": consecutive_failures,
            "cooldown_until": cooldown_until,
            "last_attempted_at": last_attempted_at,
            "last_status": str(raw.get("last_status") or "").strip(),
        }
    return cleaned


def _clean_monitored_artist_entry(entry: object) -> dict | None:
    if not isinstance(entry, dict):
        return None

    artist = _normalize_monitor_artist_name(entry.get("artist") or "")
    if not artist:
        return None

    return {
        "artist": artist,
        "artist_key": _monitored_artist_key(artist),
        "enabled_at": int(entry.get("enabled_at") or time.time()),
        "last_checked_at": int(entry.get("last_checked_at") or 0),
        "last_error": str(entry.get("last_error") or "").strip(),
        "last_seen_release_key": str(entry.get("last_seen_release_key") or "").strip(),
        "last_seen_release_name": str(entry.get("last_seen_release_name") or "").strip(),
        "last_seen_release_ts": int(entry.get("last_seen_release_ts") or 0),
        "seen_release_keys": _clean_seen_release_keys(entry.get("seen_release_keys") or []),
        "release_retry_state": _clean_release_retry_state(entry.get("release_retry_state") or {}),
    }


def _clean_monitored_artist_list(values: object) -> list[dict]:
    out: list[dict] = []
    seen_keys: set[str] = set()
    if not isinstance(values, list):
        return out
    for raw in values:
        cleaned = _clean_monitored_artist_entry(raw)
        if not cleaned:
            continue
        artist_key = cleaned["artist_key"]
        if artist_key in seen_keys:
            continue
        seen_keys.add(artist_key)
        out.append(cleaned)
    out.sort(key=lambda item: (int(item.get("enabled_at") or 0), item.get("artist", "").lower()))
    return out


def _artist_release_sort_key(info: dict) -> tuple[int, str]:
    try:
        published_ts = int(info.get("published_ts") or 0)
    except Exception:
        published_ts = 0
    return (published_ts, _norm_text(info.get("album") or ""))


def _artist_release_key(info: dict) -> str:
    artist = _normalize_monitor_artist_name(info.get("artist") or "")
    album = str(info.get("album") or "").strip()
    if not artist or not album:
        return ""
    try:
        published_ts = int(info.get("published_ts") or 0)
    except Exception:
        published_ts = 0
    return f"{_monitored_artist_key(artist)}|||{_norm_text(album)}|||{published_ts}"


def _get_artist_release_candidates(artist: str, album_limit: int) -> list[dict]:
    artist_name = _normalize_monitor_artist_name(artist)
    if not artist_name or not LASTFM_API_KEY:
        return []

    limit = max(1, min(int(album_limit or 1), 15))
    lastfm = LastFmClient(api_key=LASTFM_API_KEY, username=LASTFM_USERNAME)
    albums = lastfm.get_artist_albums(artist_name, limit=limit) or []
    if not albums:
        return []

    names: list[str] = []
    for album in albums:
        name = str(album.get("name") or "").strip()
        if not name or name in names:
            continue
        names.append(name)
        if len(names) >= limit:
            break
    if not names:
        return []

    infos: list[dict] = []
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(6, len(names))) as ex:
            futures = [ex.submit(lastfm.get_album_tracks_detailed, artist_name, name) for name in names]
            for fut in concurrent.futures.as_completed(futures):
                try:
                    info = fut.result() or {}
                except Exception:
                    continue
                if isinstance(info, dict) and (info.get("tracks") or []):
                    info["artist"] = artist_name
                    infos.append(info)
    except Exception:
        infos = []

    if not infos:
        fallback = lastfm.get_album_tracks_detailed(artist_name, names[0]) or {}
        if isinstance(fallback, dict) and (fallback.get("tracks") or []):
            fallback["artist"] = artist_name
            infos = [fallback]

    deduped: list[dict] = []
    seen_release_keys: set[str] = set()
    for info in sorted(infos, key=_artist_release_sort_key, reverse=True):
        release_key = _artist_release_key(info)
        if not release_key or release_key in seen_release_keys:
            continue
        seen_release_keys.add(release_key)
        deduped.append(info)
    return deduped


def _get_monitored_artist_entry_for_key(user_key: str, artist: str) -> dict | None:
    user_key = (user_key or "").strip()
    artist_key = _monitored_artist_key(artist)
    if not user_key or not artist_key:
        return None

    with _user_store_lock:
        store = _load_user_store()
        user = store.get(user_key) or {}
    if not isinstance(user, dict):
        return None

    monitors = _clean_monitored_artist_list(user.get("monitored_artists") or [])
    for item in monitors:
        if item.get("artist_key") == artist_key:
            return item
    return None


def _set_artist_monitoring(artist: str, monitored: bool) -> dict:
    key = _plex_user_key()
    if not key:
        raise RuntimeError("Not authenticated")

    artist_name = _normalize_monitor_artist_name(artist)
    if not artist_name:
        raise ValueError("Missing artist")
    artist_key = _monitored_artist_key(artist_name)

    seen_release_keys: list[str] = []
    newest_release_name = ""
    newest_release_ts = 0

    if monitored:
        if not LASTFM_API_KEY:
            raise ValueError("LASTFM_API_KEY is required for artist monitoring")
        candidates = _get_artist_release_candidates(artist_name, ARTIST_MONITOR_RELEASE_SCAN_LIMIT)
        for candidate in candidates:
            release_key = _artist_release_key(candidate)
            if release_key and release_key not in seen_release_keys:
                seen_release_keys.append(release_key)
            if not newest_release_name:
                newest_release_name = str(candidate.get("album") or "").strip()
                try:
                    newest_release_ts = int(candidate.get("published_ts") or 0)
                except Exception:
                    newest_release_ts = 0
        seen_release_keys = seen_release_keys[:_ARTIST_MONITOR_MAX_SEEN_RELEASES]

    with _user_store_lock:
        store = _load_user_store()
        user = store.get(key)
        if not isinstance(user, dict):
            user = {}

        monitors = _clean_monitored_artist_list(user.get("monitored_artists") or [])
        monitors = [item for item in monitors if item.get("artist_key") != artist_key]

        if monitored:
            now_ts = int(time.time())
            monitors.append(
                {
                    "artist": artist_name,
                    "artist_key": artist_key,
                    "enabled_at": now_ts,
                    "last_checked_at": now_ts if seen_release_keys else 0,
                    "last_error": "",
                    "last_seen_release_key": seen_release_keys[0] if seen_release_keys else "",
                    "last_seen_release_name": newest_release_name,
                    "last_seen_release_ts": newest_release_ts,
                    "seen_release_keys": seen_release_keys,
                    "release_retry_state": {},
                }
            )

        user["monitored_artists"] = _clean_monitored_artist_list(monitors)
        store[key] = user
        _save_user_store(store)

    return {
        "artist": artist_name,
        "monitored": bool(monitored),
        "last_seen_release_name": newest_release_name,
        "last_seen_release_ts": newest_release_ts,
    }



def _spotify_token_from_sp_dc(sp_dc: str) -> dict:
    """Exchange a Spotify sp_dc session cookie for a web-player access token.

    Uses Spotify's internal get_access_token endpoint — no developer app needed.
    Returns dict with 'access_token' and 'expires_at' (Unix timestamp).
    Raises ValueError if the cookie is invalid/anonymous.
    """
    resp = requests.get(
        "https://open.spotify.com/get_access_token",
        params={"reason": "transport", "productType": "web_player"},
        headers={
            "Cookie": f"sp_dc={sp_dc}",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
        },
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    if data.get("isAnonymous") or not data.get("accessToken"):
        raise ValueError("sp_dc cookie is invalid or expired — please copy a fresh value")
    exp_ms = data.get("accessTokenExpirationTimestampMs") or 0
    expires_at = (int(exp_ms) / 1000 - 30) if exp_ms else (time.time() + 3570)
    return {
        "access_token": data["accessToken"],
        "expires_at": expires_at,
    }


def _oauth_close_page(success: bool, message: str) -> str:
    """Return an HTML page that closes itself and posts a message to the opener."""
    status = "success" if success else "error"
    return (
        f'<!DOCTYPE html><html><head><title>Spotify Auth</title></head><body><script>\n'
        f'  if(window.opener){{\n'
        f'    window.opener.postMessage({{type:"spotify_oauth",status:"{status}",'
        f'message:{json.dumps(message)}}},"*");\n'
        f'    setTimeout(()=>window.close(),300);\n'
        f'  }}else{{\n'
        f'    document.body.innerHTML="<p>{message}</p>";\n'
        f'  }}\n'
        f'</script><p>{message}</p></body></html>'
    )


def _load_history() -> list:
    try:
        if not _HISTORY_PATH.exists():
            return []
        raw = _HISTORY_PATH.read_text(encoding="utf-8")
        data = json.loads(raw)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _save_history_entry(job: dict) -> None:
    """Append a slim copy of a finished job to the on-disk history."""
    slim = {
        "id":               job.get("id", ""),
        "type":             job.get("type", ""),
        "artist":           job.get("artist", ""),
        "title":            job.get("title", ""),
        "status":           job.get("status", ""),
        "submitted_by":     job.get("submitted_by", ""),
        "created_at":       job.get("created_at"),
        "started_at":       job.get("started_at"),
        "finished_at":      job.get("finished_at"),
        "total_tracks":     int(job.get("total_tracks") or 0),
        "completed_tracks": int(job.get("completed_tracks") or 0),
        "failed_tracks":    int(job.get("failed_tracks") or 0),
        "failed_tracks_list": list(job.get("failed_tracks_list") or []),
        "skipped":          int(job.get("skipped") or 0),
    }
    with _history_lock:
        history = _load_history()
        # Avoid duplicate entries if the worker somehow calls this twice.
        if not any(e.get("id") == slim["id"] for e in history):
            history.insert(0, slim)
            if len(history) > _HISTORY_MAX_ENTRIES:
                history = history[:_HISTORY_MAX_ENTRIES]
        _HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = _HISTORY_PATH.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(history, indent=2, default=str), encoding="utf-8")
        tmp.replace(_HISTORY_PATH)


def _plex_user_key() -> str:
    """Stable identifier for the currently authenticated Plex user."""
    try:
        u = session.get("plex_user") or {}
        uid = str(u.get("id") or "").strip()
        if uid:
            return uid
        name = (u.get("username") or u.get("title") or "").strip().lower()
        return name
    except Exception:
        return ""


def _get_linked_lastfm_username() -> str:
    key = _plex_user_key()
    if not key:
        return ""

    with _user_store_lock:
        store = _load_user_store()
        user = store.get(key) or {}
        if not isinstance(user, dict):
            return ""
        lf = user.get("lastfm") or {}
        if not isinstance(lf, dict):
            return ""
        return (lf.get("username") or "").strip()


def _set_linked_lastfm(username: str) -> None:
    key = _plex_user_key()
    if not key:
        raise RuntimeError("Not authenticated")

    username = (username or "").strip()
    if not username:
        raise ValueError("Missing Last.fm username")

    with _user_store_lock:
        store = _load_user_store()
        user = store.get(key)
        if not isinstance(user, dict):
            user = {}

        try:
            pu = session.get("plex_user") or {}
            if isinstance(pu, dict):
                user["plex"] = {
                    "id": pu.get("id"),
                    "username": pu.get("username"),
                    "title": pu.get("title"),
                }
        except Exception:
            pass

        lf = user.get("lastfm")
        if not isinstance(lf, dict):
            lf = {}
        lf["username"] = username
        user["lastfm"] = lf
        store[key] = user
        _save_user_store(store)


def _unlink_lastfm() -> None:
    key = _plex_user_key()
    if not key:
        raise RuntimeError("Not authenticated")
    with _user_store_lock:
        store = _load_user_store()
        user = store.get(key)
        if isinstance(user, dict) and isinstance(user.get("lastfm"), dict):
            user.pop("lastfm", None)
            # Auto discovery requires Last.fm; disable it if user unlinks.
            try:
                ad = user.get("auto_discovery")
                if isinstance(ad, dict):
                    ad["enabled"] = False
                    user["auto_discovery"] = ad
            except Exception:
                pass
            store[key] = user
            _save_user_store(store)


def _plex_headers() -> dict[str, str]:
    return {
        "Accept": "application/json",
        "X-Plex-Client-Identifier": PLEX_OAUTH_CLIENT_ID,
        "X-Plex-Product": "SoundScout",
        "X-Plex-Version": "1.0",
        "X-Plex-Device": "Web",
        "X-Plex-Device-Name": "SoundScout Web UI",
        "X-Plex-Platform": "Web",
    }


def _public_base_url() -> str:
    """Best-effort public base URL for OAuth callbacks.

    Priority:
    1) WEBUI_PUBLIC_URL (explicit)
    2) X-Forwarded-Proto / X-Forwarded-Host (reverse proxy)
    3) request.host_url

    Always returns a string ending with '/'.
    """

    env_url = (os.environ.get("WEBUI_PUBLIC_URL") or "").strip()
    if env_url:
        return env_url if env_url.endswith("/") else (env_url + "/")

    # Reverse-proxy headers (may be comma-separated lists)
    xf_proto = (request.headers.get("X-Forwarded-Proto") or "").split(",")[0].strip()
    xf_host = (request.headers.get("X-Forwarded-Host") or "").split(",")[0].strip()

    if xf_proto and xf_host:
        base = f"{xf_proto}://{xf_host}"
        return base if base.endswith("/") else (base + "/")

    base = (request.host_url or "").strip()
    if not base:
        return ""
    return base if base.endswith("/") else (base + "/")


_discovery_state: dict[str, object] = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "last_ok": None,
    "last_error": None,
}
_discovery_lock = threading.Lock()


def _session_username_lower() -> str:
    try:
        u = session.get("plex_user") or {}
        username = (u.get("username") or u.get("title") or "").strip().lower()
        return username
    except Exception:
        return ""




def _get_current_user_run_config() -> tuple[str, str, str]:
    """Return (lastfm_username, plex_token, plex_baseurl) for the current session user."""

    plex_token = (session.get("plex_token") or "").strip()
    plex_baseurl = (session.get("plex_baseurl") or "").strip()
    if not plex_baseurl:
        # Fall back to persisted per-user base URL (so we don't regress to PLEX_BASEURL after session changes).
        key = _plex_user_key()
        if key:
            with _user_store_lock:
                store = _load_user_store()
            rec = store.get(key) if isinstance(store, dict) else None
            if isinstance(rec, dict):
                plex = rec.get("plex")
                if isinstance(plex, dict):
                    plex_baseurl = (plex.get("baseurl") or "").strip()

    plex_baseurl = plex_baseurl or PLEX_BASEURL
    lastfm_username = (_get_linked_lastfm_username() or "").strip()
    return lastfm_username, plex_token, plex_baseurl




def _can_run_discovery() -> tuple[bool, str]:
    if not WEBUI_REQUIRE_PLEX_LOGIN:
        return False, "Plex login is disabled"
    if not session.get("plex_token"):
        return False, "Not authenticated"
    lastfm_username, plex_token, _baseurl = _get_current_user_run_config()
    if not plex_token:
        return False, "Not authenticated"
    if not lastfm_username:
        return False, "Last.fm is not linked"
    return True, ""


def _run_discovery_background(users: list[tuple[str, str, str]]) -> None:
    with _discovery_lock:
        _discovery_state["running"] = True
        _discovery_state["started_at"] = time.time()
        _discovery_state["finished_at"] = None
        _discovery_state["last_ok"] = None
        _discovery_state["last_error"] = None

    try:
        s = load_settings()
        setup_logging(s.log_level)

        if not users:
            raise RuntimeError("No WebUI-linked users found (Last.fm + Plex token)")

        # Deterministic order
        for lastfm_username, plex_token, plex_baseurl in sorted(users, key=lambda t: (t[0].lower(), t[1])):
            run_full_pipeline(lastfm_username=lastfm_username, plex_token=plex_token, plex_baseurl=plex_baseurl or None)
        with _discovery_lock:
            _discovery_state["last_ok"] = True
    except Exception as e:
        logger.error("Manual discovery run failed: %s", e)
        with _discovery_lock:
            _discovery_state["last_ok"] = False
            _discovery_state["last_error"] = str(e)
    finally:
        with _discovery_lock:
            _discovery_state["running"] = False
            _discovery_state["finished_at"] = time.time()


@app.route("/api/discovery/status", methods=["GET"])
def discovery_status():
    ok, reason = _can_run_discovery()
    if not ok:
        return jsonify({"error": reason}), 403
    with _discovery_lock:
        return jsonify(dict(_discovery_state))


@app.route("/api/discovery/run", methods=["POST"])
def discovery_run():
    ok, reason = _can_run_discovery()
    if not ok:
        return jsonify({"error": reason}), 403

    lastfm_username, plex_token, plex_baseurl = _get_current_user_run_config()
    machine_id = _plex_server_machine_identifier()
    if machine_id:
        working_baseurl, last_status = _plex_find_working_baseurl(plex_token, machine_id)
        if working_baseurl:
            session["plex_baseurl"] = working_baseurl
            _persist_current_plex_auth_to_store()
            plex_baseurl = working_baseurl
        else:
            logger.warning(
                "Plex base URL probe failed for current user (status=%s); proceeding with plex_baseurl=%s",
                last_status,
                plex_baseurl,
            )
    users = [(lastfm_username, plex_token, plex_baseurl)]

    with _discovery_lock:
        if _discovery_state.get("running"):
            return jsonify({"error": "already_running"}), 409

        t = threading.Thread(target=_run_discovery_background, args=(users,), daemon=True)
        t.start()

    return jsonify({"success": True, "status": "started"})


def _plex_create_pin() -> dict:
    resp = requests.post(
        "https://plex.tv/api/v2/pins",
        params={"strong": "true"},
        headers=_plex_headers(),
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict) or not data.get("id") or not data.get("code"):
        raise RuntimeError("Unexpected Plex PIN response")
    return data


def _plex_poll_pin(pin_id: str) -> dict:
    # Primary: Plex v2 PIN poll (JSON)
    resp = requests.get(
        f"https://plex.tv/api/v2/pins/{pin_id}",
        headers=_plex_headers(),
        timeout=15,
    )

    if resp.status_code == 404:
        # Plex may return 404 while the PIN is not yet authorized.
        # Try legacy XML as well, and if still 404 treat as "pending".
        legacy = requests.get(
            f"https://plex.tv/pins/{pin_id}.xml",
            headers=_plex_headers(),
            timeout=15,
        )
        if legacy.status_code == 404:
            return {"id": str(pin_id), "authToken": ""}
        legacy.raise_for_status()

        try:
            root = ET.fromstring(legacy.text or "")
            attrs = getattr(root, "attrib", {}) or {}
            auth_token = (attrs.get("auth_token") or attrs.get("authToken") or "").strip()
            code = (attrs.get("code") or "").strip()
            pid = (attrs.get("id") or pin_id or "").strip()
            return {"id": pid, "code": code, "authToken": auth_token}
        except Exception:
            raise RuntimeError("Unexpected Plex PIN poll response")

    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError("Unexpected Plex PIN poll response")
    return data


def _plex_get_user(token: str) -> dict:
    resp = requests.get(
        "https://plex.tv/api/v2/user",
        headers={**_plex_headers(), "X-Plex-Token": token},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError("Unexpected Plex user response")
    return {
        "id": data.get("id"),
        "username": data.get("username") or data.get("title") or "",
        "title": data.get("title") or "",
        "email": data.get("email") or "",
    }


def _plex_server_machine_identifier() -> str:
    """Return the Plex server machineIdentifier for the configured PLEX_BASEURL.

    This endpoint does not require authentication, so it can help diagnose cases
    where a user token is rejected vs. the base URL pointing at the wrong server.
    """

    if not PLEX_BASEURL:
        return ""

    try:
        resp = requests.get(
            f"{PLEX_BASEURL}/identity",
            headers={"X-Plex-Client-Identifier": PLEX_OAUTH_CLIENT_ID},
            timeout=10,
            verify=PLEX_VERIFY_SSL,
        )
        if resp.status_code != 200:
            return ""
        root = ET.fromstring(resp.text or "")
        if not hasattr(root, "attrib"):
            return ""
        return (root.attrib.get("machineIdentifier") or "").strip()
    except Exception:
        return ""


def _plex_user_has_resource_access(user_token: str, machine_identifier: str) -> bool:
    """Check if a Plex account token can see a specific server resource on plex.tv."""

    machine_identifier = (machine_identifier or "").strip()
    user_token = (user_token or "").strip()
    if not machine_identifier or not user_token:
        return False

    def _check_xml() -> bool:
        try:
            resp = requests.get(
                "https://plex.tv/api/resources",
                params={"includeHttps": "1", "includeRelay": "1"},
                headers={
                    **_plex_headers(),
                    "X-Plex-Token": user_token,
                    "Accept": "application/xml",
                },
                timeout=15,
            )
            resp.raise_for_status()
            root = ET.fromstring(resp.text or "")
            for dev in root.findall(".//Device"):
                mid = (
                    (dev.attrib.get("clientIdentifier") or "")
                    or (dev.attrib.get("machineIdentifier") or "")
                ).strip()
                if mid and mid == machine_identifier:
                    return True
            return False
        except Exception:
            return False

    # Prefer v2 JSON, but fall back to the older XML endpoint (used by Overseerr).
    try:
        resp = requests.get(
            "https://plex.tv/api/v2/resources",
            params={"includeHttps": "1", "includeRelay": "1"},
            headers={
                **_plex_headers(),
                "X-Plex-Token": user_token,
                "Accept": "application/json",
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    continue
                client_id = (item.get("clientIdentifier") or item.get("machineIdentifier") or "").strip()
                if client_id and client_id == machine_identifier:
                    return True
            return False
    except Exception:
        pass

    return _check_xml()


def _plex_resource_connection_uris(user_token: str, machine_identifier: str) -> list[tuple[str, str]]:
    """Return candidate base URLs for the given server from plex.tv resources.

    Returns a list of (baseurl, access_token) pairs.

    Note: For shared-library users, plex.tv may return a server-scoped accessToken
    on the resource which is accepted by PMS even when the account token is not.
    """

    machine_identifier = (machine_identifier or "").strip()
    user_token = (user_token or "").strip()
    if not machine_identifier or not user_token:
        return []

    def _dedupe_and_sort(uris: list[tuple[int, int, str, str]]) -> list[tuple[str, str]]:
        uris_sorted = sorted(uris, key=lambda t: (-t[0], -t[1], t[2]))
        deduped: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for _local, _https, uri, access_token in uris_sorted:
            key = (uri, access_token)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(key)
        return deduped

    uris: list[tuple[int, int, str, str]] = []

    # 1) v2 JSON resources
    try:
        resp = requests.get(
            "https://plex.tv/api/v2/resources",
            params={"includeHttps": "1", "includeRelay": "1"},
            headers={
                **_plex_headers(),
                "X-Plex-Token": user_token,
                "Accept": "application/json",
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    continue
                client_id = (item.get("clientIdentifier") or item.get("machineIdentifier") or "").strip()
                if not client_id or client_id != machine_identifier:
                    continue

                access_token = (item.get("accessToken") or "").strip()
                conns = item.get("connections")
                if not isinstance(conns, list):
                    continue
                for c in conns:
                    if not isinstance(c, dict):
                        continue
                    uri = (c.get("uri") or "").strip().rstrip("/")
                    if not uri.startswith("http"):
                        continue
                    is_local = 1 if c.get("local") else 0
                    is_https = 1 if uri.lower().startswith("https://") else 0
                    uris.append((is_local, is_https, uri, access_token))
    except Exception:
        pass

    # 2) XML resources fallback (used by Overseerr)
    try:
        resp = requests.get(
            "https://plex.tv/api/resources",
            params={"includeHttps": "1", "includeRelay": "1"},
            headers={
                **_plex_headers(),
                "X-Plex-Token": user_token,
                "Accept": "application/xml",
            },
            timeout=15,
        )
        resp.raise_for_status()
        root = ET.fromstring(resp.text or "")

        for dev in root.findall(".//Device"):
            mid = (
                (dev.attrib.get("clientIdentifier") or "")
                or (dev.attrib.get("machineIdentifier") or "")
            ).strip()
            if not mid or mid != machine_identifier:
                continue

            access_token = (dev.attrib.get("accessToken") or dev.attrib.get("access_token") or "").strip()

            for conn in dev.findall(".//Connection"):
                uri = (conn.attrib.get("uri") or "").strip().rstrip("/")
                if not uri.startswith("http"):
                    continue
                local_attr = (conn.attrib.get("local") or "").strip()
                is_local = 1 if local_attr in {"1", "true", "True"} else 0
                is_https = 1 if uri.lower().startswith("https://") else 0
                uris.append((is_local, is_https, uri, access_token))
    except Exception:
        pass

    return _dedupe_and_sort(uris)


def _plex_tv_admin_shared_users_xml(admin_token: str) -> str:
    admin_token = (admin_token or "").strip()
    if not admin_token:
        return ""
    resp = requests.get(
        "https://plex.tv/api/users",
        headers={
            **_plex_headers(),
            "X-Plex-Token": admin_token,
            # plex.tv returns XML here
            "Accept": "application/xml",
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.text or ""


def _plex_admin_shared_list_allows_user(plex_user_id: str, server_machine_id: str) -> tuple[bool, str]:
    """Return True if the admin Plex account shares the configured server with this user.

    Mirrors Overseerr's approach: use the admin token to fetch plex.tv's shared user list
    and check whether the target user has a <Server machineIdentifier=...> entry.
    """

    plex_user_id = (plex_user_id or "").strip()
    server_machine_id = (server_machine_id or "").strip()
    if not plex_user_id:
        return False, "Missing Plex user id"
    if not server_machine_id:
        return False, "Could not determine Plex server machineIdentifier"

    admin_token = ""
    if not admin_token:
        return False, "No admin token configured"

    try:
        admin_user = _plex_get_user(admin_token)
        if str(admin_user.get("id") or "").strip() == plex_user_id:
            return True, ""
    except Exception:
        # If the admin token is invalid, fall through with a clear message
        return False, "Stored admin Plex token is invalid"

    try:
        xml_text = _plex_tv_admin_shared_users_xml(admin_token)
        root = ET.fromstring(xml_text)

        # Structure: <MediaContainer> <User id="..."> <Server machineIdentifier="..."/> ... </User> ...</MediaContainer>
        for user_el in root.findall(".//User"):
            uid = (user_el.attrib.get("id") or "").strip()
            if uid != plex_user_id:
                continue

            for server_el in user_el.findall(".//Server"):
                mid = (server_el.attrib.get("machineIdentifier") or "").strip()
                if mid and mid == server_machine_id:
                    return True, ""

            return False, "This Plex user is not shared on the configured server"

        return False, "This Plex user is not in the admin account's shared user list"
    except Exception:
        return False, "Could not validate user access via plex.tv shared user list"


def _plex_probe_library_sections(baseurl: str, user_token: str) -> tuple[bool, int | None]:
    baseurl = (baseurl or "").strip().rstrip("/")
    user_token = (user_token or "").strip()
    if not baseurl or not user_token:
        return False, None

    try:
        resp = requests.get(
            f"{baseurl}/library/sections",
            headers={
                **_plex_headers(),
                "X-Plex-Token": user_token,
            },
            timeout=5,
            verify=PLEX_VERIFY_SSL,
        )
        if resp.status_code == 200:
            return True, 200
        if resp.status_code in {401, 403}:
            # Some proxies strip auth headers; query param is also supported by Plex.
            resp2 = requests.get(
                f"{baseurl}/library/sections",
                params={"X-Plex-Token": user_token},
                headers={
                    **_plex_headers(),
                },
                timeout=5,
                verify=PLEX_VERIFY_SSL,
            )
            if resp2.status_code == 200:
                return True, 200
            return False, int(resp2.status_code)

        return False, int(resp.status_code)
    except Exception:
        return False, None


def _plex_find_working_baseurl(user_token: str, server_machine_id: str) -> tuple[str, int | None]:
    """Try to find a base URL that successfully serves /library/sections for this user token.

    Returns (baseurl, last_status). baseurl is empty if none worked.
    """

    user_token = (user_token or "").strip()
    server_machine_id = (server_machine_id or "").strip()
    if not user_token or not PLEX_BASEURL or not server_machine_id:
        return "", None

    candidates: list[str] = []
    seen: set[str] = set()

    candidates_with_tokens: list[tuple[str, str]] = [(PLEX_BASEURL, "")]
    candidates_with_tokens.extend(_plex_resource_connection_uris(user_token, server_machine_id))

    for c, _access_token in candidates_with_tokens:
        c = (c or "").strip().rstrip("/")
        if not c or c in seen:
            continue
        seen.add(c)
        candidates.append(c)

    last_status: int | None = None
    # Don't let probing block the UI for too long.
    # Don't let probing block the UI for too long.
    # Prefer probing with a server-scoped accessToken when available.
    for baseurl in candidates[:6]:
        access_token = ""
        for u, t in candidates_with_tokens:
            if (u or "").strip().rstrip("/") == baseurl:
                access_token = (t or "").strip()
                break

        ok, status = _plex_probe_library_sections(baseurl, access_token or user_token)
        if status is not None:
            last_status = status
        if ok:
            return baseurl, status

    return "", last_status


def _looks_like_plex_server_response(resp: requests.Response | None) -> bool:
    if resp is None:
        return False
    try:
        headers = {k.lower(): str(v) for k, v in (resp.headers or {}).items()}
        if any(k.startswith("x-plex-") for k in headers.keys()):
            return True
        server_hdr = headers.get("server", "")
        if "plex" in server_hdr.lower():
            return True
        ctype = headers.get("content-type", "")
        if "xml" in ctype.lower():
            return True
        return False
    except Exception:
        return False


def _response_hint(resp: requests.Response | None) -> dict:
    """Return a small, safe diagnostic hint about an HTTP response."""

    if resp is None:
        return {}
    try:
        server_hdr = (resp.headers.get("Server") or "").strip()
        ctype = (resp.headers.get("Content-Type") or "").strip()
        return {
            "status": int(getattr(resp, "status_code", 0) or 0),
            "server": server_hdr[:80],
            "content_type": ctype[:120],
            "looks_like_plex": bool(_looks_like_plex_server_response(resp)),
        }
    except Exception:
        return {}


def _plex_user_has_server_access(user_token: str) -> tuple[bool, str, int | None, str]:
    if not PLEX_BASEURL:
        return False, "PLEX_BASEURL is not configured", None, ""

    # If the user doesn't have access to the server, this will return 401/403.
    # Some reverse proxies strip unknown headers, so we retry with X-Plex-Token
    # as a query param (supported by Plex) before concluding access is denied.
    try:
        resp = requests.get(
            f"{PLEX_BASEURL}/library/sections",
            headers={
                **_plex_headers(),
                "X-Plex-Token": user_token,
            },
            timeout=15,
            verify=PLEX_VERIFY_SSL,
        )
        if resp.status_code == 200:
            return True, "", 200, PLEX_BASEURL
        if resp.status_code in {401, 403}:
            hint1 = _response_hint(resp)
            try:
                resp2 = requests.get(
                    f"{PLEX_BASEURL}/library/sections",
                    params={"X-Plex-Token": user_token},
                    headers={
                        **_plex_headers(),
                    },
                    timeout=15,
                    verify=PLEX_VERIFY_SSL,
                )
                if resp2.status_code == 200:
                    return True, "", 200, PLEX_BASEURL
                hint2 = _response_hint(resp2)
            except Exception:
                hint2 = {}
                pass

            machine_id = _plex_server_machine_identifier()
            if machine_id:
                if not _plex_user_has_resource_access(user_token, machine_id):
                    return (
                        False,
                        "This Plex account token cannot see the configured server (PLEX_BASEURL) on plex.tv. "
                        "Double-check the server share/invitation for this account, and ensure PLEX_BASEURL points "
                        "to the same Plex server you shared libraries from.",
                        resp.status_code,
                        "",
                    )

                # The account can see the server resource, but the configured base URL rejected the token.
                # Try alternative connection URIs from plex.tv (e.g., https/plex.direct).
                for uri, access_token in _plex_resource_connection_uris(user_token, machine_id):
                    if not uri or uri.rstrip("/") == PLEX_BASEURL.rstrip("/"):
                        continue
                    try:
                        eff_token = (access_token or user_token).strip()
                        r = requests.get(
                            f"{uri}/library/sections",
                            headers={
                                **_plex_headers(),
                                "X-Plex-Token": eff_token,
                            },
                            timeout=15,
                            verify=PLEX_VERIFY_SSL,
                        )
                        if r.status_code == 200:
                            logger.info("Using alternate Plex base URL for this user: %s", uri)
                            return True, "", 200, uri
                        if r.status_code in {401, 403}:
                            r2 = requests.get(
                                f"{uri}/library/sections",
                                params={"X-Plex-Token": eff_token},
                                headers={
                                    **_plex_headers(),
                                },
                                timeout=15,
                                verify=PLEX_VERIFY_SSL,
                            )
                            if r2.status_code == 200:
                                logger.info("Using alternate Plex base URL for this user: %s", uri)
                                return True, "", 200, uri
                    except Exception:
                        continue

                if not (hint1.get("looks_like_plex") or hint2.get("looks_like_plex")):
                    return (
                        False,
                        "The configured PLEX_BASEURL responded 401/403 but the response does not look like Plex (likely a reverse proxy/WAF). "
                        "Point PLEX_BASEURL directly at Plex Media Server (LAN IP:32400) or configure your proxy to pass Plex through without auth.",
                        resp.status_code,
                        "",
                    )

                return (
                    False,
                    "The Plex server rejected this token (401/403) even though the account can see the server resource. "
                    "This usually means PLEX_BASEURL is not pointing directly at Plex (e.g., reverse-proxy auth), or HTTPS/SSL settings are mismatched.",
                    resp.status_code,
                    "",
                )
            return False, "This Plex user does not have access to your Plex server", resp.status_code, ""
        return False, f"Plex server check failed ({resp.status_code})", resp.status_code, ""
    except Exception as e:
        return False, f"Could not contact Plex server: {e}", None, ""


def _persist_current_plex_auth_to_store() -> None:
    """Persist the current Plex user identity + token for scheduled runs.

    This enables scheduled discovery to run as each Plex user (so playlists are created
    inside that user's Plex account).
    """

    try:
        token = (session.get("plex_token") or "").strip()
        baseurl = (session.get("plex_baseurl") or "").strip()
        user = session.get("plex_user") or {}
        if not token or not isinstance(user, dict):
            return

        key = _plex_user_key()
        if not key:
            return

        with _user_store_lock:
            store = _load_user_store()
            rec = store.get(key)
            if not isinstance(rec, dict):
                rec = {}

            if not baseurl:
                plex_existing = rec.get("plex")
                if isinstance(plex_existing, dict):
                    baseurl = (plex_existing.get("baseurl") or "").strip()
            baseurl = baseurl or PLEX_BASEURL

            rec["plex"] = {
                "id": user.get("id"),
                "username": user.get("username"),
                "title": user.get("title"),
                "token": token,
                "baseurl": baseurl,
                "token_saved_at": int(time.time()),
            }
            store[key] = rec
            _save_user_store(store)
    except Exception:
        return


@app.before_request
def _enforce_webui_auth():
    if not WEBUI_REQUIRE_PLEX_LOGIN:
        return None

    path = request.path or ""
    if not path.startswith("/api/"):
        return None
    if path.startswith("/api/auth/"):
        return None
    if path == "/api/health":
        return None

    token = session.get("plex_token")
    if token:
        return None
    return jsonify({"error": "auth_required"}), 401


@app.route("/api/auth/status", methods=["GET"])
def auth_status():
    can_run, _ = _can_run_discovery()
    return jsonify(
        {
            "require_login": WEBUI_REQUIRE_PLEX_LOGIN,
            "authed": bool(session.get("plex_token")),
            "user": session.get("plex_user") or None,
            "can_run_discovery": bool(can_run),
        }
    )


@app.route("/api/auth/start", methods=["POST"])
def auth_start():
    if not WEBUI_REQUIRE_PLEX_LOGIN:
        return jsonify({"error": "login_not_required"}), 400

    try:
        pin = _plex_create_pin()
        pin_id = str(pin.get("id"))
        code = str(pin.get("code"))
        base_url = (os.environ.get("WEBUI_PUBLIC_URL") or request.host_url or "").strip()
        if base_url and not base_url.endswith("/"):
            base_url += "/"
        # Redirect the Plex login window to a lightweight callback page that can close itself.
        forward_url = f"{base_url}auth/callback" if base_url else ""

        auth_url = (
            "https://app.plex.tv/auth#?"
            f"clientID={requests.utils.quote(PLEX_OAUTH_CLIENT_ID)}"
            f"&code={requests.utils.quote(code)}"
            f"&forwardUrl={requests.utils.quote(forward_url)}"
        )
        return jsonify({"pin_id": pin_id, "auth_url": auth_url})
    except Exception as e:
        logger.error("Failed to start Plex auth: %s", e)
        return jsonify({"error": "failed_to_start_auth"}), 500


@app.route("/api/auth/poll/<pin_id>", methods=["GET"])
def auth_poll(pin_id: str):
    if not WEBUI_REQUIRE_PLEX_LOGIN:
        return jsonify({"status": "not_required"})

    pin_id = (pin_id or "").strip()
    if not pin_id:
        return jsonify({"error": "missing_pin_id"}), 400

    stage = "poll_pin"
    try:
        pin = _plex_poll_pin(pin_id)
        token = (pin.get("authToken") or "").strip()
        if not token:
            return jsonify({"status": "pending"})

        stage = "get_user"
        user = _plex_get_user(token)

        stage = "server_access"
        machine_id = _plex_server_machine_identifier()
        if not machine_id:
            session.pop("plex_token", None)
            session.pop("plex_user", None)
            return (
                jsonify(
                    {
                        "status": "denied",
                        "reason": "Could not determine Plex server identity. Check PLEX_BASEURL (must point to your Plex Media Server).",
                        "user": user,
                        "server_status": None,
                        "server_hint": {},
                    }
                ),
                403,
            )

        user_id = str(user.get("id") or "").strip()

        # Base URL selection: pick the best plex.tv connection URI for speed;
        # let discovery_run do deeper probing later if needed.
        working_baseurl = ""
        last_status: int | None = None
        try:
            conns = _plex_resource_connection_uris(token, machine_id)
            if conns:
                working_baseurl = (conns[0][0] or "").strip().rstrip("/")
        except Exception:
            working_baseurl = ""

        # Access decision: mirror Overseerr — validate via the admin account's
        # shared-users list (plex.tv XML). If that isn't possible, fall back to
        # whether the user's token can see the server resource on plex.tv.
        allowed = False
        reason = ""
        ok_shared, shared_reason = _plex_admin_shared_list_allows_user(user_id, machine_id)
        if ok_shared:
            allowed = True
        else:
            if _plex_user_has_resource_access(token, machine_id):
                allowed = True
            else:
                allowed = False
                reason = shared_reason or "This Plex user does not have access to your Plex server"

        if not allowed:
            logger.warning(
                "Plex auth denied for user=%s (id=%s, email=%s), last_status=%s",
                user.get("username") or user.get("title") or "",
                user.get("id") or "",
                user.get("email") or "",
                last_status,
            )
            session.pop("plex_token", None)
            session.pop("plex_user", None)
            # Provide a small hint so users can tell if they're hitting Plex or a proxy.
            try:
                probe = requests.get(
                    f"{PLEX_BASEURL}/library/sections",
                    headers={
                        **_plex_headers(),
                    },
                    timeout=10,
                    verify=PLEX_VERIFY_SSL,
                )
                server_hint = _response_hint(probe)
            except Exception:
                server_hint = {}

            return (
                jsonify(
                    {
                        "status": "denied",
                        "reason": reason,
                        "user": user,
                        "server_status": last_status,
                        "server_hint": server_hint,
                    }
                ),
                403,
            )

        stage = "persist_session"
        session["plex_token"] = token
        session["plex_user"] = user
        if working_baseurl:
            session["plex_baseurl"] = working_baseurl

        stage = "persist_store"
        _persist_current_plex_auth_to_store()
        return jsonify({"status": "authed", "user": user})
    except Exception as e:
        # Avoid leaking tokens; return only a safe diagnostic.
        # Suppress full traceback for routine network errors (DNS failures, timeouts).
        import requests as _req_mod
        if isinstance(e, (_req_mod.exceptions.ConnectionError, _req_mod.exceptions.Timeout)):
            logger.warning("Failed to poll Plex auth (stage=%s): %s", stage, e)
        else:
            logger.exception("Failed to poll Plex auth (stage=%s): %s", stage, e)

        detail = str(e)
        if len(detail) > 500:
            detail = detail[:500] + "…"
        return (
            jsonify(
                {
                    "status": "error",
                    "reason": "failed_to_poll",
                    "stage": stage,
                    "detail": detail,
                }
            ),
            500,
        )


@app.route("/api/auth/logout", methods=["POST"])
def auth_logout():
    session.pop("plex_token", None)
    session.pop("plex_user", None)
    return jsonify({"success": True})


@app.route("/auth/callback", methods=["GET"])
def auth_callback():
        """Landing page used as Plex OAuth forwardUrl."""
        return Response(
                """<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
    <meta name="theme-color" content="#00060E">
    <title>Authentication Complete</title>

    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600&family=Rajdhani:wght@600;700&display=swap" rel="stylesheet">

    <style>
        :root {
            --bg: #00060E;
            --panel: #070B12;
            --border: rgba(2,215,242,0.35);
            --accent: #FFD300;
            --text: #EAF2FF;
            --muted: #7F8CA3;
            --glow: 0 0 24px rgba(255,211,0,0.45);
        }

        * { box-sizing: border-box; }

        body {
            margin: 0;
            min-height: 100vh;
            background:
                radial-gradient(800px 400px at 50% -20%, rgba(255,211,0,0.12), transparent 60%),
                var(--bg);
            color: var(--text);
            font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 24px;
        }

        .panel {
            width: 100%;
            max-width: 520px;
            background: linear-gradient(180deg, rgba(7,11,18,0.95), rgba(0,6,14,0.95));
            border: 1px solid var(--border);
            border-radius: 10px;
            padding: 28px 30px;
            box-shadow: 0 30px 80px rgba(0,0,0,0.7);
            position: relative;
        }

        .panel::before {
            content: "";
            position: absolute;
            inset: 0;
            border-radius: inherit;
            box-shadow: inset 0 1px 0 rgba(2,215,242,0.25);
            pointer-events: none;
        }

        h1 {
            font-family: Rajdhani, Inter, sans-serif;
            font-weight: 700;
            font-size: 1.6rem;
            margin: 0 0 6px;
            letter-spacing: 0.06em;
            text-transform: uppercase;
            color: var(--accent);
        }

        .subtitle {
            color: var(--muted);
            font-size: 0.9rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            margin-bottom: 22px;
        }

        .status {
            margin-top: 14px;
            padding: 14px 16px;
            border: 1px solid rgba(255,211,0,0.4);
            background: rgba(255,211,0,0.08);
            border-radius: 6px;
            font-family: Rajdhani, Inter, sans-serif;
            font-weight: 600;
            letter-spacing: 0.04em;
            box-shadow: var(--glow);
        }

        .muted {
            margin-top: 14px;
            color: var(--muted);
            font-size: 0.9rem;
        }
    </style>
</head>

<body>
    <div class="panel">
        <h1>Access Granted</h1>
        <div class="subtitle">SoundScout Authentication</div>

        <div class="status">
            ✔ Plex login successful
        </div>

        <p class="muted">
            This window will close automatically.
        </p>
    </div>

    <script>
        (function () {
            try {
                if (window.opener && !window.opener.closed) {
                    window.opener.postMessage({ type: 'plex-auth-complete' }, '*');
                    setTimeout(() => window.close(), 300);
                    return;
                }
            } catch (e) {}
            try { window.location.href = '/'; } catch (e) {}
        })();
    </script>
</body>
</html>""",
                content_type="text/html; charset=utf-8",
        )


@app.route("/api/lastfm/status", methods=["GET"])
def lastfm_status():
    """Return Last.fm linkage status for the current Plex user."""
    if not session.get("plex_token"):
        return jsonify({"error": "auth_required"}), 401

    username = _get_linked_lastfm_username()
    return jsonify({"linked": bool(username), "username": username or None})


@app.route("/api/lastfm/link", methods=["POST"])
def lastfm_link_username():
    """Link Last.fm by username. Validates the username against Last.fm's API."""
    if not session.get("plex_token"):
        return jsonify({"error": "auth_required"}), 401

    data = request.json or {}
    username = (data.get("username") or "").strip()
    if not username:
        return jsonify({"error": "missing_username"}), 400

    # Best-effort validation: ensure user exists.
    try:
        resp = requests.get(
            "https://ws.audioscrobbler.com/2.0/",
            params={"method": "user.getInfo", "user": username, "api_key": LASTFM_API_KEY, "format": "json"},
            timeout=15,
        )
        payload = resp.json() if resp.content else {}
        if not isinstance(payload, dict) or not payload.get("user"):
            return jsonify({"error": "invalid_username"}), 400
    except Exception:
        # If validation fails due to networking, still allow linking.
        pass

    try:
        _set_linked_lastfm(username)
        return jsonify({"success": True, "username": username})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/lastfm/unlink", methods=["POST"])
def lastfm_unlink():
    if not session.get("plex_token"):
        return jsonify({"error": "auth_required"}), 401

    try:
        _unlink_lastfm()
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/autodiscovery/settings", methods=["GET"])
def autodiscovery_get_settings():
    """Return per-user auto discovery settings for the WebUI."""
    if not session.get("plex_token"):
        return jsonify({"error": "auth_required"}), 401

    key = _plex_user_key()
    if not key:
        return jsonify({"error": "auth_required"}), 401

    linked = bool(_get_linked_lastfm_username())
    s = _get_autodiscovery_settings_for_key(key)
    default_dow, default_time = _parse_default_autodiscovery_from_cron()
    tz = (os.environ.get("TZ") or "UTC").strip() or "UTC"

    return jsonify(
        {
            "linked_lastfm": linked,
            "enabled": bool(s.get("enabled")),
            "weekday": int(s.get("weekday", default_dow)),
            "time": str(s.get("time", default_time)),
            "default_weekday": int(default_dow),
            "default_time": str(default_time),
            "tz": tz,
        }
    )


@app.route("/api/autodiscovery/settings", methods=["POST"])
def autodiscovery_set_settings():
    """Update per-user auto discovery opt-in + schedule."""
    if not session.get("plex_token"):
        return jsonify({"error": "auth_required"}), 401

    data = request.json if request.is_json else {}
    if not isinstance(data, dict):
        data = {}

    enabled = bool(data.get("enabled"))
    weekday = data.get("weekday", 0)
    time_str = data.get("time", "")

    try:
        if enabled and not _get_linked_lastfm_username():
            return (
                jsonify({"error": "lastfm_required", "message": "Link Last.fm before enabling auto discovery."}),
                400,
            )
        _set_autodiscovery_settings(enabled=enabled, weekday=int(weekday), time_str=str(time_str))
    except ValueError as e:
        return jsonify({"error": "invalid", "message": str(e)}), 400
    except Exception as e:
        return jsonify({"error": "failed", "message": str(e)}), 500

    s = _get_autodiscovery_settings_for_key(_plex_user_key())
    return jsonify(
        {
            "ok": True,
            "enabled": bool(s.get("enabled")),
            "weekday": int(s.get("weekday", 0)),
            "time": str(s.get("time", "")),
        }
    )


_LIBRARY_CACHE_TTL_SECONDS = 60 * 5
_library_index_lock = threading.Lock()
_library_index_cache: dict[str, object] = {
    "ts": 0.0,
    "root": "",
    "track_keys": set(),
    "album_keys": set(),
    "norm_paths": [],
}

_ALBUM_STATUS_CACHE_TTL_SECONDS = 60 * 30
_album_status_cache: dict[str, tuple[float, bool, int]] = {}
_ALBUM_STATUS_CACHE_MAX = 512

_PLEX_COVER_CACHE_TTL_SECONDS = 60 * 60
_plex_cover_cache: dict[str, tuple[float, str]] = {}
_PLEX_COVER_CACHE_MAX = 2048

_TRACK_COVER_CACHE_TTL_SECONDS = 60 * 60
_track_cover_cache: dict[str, tuple[float, str, str]] = {}  # key -> (ts, url, album_name)
_TRACK_COVER_CACHE_MAX = 2048

# Library disk-scan cache: avoid re-walking the filesystem on every request.
_LIBRARY_SCAN_CACHE_TTL_SECONDS = 5 * 60
_library_scan_cache: dict[str, tuple[float, dict]] = {}

# Recommendations response cache: per-user, keyed by Last.fm username.
_RECOMMENDATIONS_CACHE_TTL_SECONDS = 5 * 60
_recommendations_cache: dict[str, tuple[float, list]] = {}


def _plex_music_library_name() -> str:
    """Best-effort: prefer env var, otherwise fall back to settings if available."""
    name = (PLEX_MUSIC_LIBRARY or "").strip()
    if name:
        return name
    try:
        s = load_settings()
        return (getattr(s, "plex_music_library", "") or "").strip() or "Music"
    except Exception:
        return "Music"


@lru_cache(maxsize=16)
def _plex_client_cached(baseurl: str, token: str, library_name: str) -> PlexClient:
    return PlexClient(baseurl, token, library_name)


def _plex_client_for_request() -> PlexClient | None:
    if not PLEX_BASEURL:
        return None
    # For library browsing/viewing, prefer the configured admin user's stored token.
    # Fallback to the current session's token if the admin hasn't signed in yet.
    token = (session.get("plex_token") or "").strip()
    if not token:
        return None

    lib = _plex_music_library_name()
    try:
        return _plex_client_cached(PLEX_BASEURL, token, lib)
    except Exception:
        return None


def _plex_norm(s: str) -> str:
    t = (s or "").lower().strip()
    t = re.sub(r"\([^)]*\)", " ", t)
    t = re.sub(r"\[[^\]]*\]", " ", t)
    t = t.replace("&", " and ")
    t = re.sub(r"[^a-z0-9]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _clean_lookup_text(value: str) -> str:
    """Clean a folder-derived name for external lookups (Plex/Last.fm).

    Keeps unicode, but removes obvious filesystem artifacts.
    Examples:
      "What Was I Made For_" -> "What Was I Made For"
      "Billie Eilish — What Was I Made For_" -> "Billie Eilish - What Was I Made For"
    """
    s = (value or "").strip()
    if not s:
        return ""
    s = s.replace("_", " ")
    # Normalize common dash variants
    s = s.replace("—", "-").replace("–", "-").replace("−", "-")
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    # Strip trailing punctuation frequently introduced by bad taggers
    s = re.sub(r"[\s\-_.]+$", "", s).strip()
    return s


def _strip_redundant_artist_prefix(artist: str, album: str) -> str:
    """If album starts with 'Artist - ' (or similar), remove that prefix."""
    a = _clean_lookup_text(artist)
    al = _clean_lookup_text(album)
    if not a or not al:
        return al

    # Compare in a normalized ASCII-ish way.
    na = _plex_norm(a)
    nal = _plex_norm(al)
    if na and nal and (nal.startswith(na + " ") or nal.startswith(na)):
        # Try splitting the original cleaned string on separators.
        # Accept patterns like "Artist - Album", "Artist — Album", "Artist: Album".
        parts = re.split(r"\s*[-:]+\s*", al, maxsplit=1)
        if len(parts) == 2 and _plex_norm(parts[0]) == na:
            return parts[1].strip()
    return al


def _plex_find_album_thumb(artist: str, album: str) -> str:
    """Return Plex thumb path for an album, or empty string if not found."""
    a = _clean_lookup_text(artist)
    al = _strip_redundant_artist_prefix(a, album)
    if not a or not al:
        return ""

    cache_key = f"{_plex_norm(a)}|||{_plex_norm(al)}"
    now = time.time()
    cached = _plex_cover_cache.get(cache_key)
    if cached and (now - cached[0]) < _PLEX_COVER_CACHE_TTL_SECONDS:
        return cached[1] or ""

    client = _plex_client_for_request()
    if not client:
        return ""

    want_a = _plex_norm(a)
    want_al = _plex_norm(al)

    candidates = []
    try:
        candidates = client.music_section.search(title=al, libtype="album") or []
    except Exception:
        candidates = []

    if not candidates:
        try:
            candidates = client.music_section.search(f"{a} {al}", libtype="album") or []
        except Exception:
            candidates = []

    best_thumb = ""
    best_score = 0
    for it in candidates:
        try:
            it_title = _plex_norm(getattr(it, "title", "") or "")
            it_artist = _plex_norm(
                getattr(it, "parentTitle", "")
                or getattr(it, "grandparentTitle", "")
                or ""
            )
            score = 0
            if want_al and it_title:
                if want_al == it_title:
                    score += 3
                elif want_al in it_title or it_title in want_al:
                    score += 2
            if want_a and it_artist:
                if want_a == it_artist:
                    score += 3
                elif want_a in it_artist or it_artist in want_a:
                    score += 2

            thumb = (
                getattr(it, "thumb", "")
                or getattr(it, "parentThumb", "")
                or ""
            )
            if thumb:
                # Slightly prefer items with art.
                score += 1

            if score > best_score and thumb:
                best_score = score
                best_thumb = str(thumb)
        except Exception:
            continue

    # Basic eviction
    if len(_plex_cover_cache) >= _PLEX_COVER_CACHE_MAX:
        oldest = sorted(_plex_cover_cache.items(), key=lambda kv: kv[1][0])[: max(1, _PLEX_COVER_CACHE_MAX // 4)]
        for k, _ in oldest:
            _plex_cover_cache.pop(k, None)

    _plex_cover_cache[cache_key] = (now, best_thumb)
    return best_thumb


def _is_disc_folder(name: str) -> bool:
    s = (name or "").strip().lower()
    if not s:
        return False
    # Common patterns: CD1, CD 1, Disc 1, Disk 1
    return bool(re.match(r"^(cd|disc|disk)\s*\d+$", s))


def _iter_library_album_entries(root: Path) -> dict[tuple[str, str], dict]:
    """Return a mapping of (artist, album) -> info based on files on disk.

    Assumes a Plex-ish folder layout: Artist/Album/(Disc N/)?Track.ext
    """
    albums: dict[tuple[str, str], dict] = {}
    if not root.exists() or not root.is_dir():
        return albums

    audio_exts = {".flac", ".mp3", ".m4a", ".aac", ".ogg", ".opus", ".wav", ".aiff", ".alac"}

    for p in root.rglob("*"):
        try:
            if not p.is_file():
                continue
            if p.suffix.lower() not in audio_exts:
                continue

            album_dir = p.parent
            artist_dir = album_dir.parent

            # Handle multi-disc: Artist/Album/Disc 1/Track
            if _is_disc_folder(album_dir.name):
                album_dir = album_dir.parent
                artist_dir = album_dir.parent

            if not album_dir.name or not artist_dir.name:
                continue

            # Ensure we are still inside root
            try:
                album_dir.relative_to(root)
            except Exception:
                continue

            artist = artist_dir.name
            album = album_dir.name
            key = (artist, album)
            ent = albums.get(key)
            if not ent:
                cover_path = ""
                # Best-effort local cover detection.
                # Plex/Taggers commonly use folder/cover/front/art filenames; some libraries just have any .jpg/.png.
                preferred = (
                    "cover.jpg",
                    "cover.jpeg",
                    "cover.png",
                    "folder.jpg",
                    "folder.jpeg",
                    "folder.png",
                    "front.jpg",
                    "front.jpeg",
                    "front.png",
                    "art.jpg",
                    "art.png",
                )

                def _rel_or_empty(fp: Path) -> str:
                    try:
                        return str(fp.relative_to(root)).replace("\\\\", "/")
                    except Exception:
                        return ""

                for cand in preferred:
                    cp = album_dir / cand
                    if cp.exists() and cp.is_file():
                        cover_path = _rel_or_empty(cp)
                        break

                if not cover_path:
                    img_exts = {".jpg", ".jpeg", ".png", ".webp"}
                    imgs: list[Path] = []
                    try:
                        for cp in album_dir.iterdir():
                            if cp.is_file() and cp.suffix.lower() in img_exts:
                                imgs.append(cp)
                    except Exception:
                        imgs = []

                    # Prefer names containing cover-like keywords.
                    def _img_score(fp: Path) -> tuple[int, str]:
                        name = fp.name.lower()
                        score = 0
                        if any(k in name for k in ("cover", "folder", "front", "art", "album")):
                            score += 10
                        if "back" in name:
                            score -= 3
                        return (score, name)

                    if imgs:
                        imgs.sort(key=_img_score, reverse=True)
                        cover_path = _rel_or_empty(imgs[0])
                ent = {
                    "type": "album",
                    "artist": artist,
                    "name": album,
                    "track_count": 0,
                    "cover_local": cover_path,
                }
                albums[key] = ent
            ent["track_count"] = int(ent.get("track_count", 0) or 0) + 1
        except Exception:
            continue

    return albums


@app.route("/api/library/albums", methods=["GET"])
def library_albums():
    """List albums discovered in the OUTPUT_PATH library.

    Query params:
        q: optional filter (matches artist or album)
        limit: maximum albums to return (default: 500)
    """
    q = (request.args.get("q", "") or "").strip().lower()
    try:
        limit = int(request.args.get("limit", 500))
    except Exception:
        limit = 500
    limit = max(1, min(limit, 5000))

    # Use an in-memory scan cache to avoid re-walking the entire filesystem on
    # every request.  The cache is keyed by the library root path and has a
    # short TTL so newly downloaded albums appear within a few minutes.
    _scan_cache_key = str(MUSIC_LIBRARY_PATH)
    _now = time.time()
    _cached_scan = _library_scan_cache.get(_scan_cache_key)
    if _cached_scan and (_now - _cached_scan[0]) < _LIBRARY_SCAN_CACHE_TTL_SECONDS:
        # Shallow-copy each value dict so downstream mutations don't corrupt the cache.
        albums_map = {k: dict(v) for k, v in _cached_scan[1].items()}
    else:
        raw_map = _iter_library_album_entries(MUSIC_LIBRARY_PATH)
        _library_scan_cache[_scan_cache_key] = (_now, raw_map)
        albums_map = {k: dict(v) for k, v in raw_map.items()}
    items = list(albums_map.values())

    if q:
        items = [
            it
            for it in items
            if q in (it.get("artist", "").lower()) or q in (it.get("name", "").lower())
        ]

    items.sort(key=lambda it: ((it.get("artist") or "").lower(), (it.get("name") or "").lower()))
    items = items[:limit]

    # For card rendering convenience, expose track count as a subtitle line.
    for it in items:
        try:
            it["album"] = {"name": f"{int(it.get('track_count', 0) or 0)} tracks"}
        except Exception:
            it["album"] = {"name": ""}

        # Local cover (preferred when available).
        cover_local = (it.get("cover_local") or "").strip()
        if cover_local:
            it["cover_url"] = f"/api/library/cover?path={quote(cover_local, safe='/')}"
        else:
            # Fallback: pull from Plex.
            it["cover_url"] = (
                f"/api/library/plex/album_cover?artist={quote(it.get('artist',''), safe='')}&album={quote(it.get('name',''), safe='')}"
            )

        # This view is explicitly "albums found on disk".
        # Completeness is hydrated client-side (via /api/album/status) so we don't
        # block this call on potentially hundreds of Last.fm requests.
        it["library_owned"] = True
        it["complete"] = None
        it["missing"] = None
        it["in_library"] = False

    return jsonify({"root": str(MUSIC_LIBRARY_PATH), "items": items})


@app.route("/api/library/cover", methods=["GET"])
def library_cover():
    """Serve a cover image from inside OUTPUT_PATH (best-effort).

    Query params:
        path: a root-relative path previously returned by /api/library/albums
    """
    rel = (request.args.get("path", "") or "").strip().lstrip("/")
    if not rel:
        return jsonify({"error": "Missing path"}), 400

    # Prevent path traversal
    rel_path = Path(rel)
    if any(part in {"..", ""} for part in rel_path.parts):
        return jsonify({"error": "Invalid path"}), 400

    full = (MUSIC_LIBRARY_PATH / rel_path).resolve()
    try:
        full.relative_to(MUSIC_LIBRARY_PATH.resolve())
    except Exception:
        return jsonify({"error": "Invalid path"}), 400

    if not full.exists() or not full.is_file():
        return jsonify({"error": "Not found"}), 404

    resp = send_from_directory(str(full.parent), full.name)
    # Local cover files are static — cache aggressively in the browser.
    resp.headers["Cache-Control"] = "public, max-age=86400"
    return resp


@app.route("/api/library/plex/album_cover", methods=["GET"])
def plex_album_cover():
    """Proxy an album cover from Plex without exposing tokens to the client.

    Query params:
        artist: album artist name
        album: album title
        w: optional width for transcode (default 420)
        h: optional height for transcode (default 420)
    """
    artist = (request.args.get("artist", "") or "").strip()
    album = (request.args.get("album", "") or "").strip()
    if not artist or not album:
        return jsonify({"error": "Missing artist or album parameter"}), 400

    # Use the current session's token for cover proxy.
    token = (session.get("plex_token") or "").strip()
    if not token or not PLEX_BASEURL:
        return jsonify({"error": "Plex not configured"}), 500

    try:
        w = int(request.args.get("w", 420))
        h = int(request.args.get("h", 420))
    except Exception:
        w, h = 420, 420
    w = max(64, min(w, 1200))
    h = max(64, min(h, 1200))

    ca = _clean_lookup_text(artist)
    cal = _strip_redundant_artist_prefix(ca, album)

    thumb = _plex_find_album_thumb(ca or artist, cal or album)
    if not thumb:
        thumb = _plex_find_album_thumb(artist, album)
    if not thumb:
        return jsonify({"error": "Not found"}), 404

    # Use Plex's photo transcode endpoint for consistent sizing.
    # It expects a URL-encoded 'url' parameter pointing to the image path.
    transcode_path = "/photo/:/transcode"
    try:
        resp = requests.get(
            f"{PLEX_BASEURL}{transcode_path}",
            params={
                "url": thumb,
                "width": str(w),
                "height": str(h),
                "minSize": "1",
                "upscale": "1",
                "X-Plex-Token": token,
            },
            headers={"Accept": "image/*", "X-Plex-Client-Identifier": PLEX_OAUTH_CLIENT_ID},
            timeout=15,
            stream=True,
            verify=PLEX_VERIFY_SSL,
        )
        if resp.status_code != 200:
            return jsonify({"error": "Plex fetch failed", "status": resp.status_code}), 502

        content_type = resp.headers.get("Content-Type", "image/jpeg")
        return Response(
            resp.content,
            status=200,
            content_type=content_type,
            headers={"Cache-Control": "public, max-age=86400"},
        )
    except Exception as e:
        return jsonify({"error": f"Plex fetch error: {e}"}), 502


@app.route("/api/library/album/local_tracks", methods=["GET"])
def library_album_local_tracks():
    """List track filenames found on disk for a given Artist/Album folder.

    This is a fallback when Last.fm isn't configured, and also useful for debugging.

    Query params:
        artist: folder name under OUTPUT_PATH
        album: folder name under artist
    """
    artist = request.args.get("artist", "").strip()
    album = request.args.get("album", "").strip()
    if not artist or not album:
        return jsonify({"error": "Missing artist or album parameter"}), 400

    album_dir = (MUSIC_LIBRARY_PATH / artist / album).resolve()
    try:
        album_dir.relative_to(MUSIC_LIBRARY_PATH.resolve())
    except Exception:
        return jsonify({"error": "Invalid path"}), 400

    if not album_dir.exists() or not album_dir.is_dir():
        return jsonify({"error": "Album folder not found"}), 404

    audio_exts = {".flac", ".mp3", ".m4a", ".aac", ".ogg", ".opus", ".wav", ".aiff", ".alac"}
    files: list[str] = []
    for p in album_dir.rglob("*"):
        try:
            if not p.is_file() or p.suffix.lower() not in audio_exts:
                continue
            files.append(p.stem)
        except Exception:
            continue

    files.sort(key=lambda s: _norm_text(s))
    items = []
    for i, stem in enumerate(files):
        title_guess = _strip_track_number(stem)
        items.append({"type": "track", "name": title_guess or stem, "artist": artist, "rank": i + 1, "in_library": True})

    return jsonify({"artist": artist, "album": album, "items": items})


def _norm_text(value: str) -> str:
    s = (value or "").lower()
    s = s.replace("&", " and ")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _norm_artist(value: str) -> str:
    """Normalize artist names for matching.

    Last.fm (and some services) include collaborators in the artist field
    (e.g. "Laufey feat. Philharmonia Orchestra"). Your Plex layout typically
    stores the primary artist as the folder name (e.g. "Laufey"), so we strip
    common collaboration suffixes.
    """
    s = (value or "").strip()
    if not s:
        return ""

    # Remove bracketed qualifiers.
    s = re.sub(r"\([^)]*\)", " ", s)
    s = re.sub(r"\[[^\]]*\]", " ", s)

    # Keep only the primary artist before collaboration markers.
    s = re.split(r"(?i)\b(feat\.?|ft\.?|featuring|with)\b", s, maxsplit=1)[0]
    s = re.split(r"(?i)\s+\bx\b\s+", s, maxsplit=1)[0]

    return _norm_text(s)


def _norm_track_title(value: str) -> str:
    """Normalize track titles to improve duplicate detection across albums.

    This is intentionally a bit more aggressive than _norm_text.
    """
    s = (value or "").strip()

    # Remove bracketed qualifiers: (Remastered 2011), [Explicit], etc.
    s = re.sub(r"\([^)]*\)", " ", s)
    s = re.sub(r"\[[^\]]*\]", " ", s)

    # Remove common featuring patterns.
    s = re.sub(r"(?i)\bfeat\.?\b.*$", " ", s)
    s = re.sub(r"(?i)\bft\.?\b.*$", " ", s)

    # Common suffixes that frequently differ between releases.
    s = re.sub(r"(?i)\b(remaster(ed)?|mono|stereo|explicit|clean|radio edit|edit)\b", " ", s)

    return _norm_text(s)


def _track_key(artist: str, title: str) -> str:
    return f"{_norm_artist(artist)}|||{_norm_track_title(title)}"


def _album_key(artist: str, album: str) -> str:
    return f"{_norm_artist(artist)}|||{_norm_text(album)}"


def _strip_track_number(name: str) -> str:
    # Examples: "01 - Title", "1. Title", "01 Title"
    s = (name or "").strip()
    s = re.sub(r"^\s*\d{1,3}\s*[-. ]\s*", "", s)
    return s.strip()


def _build_library_index(root: Path) -> tuple[set[str], set[str], list[str]]:
    track_keys: set[str] = set()
    album_keys: set[str] = set()
    norm_paths: list[str] = []

    if not root.exists() or not root.is_dir():
        return track_keys, album_keys, norm_paths

    audio_exts = {".flac", ".mp3", ".m4a", ".aac", ".ogg", ".opus", ".wav", ".aiff", ".alac"}

    # Walk all files once. This is cached with a TTL.
    for p in root.rglob("*"):
        try:
            if not p.is_file():
                continue
            if p.suffix.lower() not in audio_exts:
                continue

            try:
                rel = p.relative_to(root)
                rel_str = str(rel)
            except Exception:
                rel_str = str(p)

            norm_paths.append(_norm_text(rel_str))

            # Primary heuristics for Plex-style layout: Artist/Album/Track.ext
            parent = p.parent.name
            grandparent = p.parent.parent.name if p.parent and p.parent.parent else ""
            if parent and grandparent:
                album_keys.add(_album_key(grandparent, parent))

                # Track key from folder artist + filename (minus track number)
                title_guess = _strip_track_number(p.stem)
                if title_guess:
                    track_keys.add(_track_key(grandparent, title_guess))

            # Secondary heuristic for non-Plex layouts: Artist - ... - Title
            stem = p.stem.replace("–", "-").replace("—", "-")
            parts = [x.strip() for x in stem.split("-") if x.strip()]
            if len(parts) >= 2:
                artist_guess = parts[0]
                title_guess = _strip_track_number(parts[-1])
                if artist_guess and title_guess:
                    track_keys.add(_track_key(artist_guess, title_guess))
        except Exception:
            continue

    return track_keys, album_keys, norm_paths


def _get_library_index() -> tuple[set[str], set[str], list[str]]:
    now = time.time()
    root_str = str(MUSIC_LIBRARY_PATH)

    # Fast path: check without the lock first to avoid contention on cache hits.
    ts = float(_library_index_cache.get("ts", 0.0) or 0.0)
    cached_root = str(_library_index_cache.get("root", "") or "")
    if cached_root == root_str and (now - ts) < _LIBRARY_CACHE_TTL_SECONDS:
        return (
            _library_index_cache.get("track_keys", set()),
            _library_index_cache.get("album_keys", set()),
            _library_index_cache.get("norm_paths", []),
        )

    # Slow path: acquire lock so only one thread builds at a time.
    with _library_index_lock:
        # Re-check inside the lock — another thread may have built it while we waited.
        ts = float(_library_index_cache.get("ts", 0.0) or 0.0)
        cached_root = str(_library_index_cache.get("root", "") or "")
        if cached_root == root_str and (now - ts) < _LIBRARY_CACHE_TTL_SECONDS:
            return (
                _library_index_cache.get("track_keys", set()),
                _library_index_cache.get("album_keys", set()),
                _library_index_cache.get("norm_paths", []),
            )

        track_keys, album_keys, norm_paths = _build_library_index(MUSIC_LIBRARY_PATH)
        _library_index_cache["ts"] = time.time()
        _library_index_cache["root"] = root_str
        _library_index_cache["track_keys"] = track_keys
        _library_index_cache["album_keys"] = album_keys
        _library_index_cache["norm_paths"] = norm_paths
        return track_keys, album_keys, norm_paths


def _primary_artist(artist: str) -> str:
    """Return just the first/primary artist from a comma-separated list.

    Spotify returns all collaborators joined by commas, e.g. "beabadoobee, Laufey".
    Plex typically stores tracks under the primary (first) artist folder only.
    """
    return artist.split(",")[0].strip()


def _track_in_library(artist: str, title: str) -> bool:
    track_keys, _, norm_paths = _get_library_index()

    # Try full artist string first.
    key = _track_key(artist, title)
    if key in track_keys:
        return True

    # Try with only the primary (first) artist — handles "Artist A, Artist B" style.
    primary = _primary_artist(artist)
    if primary and primary.lower() != artist.lower():
        key2 = _track_key(primary, title)
        if key2 in track_keys:
            return True

    # Fallback: fuzzy match in normalized paths using primary artist.
    a_full = _norm_artist(artist)
    a_primary = _norm_artist(primary)
    t = _norm_track_title(title)
    if not t:
        return False
    for np in norm_paths:
        if t in np and (a_full in np or a_primary in np):
            return True
    return False


def _album_in_library(artist: str, album: str) -> bool:
    _, album_keys, norm_paths = _get_library_index()
    key = _album_key(artist, album)
    if key in album_keys:
        return True
    a = _norm_artist(artist)
    al = _norm_text(album)
    if not a or not al:
        return False
    for np in norm_paths:
        if a in np and al in np:
            return True
    return False


def _annotate_in_library(items: list[dict], item_type: str) -> None:
    t = (item_type or "").lower()
    if t == "track":
        for it in items:
            title = it.get("name") or it.get("title") or ""
            artist = it.get("artist") or ""
            it["in_library"] = bool(artist and title and _track_in_library(artist, title))
    elif t == "album":
        for it in items:
            album = it.get("name") or it.get("title") or ""
            artist = it.get("artist") or ""
            
            # Basic folder check first (fast)
            basic_in_lib = bool(artist and album and _album_in_library(artist, album))
            
            # If folder exists, do a strict check of all tracks to avoid false positives on partials.
            # This triggers an API call, but only for albums we *think* we have.
            if basic_in_lib:
                # _album_all_tracks_in_library caches results so re-renders are fast.
                try:
                    strict_in_lib, _ = _album_all_tracks_in_library(artist, album)
                    it["in_library"] = strict_in_lib
                except Exception:
                    # API error or timeout? Fallback to basic check but log it?
                    # For now, trust the basic check if API fails.
                    it["in_library"] = True
            else:
                it["in_library"] = False


def _album_all_tracks_in_library(artist: str, album: str) -> tuple[bool, int]:
    """Return (in_library, missing_count) based on track presence anywhere in library."""
    a = (artist or "").strip()
    al = (album or "").strip()
    if not a or not al:
        return False, 0

    # Clean up folder-derived names for Last.fm lookups.
    ca = _clean_lookup_text(a)
    cal = _strip_redundant_artist_prefix(ca, al)

    # Cache by normalized key to avoid hammering Last.fm.
    cache_key = _album_key(a, al)
    now = time.time()
    cached = _album_status_cache.get(cache_key)
    if cached and (now - cached[0]) < _ALBUM_STATUS_CACHE_TTL_SECONDS:
        _, in_lib, missing = cached
        return in_lib, missing

    if not LASTFM_API_KEY:
        return False, 0

    tracks = []
    try:
        lastfm = LastFmClient(api_key=LASTFM_API_KEY, username=LASTFM_USERNAME)
        # Try a few variants to handle odd folder naming.
        candidates = []
        for aa, bb in ((a, al), (ca, cal), (a, cal), (ca, al)):
            aa = (aa or "").strip()
            bb = (bb or "").strip()
            if aa and bb:
                candidates.append((aa, bb))
        # De-dupe preserving order
        seen = set()
        uniq = []
        for aa, bb in candidates:
            k = (_plex_norm(aa), _plex_norm(bb))
            if k in seen:
                continue
            seen.add(k)
            uniq.append((aa, bb))

        for aa, bb in uniq:
            tracks = lastfm.get_album_tracks(aa, bb)
            if tracks:
                break
    except Exception:
        tracks = []

    if not tracks:
        in_lib = _album_in_library(a, al)
        missing = 0 if in_lib else 0
    else:
        missing = 0
        for t_artist, t_title in tracks:
            if not _track_in_library(t_artist, t_title):
                missing += 1
        in_lib = missing == 0

    # Basic eviction
    if len(_album_status_cache) >= _ALBUM_STATUS_CACHE_MAX:
        oldest = sorted(_album_status_cache.items(), key=lambda kv: kv[1][0])[: max(1, _ALBUM_STATUS_CACHE_MAX // 4)]
        for k, _ in oldest:
            _album_status_cache.pop(k, None)

    _album_status_cache[cache_key] = (now, in_lib, missing)
    return in_lib, missing


@app.route("/api/album/status", methods=["GET"])
def album_status():
    """Check whether an album is already satisfied by the library.

    Query params:
        artist: artist name (required)
        album: album name (required)
    """
    artist = request.args.get("artist", "").strip()
    album = request.args.get("album", "").strip()

    if not artist or not album:
        return jsonify({"error": "Missing artist or album parameter"}), 400

    try:
        in_lib, missing = _album_all_tracks_in_library(artist, album)
        return jsonify({"artist": artist, "album": album, "in_library": in_lib, "missing": missing})
    except Exception as e:
        logger.error(f"Album status error: {e}")
        return jsonify({"error": str(e)}), 500


def _read_dotenv(path: str) -> dict[str, str]:
    """Minimal .env parser (KEY=VALUE)."""
    try:
        p = Path(path)
        if not p.exists() or not p.is_file():
            return {}

        out: dict[str, str] = {}
        for raw_line in p.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.lower().startswith("export "):
                line = line[7:].strip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key:
                out[key] = value
        return out
    except Exception:
        return {}


def _get_persisted_env_path() -> Path:
    """Return the .env file used for persisting admin config.

    In Docker the /config volume is always present; locally we fall back
    to a ``.env`` file next to the package root.
    """
    docker_dir = Path("/config")
    if docker_dir.exists() and docker_dir.is_dir():
        return docker_dir / ".env"
    # Local / dev: write next to the project root (two levels above webui.py)
    return Path(__file__).resolve().parents[2] / ".env"


def _write_dotenv_key(path: Path, key: str, value: str) -> None:
    """Write or update a single KEY=\"value\" entry in a .env file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    if path.exists():
        lines = path.read_text(encoding="utf-8").splitlines()

    prefix = f"{key}="
    found = False
    new_lines: list[str] = []
    for line in lines:
        stripped = line.strip()
        bare = stripped[7:].strip() if stripped.lower().startswith("export ") else stripped
        if bare.startswith(prefix):
            new_lines.append(f'{key}="{value}"')
            found = True
        else:
            new_lines.append(line)
    if not found:
        new_lines.append(f'{key}="{value}"')
    path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")


def _get_spotify_credentials() -> tuple[str, str]:
    """Return the Spotify client ID for anonymous web-player access."""
    return _builtin_spotify_client_id(), ""


def _itunes_preview(artist: str, title: str) -> str:
    """Fallback preview provider (no credentials): iTunes Search API."""
    try:
        artist = (artist or "").strip()
        title = (title or "").strip()
        if not artist or not title:
            return ""

        term = f"{artist} {title}"
        resp = requests.get(
            "https://itunes.apple.com/search",
            params={"term": term, "entity": "song", "limit": 10},
            timeout=10,
        )
        if resp.status_code != 200:
            return ""
        data = resp.json()
        results = data.get("results", [])
        if not isinstance(results, list):
            return ""

        want_artist = _norm_artist(artist)
        want_title = _norm_track_title(title)

        best = ""
        for r in results:
            p = r.get("previewUrl") or ""
            if not p:
                continue

            ra = _norm_artist(r.get("artistName") or "")
            rt = _norm_track_title(r.get("trackName") or "")
            if ra == want_artist and rt == want_title:
                return p
            if not best:
                best = p
        return best
    except Exception:
        return ""


def _is_lastfm_placeholder_image(url: str | None) -> bool:
    if not url:
        return True
    return "2a96cbd8b46e442fc41c2b86b821562f" in url


def _best_lastfm_image_url(images: list[dict] | None) -> str:
    if not images or not isinstance(images, list):
        return ""
    for size in ["extralarge", "large", "medium"]:
        for img in images:
            if img.get("size") == size and img.get("#text"):
                return img.get("#text")
    for img in reversed(images):
        if img.get("#text"):
            return img.get("#text")
    return ""


def _lastfm_track_cover_url(artist: str, title: str) -> str:
    """Best-effort cover for a track using Last.fm track.getInfo."""
    url, _album = _lastfm_track_cover_info(artist, title)
    return url


def _lastfm_track_cover_info(artist: str, title: str) -> tuple[str, str]:
    """Return (cover_url, album_name) for a track using Last.fm track.getInfo."""
    if not LASTFM_API_KEY:
        return "", ""
    a = (artist or "").strip()
    t = (title or "").strip()
    if not a or not t:
        return "", ""

    try:
        resp = requests.get(
            "https://ws.audioscrobbler.com/2.0/",
            params={
                "method": "track.getInfo",
                "api_key": LASTFM_API_KEY,
                "artist": a,
                "track": t,
                "format": "json",
                "autocorrect": 1,
            },
            timeout=10,
        )
        if resp.status_code != 200:
            return "", ""
        data = resp.json() if isinstance(resp.json(), dict) else {}
        track_obj = data.get("track", {}) if isinstance(data, dict) else {}
        album_obj = track_obj.get("album", {}) if isinstance(track_obj, dict) else {}
        album_name = (album_obj.get("title") or "") if isinstance(album_obj, dict) else ""
        images = album_obj.get("image", []) if isinstance(album_obj, dict) else []
        url = _best_lastfm_image_url(images)
        if _is_lastfm_placeholder_image(url):
            return "", album_name
        return url, album_name
    except Exception:
        return "", ""


def _cached_lastfm_track_cover_url(artist: str, title: str) -> str:
    """Cached cover URL for a track (Last.fm track.getInfo), best-effort."""
    url, _album = _cached_lastfm_track_cover_info(artist, title)
    return url


def _cached_lastfm_track_cover_info(artist: str, title: str) -> tuple[str, str]:
    """Cached (cover_url, album_name) for a track (Last.fm track.getInfo), best-effort."""
    if not LASTFM_API_KEY:
        return "", ""
    a = (artist or "").strip()
    t = (title or "").strip()
    if not a or not t:
        return "", ""

    key = f"{_norm_artist(a)}|||{_norm_track_title(t)}"
    now = time.time()
    cached = _track_cover_cache.get(key)
    if cached and (now - cached[0]) < _TRACK_COVER_CACHE_TTL_SECONDS:
        return cached[1] or "", cached[2] if len(cached) > 2 else ""

    url, album_name = _lastfm_track_cover_info(a, t)

    if len(_track_cover_cache) >= _TRACK_COVER_CACHE_MAX:
        oldest = sorted(_track_cover_cache.items(), key=lambda kv: kv[1][0])[: max(1, _TRACK_COVER_CACHE_MAX // 4)]
        for k, _ in oldest:
            _track_cover_cache.pop(k, None)

    _track_cover_cache[key] = (now, url, album_name)
    return url, album_name

# Store for tracking download progress (in-memory, simple)
download_status = {}

# Server-side download queue (single worker thread).
_download_lock = threading.Lock()
_download_queue: list[str] = []
_download_worker: threading.Thread | None = None


def _scraper_base_cmd() -> list[str]:
    """Return the configured base command as a list.

    Supports setting SCRAPER_BIN to a composite command, e.g.
    "docker run ... scraper".
    """
    raw = (SCRAPER_BIN or "scraper").strip()
    if not raw:
        return ["scraper"]
    try:
        parts = shlex.split(raw)
        return parts if parts else [raw]
    except Exception:
        return [raw]


def _build_scraper_cmd(csv_path: str) -> list[str]:
    return _scraper_base_cmd() + ["--csv", csv_path, "--output", str(MUSIC_LIBRARY_PATH)]


def _job_view(job: dict) -> dict:
    now = time.time()
    created_at = float(job.get("created_at") or 0)
    started_at = job.get("started_at")
    finished_at = job.get("finished_at")

    total = int(job.get("total_tracks") or 0)
    completed = int(job.get("completed_tracks") or 0)
    failed = int(job.get("failed_tracks") or 0)

    elapsed = 0.0
    if started_at:
        elapsed = max(0.0, (float(finished_at or now) - float(started_at)))

    # Speed in tracks/minute (best-effort)
    speed_tpm = 0.0
    if elapsed > 1e-6:
        speed_tpm = (completed / elapsed) * 60.0

    # Best-effort Mbps (from streaming scraper output). May be missing.
    try:
        speed_mbps = float(job.get("speed_mbps") or 0.0)
    except Exception:
        speed_mbps = 0.0

    remaining = max(0, total - completed - failed)
    eta_seconds = None
    if speed_tpm > 1e-6 and remaining > 0:
        eta_seconds = int(round((remaining / speed_tpm) * 60.0))

    ratio = 0.0
    if total > 0:
        ratio = max(0.0, min(1.0, (completed + failed) / float(total)))

    # Show "current index" as X/Y while running (more intuitive than 0/Y).
    status = str(job.get("status") or "").lower()
    current_index = int(job.get("current_index") or 0)
    if status == "running" and total > 0 and current_index > 0:
        progress_text = f"{min(total, current_index)}/{total}"
    else:
        progress_text = f"{min(total, completed + failed)}/{total}" if total else "0/0"

    out = dict(job)
    out["elapsed_seconds"] = int(round(elapsed)) if started_at else 0
    out["speed_tracks_per_min"] = round(speed_tpm, 2)
    out["speed_mbps"] = round(speed_mbps, 2) if speed_mbps > 0 else 0.0
    out["eta_seconds"] = eta_seconds
    out["progress_ratio"] = round(ratio, 4)
    out["progress_text"] = progress_text

    # Best-effort cover art for the currently downloading track (running jobs only).
    # This is cached to avoid hammering Last.fm during 1s polling.
    try:
        if str(out.get("status") or "").lower() == "running":
            ct = out.get("current_track") or {}
            if isinstance(ct, dict):
                cta = (ct.get("artist") or "").strip()
                ctt = (ct.get("title") or "").strip()
                if cta and ctt:
                    out["current_track_cover_url"] = _cached_lastfm_track_cover_url(cta, ctt)
    except Exception:
        pass
    return out


def _downloads_snapshot() -> dict:
    """Return the download queue and history visible to the current session user."""
    viewer = _session_username_lower()

    with _download_lock:
        all_jobs = [_job_view(j) for j in download_status.values()]

    # Filter active jobs by owner.
    def _owned(j: dict) -> bool:
        owner = (j.get("submitted_by") or "").strip().lower()
        # Jobs created before this feature was added have no submitted_by —
        # show them to everyone so existing downloads aren't orphaned.
        return (not owner) or owner == viewer

    jobs = [j for j in all_jobs if _owned(j)]

    def _sort_key(j: dict):
        status = str(j.get("status") or "").lower()
        if status == "running":
            bucket = 0
        elif status == "queued":
            bucket = 1
        else:
            bucket = 2
        ts = float(j.get("started_at") or j.get("created_at") or 0)
        return (bucket, -ts)

    jobs.sort(key=_sort_key)

    running = sum(1 for j in jobs if j.get("status") == "running")
    queued = sum(1 for j in jobs if j.get("status") == "queued")
    active = sum(1 for j in jobs if j.get("status") in {"queued", "running"})

    active_jobs = [j for j in jobs if j.get("status") in {"queued", "running"}]
    tracks_total = sum(int(j.get("total_tracks") or 0) for j in active_jobs)
    tracks_done = sum(int(j.get("completed_tracks") or 0) + int(j.get("failed_tracks") or 0) for j in active_jobs)
    running_job = next((j for j in jobs if j.get("status") == "running"), None)

    # Queue-wide ETA (seconds) based on running job throughput (tracks/min).
    queue_eta_seconds = None
    speed_mbps = 0.0
    if running_job:
        try:
            speed_mbps = float(running_job.get("speed_mbps") or 0.0)
        except Exception:
            speed_mbps = 0.0
        try:
            tpm = float(running_job.get("speed_tracks_per_min") or 0.0)
        except Exception:
            tpm = 0.0
        if tpm > 1e-6 and tracks_total > 0:
            # Treat the currently downloading track as "in progress".
            remaining_after_current = max(0, tracks_total - (tracks_done + 1))
            queue_eta_seconds = int(round((remaining_after_current / tpm) * 60.0))

    queue_progress_text = "_/_"
    if running_job and tracks_total > 0:
        queue_progress_text = f"{min(tracks_total, tracks_done + 1)}/{tracks_total}"

    # Load on-disk history, filtered by viewer.
    try:
        raw_history = _load_history()
    except Exception:
        raw_history = []
    history = [
        h for h in raw_history
        if (not h.get("submitted_by")) or h.get("submitted_by", "").lower() == viewer
    ][:100]  # cap at 100 for the API response

    # Include a flag so the frontend knows whether to show the submitted_by column.
    return {
        "jobs": jobs,
        "history": history,
        "is_admin": False,
        "summary": {
            "queued": queued,
            "running": running,
            "active": active,
            "tracks_total": tracks_total,
            "tracks_done": tracks_done,
            "queue_progress_text": queue_progress_text,
            "queue_eta_seconds": queue_eta_seconds,
            "speed_mbps": round(speed_mbps, 2) if speed_mbps > 0 else 0.0,
        },
    }


_SPEED_RE_MBPS = re.compile(r"(?P<v>\d+(?:\.\d+)?)\s*(?P<u>MiB/s|MB/s)", re.IGNORECASE)


def _extract_speed_mbps(text: str) -> float:
    """Extract a best-effort Mbps reading from scraper output.

    The scraper prints "MB/s" (decimal megabytes/sec) and sometimes "MiB/s".
    Convert to megabits/sec (Mbps) for UI display.
    """
    if not text:
        return 0.0
    m = None
    for m in _SPEED_RE_MBPS.finditer(text):
        pass
    if not m:
        return 0.0
    try:
        v = float(m.group("v"))
    except Exception:
        return 0.0
    u = (m.group("u") or "").lower()
    if v <= 0:
        return 0.0
    if "mib" in u:
        return v * 8.388608
    return v * 8.0


def _ensure_download_worker() -> None:
    global _download_worker
    with _download_lock:
        if _download_worker and _download_worker.is_alive():
            return
        _download_worker = threading.Thread(target=_download_worker_loop, daemon=True)
        _download_worker.start()


def _has_active_download_job(submitted_by: str, artist: str, title: str, dl_type: str) -> bool:
    owner = (submitted_by or "").strip().lower()
    norm_artist = _norm_artist(artist)
    norm_title = _norm_text(title)

    with _download_lock:
        for job in download_status.values():
            if str(job.get("status") or "").lower() not in {"queued", "running"}:
                continue
            if (job.get("submitted_by") or "").strip().lower() != owner:
                continue
            if str(job.get("type") or "").lower() != str(dl_type or "").lower():
                continue
            if _norm_artist(job.get("artist") or "") != norm_artist:
                continue
            if _norm_text(job.get("title") or "") != norm_title:
                continue
            return True
    return False


def _expand_download_request(artist: str, title: str, dl_type: str) -> dict:
    if not artist:
        raise ValueError("Missing artist field")

    client = None
    if dl_type in {"album", "artist"}:
        if not LASTFM_API_KEY:
            raise ValueError("LASTFM_API_KEY needed for album/artist download expansion")
        client = LastFmClient(api_key=LASTFM_API_KEY, username=LASTFM_USERNAME)

    tracks_to_download: list[dict[str, str]] = []
    skipped_existing: list[list[str]] = []
    skipped_duplicates: list[list[str]] = []
    planned_keys: set[str] = set()

    if dl_type == "track":
        if not title:
            raise ValueError("Missing title for track download")
        if _track_in_library(artist, title):
            return {
                "tracks_to_download": [],
                "skipped_existing": skipped_existing,
                "skipped_duplicates": skipped_duplicates,
                "already_in_library": True,
                "message": "Already in library",
            }
        track_key = _track_key(artist, title)
        if track_key in planned_keys:
            return {
                "tracks_to_download": [],
                "skipped_existing": skipped_existing,
                "skipped_duplicates": skipped_duplicates,
                "already_in_library": True,
                "message": "Duplicate track in request",
            }
        planned_keys.add(track_key)
        tracks_to_download.append({"artist": artist, "title": title})

    elif dl_type == "album":
        logger.info("Expanding album: %s by %s", title, artist)
        tracks = client.get_album_tracks(artist, title)
        if not tracks:
            raise FileNotFoundError("Could not find tracks for this album")
        for t_artist, t_title in tracks:
            track_key = _track_key(t_artist, t_title)
            if track_key in planned_keys:
                skipped_duplicates.append([t_artist, t_title])
                continue
            planned_keys.add(track_key)
            if _track_in_library(t_artist, t_title):
                skipped_existing.append([t_artist, t_title])
            else:
                tracks_to_download.append({"artist": t_artist, "title": t_title})

    elif dl_type == "artist":
        logger.info("Expanding artist top tracks: %s", artist)
        tracks = client.get_artist_top_tracks(artist, limit=10)
        if not tracks:
            raise FileNotFoundError("Could not find top tracks for this artist")
        for t_artist, t_title in tracks:
            track_key = _track_key(t_artist, t_title)
            if track_key in planned_keys:
                skipped_duplicates.append([t_artist, t_title])
                continue
            planned_keys.add(track_key)
            if _track_in_library(t_artist, t_title):
                skipped_existing.append([t_artist, t_title])
            else:
                tracks_to_download.append({"artist": t_artist, "title": t_title})
    else:
        raise ValueError("Unsupported download type")

    return {
        "tracks_to_download": tracks_to_download,
        "skipped_existing": skipped_existing,
        "skipped_duplicates": skipped_duplicates,
        "already_in_library": not tracks_to_download,
        "message": "Already in library" if not tracks_to_download else "Queued",
    }


def _enqueue_download_job(
    artist: str,
    title: str,
    dl_type: str,
    submitted_by: str,
    *,
    source: str = "",
    extra_fields: dict | None = None,
) -> dict:
    expanded = _expand_download_request(artist, title, dl_type)
    tracks_to_download = expanded["tracks_to_download"]
    skipped_existing = expanded["skipped_existing"]
    skipped_duplicates = expanded["skipped_duplicates"]

    if not tracks_to_download:
        return {
            "success": True,
            "message": expanded.get("message") or "Already in library",
            "already_in_library": True,
            "skipped": len(skipped_existing),
            "skipped_duplicates": len(skipped_duplicates),
            "download_id": "",
            "total_tracks": 0,
        }

    job_id = f"dl_{int(time.time())}_{secrets.token_hex(6)}"
    job = {
        "id": job_id,
        "type": dl_type,
        "artist": artist,
        "title": title,
        "status": "queued",
        "created_at": time.time(),
        "started_at": None,
        "finished_at": None,
        "message": "Queued",
        "tracks": tracks_to_download,
        "total_tracks": len(tracks_to_download),
        "completed_tracks": 0,
        "failed_tracks": 0,
        "failed_tracks_list": [],
        "current_index": 0,
        "current_track": None,
        "submitted_by": (submitted_by or "").strip().lower(),
        "last_error": "",
        "last_output": "",
        "source": source,
    }
    if extra_fields:
        job.update(extra_fields)

    with _download_lock:
        download_status[job_id] = job
        _download_queue.append(job_id)

    _ensure_download_worker()

    return {
        "success": True,
        "message": "Queued",
        "already_in_library": False,
        "download_id": job_id,
        "total_tracks": len(tracks_to_download),
        "skipped": len(skipped_existing),
        "skipped_duplicates": len(skipped_duplicates),
    }


def _execute_track_download(job: dict, t_artist: str, t_title: str) -> bool:
    """Download one track via scraper subprocess and update job counters.

    This is the per-track unit of work shared by both normal download jobs and
    profile_import jobs. Returns True on success, False on any failure.
    """
    import tempfile
    import csv

    if not t_artist or not t_title:
        with _download_lock:
            job["failed_tracks"] = int(job.get("failed_tracks") or 0) + 1
            job.setdefault("failed_tracks_list", []).append(
                {"artist": t_artist, "title": t_title, "error": "Missing artist or title"}
            )
        return False

    tmp_path = None
    try:
        temp_csv = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, newline="", encoding="utf-8"
        )
        tmp_path = temp_csv.name
        writer = csv.writer(temp_csv)
        writer.writerow(["artist", "title"])
        writer.writerow([t_artist, t_title])
        temp_csv.close()

        cmd = _build_scraper_cmd(tmp_path)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

        output_tail_box: list[str] = [""]

        def _reader() -> None:
            try:
                if not proc.stdout:
                    return
                while True:
                    chunk = proc.stdout.read(128)
                    if not chunk:
                        return
                    output_tail_box[0] = (output_tail_box[0] + chunk)[-8000:]
                    mbps = _extract_speed_mbps(output_tail_box[0])
                    with _download_lock:
                        if mbps > 0:
                            mbps = min(mbps, WEBUI_SPEED_MBPS_CAP)
                            try:
                                prev = float(job.get("speed_mbps") or 0.0)
                            except Exception:
                                prev = 0.0
                            alpha = 0.25
                            job["speed_mbps"] = round(
                                mbps if prev <= 0 else prev * (1 - alpha) + mbps * alpha, 2
                            )
                        job["last_output"] = output_tail_box[0][-4000:]
            except Exception:
                return

        reader_t = threading.Thread(target=_reader, daemon=True)
        reader_t.start()
        timed_out = False
        try:
            proc.wait(timeout=DOWNLOAD_TRACK_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            timed_out = True
            try:
                proc.kill()
            except Exception:
                pass
            try:
                proc.wait(timeout=5)
            except Exception:
                pass
        try:
            reader_t.join(timeout=2.0)
        except Exception:
            pass

        if timed_out:
            err = f"Timed out after {DOWNLOAD_TRACK_TIMEOUT_S}s"
            with _download_lock:
                job["failed_tracks"] = int(job.get("failed_tracks") or 0) + 1
                job["last_error"] = err
                job.setdefault("failed_tracks_list", []).append(
                    {"artist": t_artist, "title": t_title, "error": err}
                )
            return False

        rc = int(proc.returncode or 0)
        if rc == 0:
            with _download_lock:
                job["completed_tracks"] = int(job.get("completed_tracks") or 0) + 1
                job["last_error"] = ""
            return True
        else:
            tail = output_tail_box[0]
            brief = next(
                (ln.strip() for ln in reversed(tail.splitlines()) if ln.strip()),
                tail[-200:].strip(),
            )
            with _download_lock:
                job["failed_tracks"] = int(job.get("failed_tracks") or 0) + 1
                job["last_error"] = tail[-4000:]
                job.setdefault("failed_tracks_list", []).append(
                    {"artist": t_artist, "title": t_title, "error": brief}
                )
            return False
    except Exception as e:
        with _download_lock:
            job["failed_tracks"] = int(job.get("failed_tracks") or 0) + 1
            job["last_error"] = str(e)
            job.setdefault("failed_tracks_list", []).append(
                {"artist": t_artist, "title": t_title, "error": str(e)[:200]}
            )
        return False
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass


def _execute_batch_download(job: dict, tracks: list[dict]) -> None:
    """Download a batch of tracks via a single scraper --csv invocation.

    One subprocess handles the whole list using the scraper's internal parallel
    workers (SCRAPER_WORKERS).  Per-track progress is tracked in real-time by
    parsing structured [TRACK_OK] / [TRACK_FAIL] lines emitted by the Go binary,
    and the [N/M] Resolving ... lines are used to update the UI during the
    song.link resolution phase.
    """
    import tempfile
    import csv as _csv
    import re as _re

    if not tracks:
        return

    tmp_path = None
    evaluated_path = None
    try:
        tf = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, newline="", encoding="utf-8"
        )
        tmp_path = tf.name
        evaluated_path = str(Path(tmp_path).with_name("discover-weekly-report-evaluated.csv"))
        writer = _csv.writer(tf)
        writer.writerow(["artist", "title", "spotify_id"])
        for t in tracks:
            writer.writerow([t["artist"], t["title"], t.get("spotify_id") or ""])
        tf.close()

        cmd = _build_scraper_cmd(tmp_path)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)

        _TRACK_OK_RE      = _re.compile(r'^\[TRACK_OK\] (.+?) \|\| (.+?)$')
        _TRACK_FAIL_RE    = _re.compile(r'^\[TRACK_FAIL\] (.+?) \|\| (.+?) \|\| (.*)$')
        _RESOLVING_RE     = _re.compile(r'^\[(\d+)/\d+\] Resolving platforms: (.+?) - (.+?)$')
        _DOWNLOADING_RE   = _re.compile(r'^\[(\d+)/(\d+)\] Downloading: (.+?) - (.+?)$')

        output_tail: list[str] = [""]

        def _reader() -> None:
            try:
                if not proc.stdout:
                    return
                for raw in proc.stdout:
                    output_tail[0] = (output_tail[0] + raw)[-8000:]
                    line = raw.rstrip()
                    mbps = _extract_speed_mbps(output_tail[0])
                    m_ok   = _TRACK_OK_RE.match(line)
                    m_fail = _TRACK_FAIL_RE.match(line)
                    m_res  = _RESOLVING_RE.match(line)
                    m_dl   = _DOWNLOADING_RE.match(line)
                    with _download_lock:
                        job["last_output"] = output_tail[0][-4000:]
                        if mbps > 0:
                            mbps = min(mbps, WEBUI_SPEED_MBPS_CAP)
                            try:
                                prev = float(job.get("speed_mbps") or 0.0)
                            except Exception:
                                prev = 0.0
                            job["speed_mbps"] = round(
                                mbps if prev <= 0 else prev * 0.75 + mbps * 0.25, 2
                            )
                        if m_ok:
                            t_a, t_t = m_ok.group(1), m_ok.group(2)
                            job["completed_tracks"] = int(job.get("completed_tracks") or 0) + 1
                            done = int(job.get("completed_tracks") or 0) + int(job.get("failed_tracks") or 0)
                            job["current_index"] = done
                            job["current_track"] = {"artist": t_a, "title": t_t}
                            job["message"] = f"Downloaded {done}/{len(tracks)}: {t_a} \u2013 {t_t}"
                            job["last_error"] = ""
                        elif m_fail:
                            t_a, t_t, err = m_fail.group(1), m_fail.group(2), m_fail.group(3)
                            job["failed_tracks"] = int(job.get("failed_tracks") or 0) + 1
                            done = int(job.get("completed_tracks") or 0) + int(job.get("failed_tracks") or 0)
                            job["current_index"] = done
                            job["current_track"] = {"artist": t_a, "title": t_t}
                            job["message"] = f"Failed {done}/{len(tracks)}: {t_a} \u2013 {t_t}"
                            job["last_error"] = err
                            job.setdefault("failed_tracks_list", []).append(
                                {"artist": t_a, "title": t_t, "error": err}
                            )
                        elif m_dl:
                            n, total_str, t_a, t_t = m_dl.group(1), m_dl.group(2), m_dl.group(3), m_dl.group(4)
                            job["current_track"] = {"artist": t_a, "title": t_t}
                            job["message"] = f"Downloading {n}/{total_str}: {t_a} \u2013 {t_t}"
                        elif m_res:
                            t_a, t_t = m_res.group(2), m_res.group(3)
                            job["current_track"] = {"artist": t_a, "title": t_t}
                            job["message"] = f"Resolving {m_res.group(1)}/{len(tracks)}: {t_a} \u2013 {t_t}"
            except Exception:
                return

        reader_t = threading.Thread(target=_reader, daemon=True)
        reader_t.start()

        # Total timeout: allow ~2 min/track on average (resolution + download),
        # with a hard cap of 4 hours.
        total_timeout = min(max(DOWNLOAD_TRACK_TIMEOUT_S, len(tracks) * 120), 14400)
        timed_out = False
        try:
            proc.wait(timeout=total_timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            try:
                proc.kill()
            except Exception:
                pass
            try:
                proc.wait(timeout=5)
            except Exception:
                pass
        try:
            reader_t.join(timeout=5.0)
        except Exception:
            pass

        # Reconcile counters from the scraper's evaluated CSV. This makes the
        # persisted history robust even if the live stdout parser misses some
        # [TRACK_OK]/[TRACK_FAIL] lines.
        success_pairs: set[tuple[str, str]] = set()
        if evaluated_path and os.path.exists(evaluated_path):
            try:
                with open(evaluated_path, "r", encoding="utf-8", newline="") as fh:
                    reader = _csv.DictReader(fh)
                    for row in reader:
                        artist = (row.get("artist") or "").strip()
                        title = (row.get("title") or "").strip()
                        if artist and title:
                            success_pairs.add((artist, title))
            except Exception:
                pass

        with _download_lock:
            total_tracks = len(tracks)
            recorded_completed = int(job.get("completed_tracks") or 0)
            recorded_failed = int(job.get("failed_tracks") or 0)

            if success_pairs:
                job["completed_tracks"] = max(recorded_completed, len(success_pairs))

            completed_pairs = set(success_pairs)
            failed_entries = list(job.get("failed_tracks_list") or [])
            failed_pairs = {
                ((entry.get("artist") or "").strip(), (entry.get("title") or "").strip())
                for entry in failed_entries
                if (entry.get("artist") or "").strip() and (entry.get("title") or "").strip()
            }

            missing_pairs = []
            for track in tracks:
                pair = ((track.get("artist") or "").strip(), (track.get("title") or "").strip())
                if not pair[0] or not pair[1]:
                    continue
                if pair in completed_pairs or pair in failed_pairs:
                    continue
                missing_pairs.append(pair)

            if missing_pairs:
                if int(proc.returncode or 0) == 0:
                    # Successful process with missing structured events: treat the
                    # remaining tracks as successful so history reflects reality.
                    job["completed_tracks"] = int(job.get("completed_tracks") or 0) + len(missing_pairs)
                else:
                    job["failed_tracks"] = int(job.get("failed_tracks") or 0) + len(missing_pairs)
                    brief = (job.get("last_error") or "Batch failed before reporting per-track results").strip()
                    for artist, title in missing_pairs:
                        job.setdefault("failed_tracks_list", []).append(
                            {"artist": artist, "title": title, "error": brief[:200]}
                        )

            accounted = int(job.get("completed_tracks") or 0) + int(job.get("failed_tracks") or 0)
            if accounted > total_tracks:
                overflow = accounted - total_tracks
                job["completed_tracks"] = max(0, int(job.get("completed_tracks") or 0) - overflow)

            job["current_index"] = min(
                total_tracks,
                int(job.get("completed_tracks") or 0) + int(job.get("failed_tracks") or 0),
            )

        # Invalidate the filesystem library index so the next import sees newly downloaded files.
        _library_index_cache["ts"] = 0.0

        if timed_out:
            with _download_lock:
                job["last_error"] = f"Batch timed out after {total_timeout}s"

    except Exception as e:
        with _download_lock:
            job["last_error"] = str(e)
    finally:
        if evaluated_path:
            try:
                os.unlink(evaluated_path)
            except Exception:
                pass
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass


def _download_worker_loop() -> None:
    """Sequentially process download jobs."""
    from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed

    while True:
        with _download_lock:
            if not _download_queue:
                return
            job_id = _download_queue.pop(0)
            job = download_status.get(job_id)
            if not job:
                continue
            job["status"] = "running"
            job["started_at"] = time.time()
            job["finished_at"] = None
            job["message"] = "Starting…"

        tracks: list[dict[str, str]] = job.get("tracks") or []

        batch_tracks = [
            {"artist": (t.get("artist") or "").strip(), "title": (t.get("title") or "").strip()}
            for t in tracks
            if (t.get("artist") or "").strip() and (t.get("title") or "").strip()
        ]
        _execute_batch_download(job, batch_tracks)
        any_failed = bool(job.get("failed_tracks"))

        with _download_lock:
            job["finished_at"] = time.time()
            job["current_track"] = None
            job["message"] = "Done"
            if any_failed:
                completed = int(job.get("completed_tracks") or 0)
                job["status"] = "partial" if completed > 0 else "failed"
            else:
                job["status"] = "completed"

        if str(job.get("source") or "") == "artist_monitor":
            monitor_user_key = str(job.get("monitor_user_key") or "").strip()
            monitor_artist_key = str(job.get("monitor_artist_key") or "").strip()
            monitor_release_key = str(job.get("monitor_release_key") or "").strip()
            monitor_release_name = str(job.get("title") or "").strip()
            final_status = str(job.get("status") or "").strip().lower()
            if final_status == "completed":
                _mark_release_seen_for_user(
                    monitor_user_key,
                    monitor_artist_key,
                    monitor_release_key,
                    monitor_release_name,
                    int(job.get("monitor_release_published_ts") or 0),
                )
            elif final_status in {"failed", "partial"}:
                _record_monitored_release_attempt(
                    monitor_user_key,
                    monitor_artist_key,
                    monitor_release_key,
                    monitor_release_name,
                    final_status,
                )

        try:
            _save_history_entry(job)
        except Exception as _he:
            logger.warning("Failed to save download history: %s", _he)

        with _download_lock:
            try:
                finished = [
                    (jid, j)
                    for jid, j in download_status.items()
                    if j.get("status") in {"completed", "failed", "partial"}
                ]
                if len(finished) > 50:
                    finished.sort(key=lambda kv: float(kv[1].get("finished_at") or kv[1].get("created_at") or 0))
                    for jid, _ in finished[: len(finished) - 50]:
                        download_status.pop(jid, None)
            except Exception:
                pass

            try:
                if not _download_queue and not any(
                    (j.get("status") in {"queued", "running"}) for j in download_status.values()
                ):
                    download_status.clear()
            except Exception:
                pass

        plex_pl_name = (job.get("plex_playlist_name") or "").strip()
        plex_pl_tracks = job.get("plex_playlist_all_tracks") or []
        plex_baseurl_for_pl = (job.get("plex_baseurl") or PLEX_BASEURL or "").strip()
        if plex_pl_name and plex_pl_tracks and plex_baseurl_for_pl:
            try:
                with _download_lock:
                    job["plex_playlist_status"] = "building"
                plex_token = (job.get("plex_token") or "").strip()
                if plex_token:
                    pc = _plex_client_cached(plex_baseurl_for_pl, plex_token, _plex_music_library_name())
                    pc.update_library()
                    n_downloaded = int(job.get("completed_tracks") or 0)
                    index_delay = min(5 + 3 * n_downloaded, 120)
                    logger.info("Waiting %ds for Plex to index %d new track(s)…", index_delay, n_downloaded)
                    time.sleep(index_delay)
                    from .models import Track as _Track
                    plex_items = []
                    for ref in plex_pl_tracks:
                        try:
                            item = pc.find_track(_Track(title=ref["title"], artist=ref["artist"]))
                            if item:
                                plex_items.append(item)
                        except Exception:
                            pass
                    if plex_items:
                        _pl_cover = job.get("plex_playlist_cover_url") or ""
                        pc.upsert_playlist(plex_pl_name, plex_items, cover_url=_pl_cover)
                        with _download_lock:
                            job["plex_playlist_status"] = f"created:{len(plex_items)}"
                    else:
                        with _download_lock:
                            job["plex_playlist_status"] = "no_tracks_found"
                else:
                    with _download_lock:
                        job["plex_playlist_status"] = "no_plex_token"
            except Exception as _pl_err:
                logger.error(f"Plex playlist creation error: {_pl_err}")
                with _download_lock:
                    job["plex_playlist_status"] = f"error:{_pl_err}"


_artist_monitor_worker: threading.Thread | None = None
_artist_monitor_state: dict[str, object] = {
    "running": False,
    "started_at": None,
    "last_run_at": None,
    "last_error": None,
}
_artist_monitor_lock = threading.Lock()


def _ensure_artist_monitor_worker() -> None:
    global _artist_monitor_worker
    with _artist_monitor_lock:
        if _artist_monitor_worker and _artist_monitor_worker.is_alive():
            return
        _artist_monitor_worker = threading.Thread(target=_artist_monitor_loop, daemon=True)
        _artist_monitor_worker.start()


def _monitored_user_snapshots() -> list[dict]:
    snapshots: list[dict] = []
    with _user_store_lock:
        store = _load_user_store()

    if not isinstance(store, dict):
        return snapshots

    for user_key, raw_user in store.items():
        if not isinstance(raw_user, dict):
            continue
        monitors = _clean_monitored_artist_list(raw_user.get("monitored_artists") or [])
        if not monitors:
            continue
        plex = raw_user.get("plex") if isinstance(raw_user.get("plex"), dict) else {}
        username = (
            (plex.get("username") or plex.get("title") or user_key or "").strip().lower()
            if isinstance(plex, dict)
            else str(user_key or "").strip().lower()
        )
        snapshots.append(
            {
                "user_key": str(user_key or "").strip(),
                "submitted_by": username,
                "monitors": monitors,
            }
        )
    return snapshots


def _mark_release_seen_for_user(user_key: str, artist_key: str, release_key: str, release_name: str, published_ts: int) -> None:
    with _user_store_lock:
        store = _load_user_store()
        user = store.get(user_key)
        if not isinstance(user, dict):
            return
        monitors = _clean_monitored_artist_list(user.get("monitored_artists") or [])
        updated = False
        for item in monitors:
            if item.get("artist_key") != artist_key:
                continue
            seen_keys = _clean_seen_release_keys(item.get("seen_release_keys") or [])
            if release_key and release_key not in seen_keys:
                seen_keys.insert(0, release_key)
            item["seen_release_keys"] = seen_keys[:_ARTIST_MONITOR_MAX_SEEN_RELEASES]
            retry_state = _clean_release_retry_state(item.get("release_retry_state") or {})
            if release_key:
                retry_state.pop(release_key, None)
            item["release_retry_state"] = retry_state
            item["last_checked_at"] = int(time.time())
            item["last_error"] = ""
            item["last_seen_release_key"] = release_key
            item["last_seen_release_name"] = release_name
            item["last_seen_release_ts"] = int(published_ts or 0)
            updated = True
            break
        if not updated:
            return
        user["monitored_artists"] = monitors
        store[user_key] = user
        _save_user_store(store)


def _set_monitored_artist_error(user_key: str, artist_key: str, error: str) -> None:
    with _user_store_lock:
        store = _load_user_store()
        user = store.get(user_key)
        if not isinstance(user, dict):
            return
        monitors = _clean_monitored_artist_list(user.get("monitored_artists") or [])
        updated = False
        for item in monitors:
            if item.get("artist_key") != artist_key:
                continue
            item["last_checked_at"] = int(time.time())
            item["last_error"] = str(error or "").strip()[:300]
            updated = True
            break
        if not updated:
            return
        user["monitored_artists"] = monitors
        store[user_key] = user
        _save_user_store(store)


def _record_monitored_release_attempt(
    user_key: str,
    artist_key: str,
    release_key: str,
    release_name: str,
    status: str,
) -> None:
    if not user_key or not artist_key or not release_key:
        return

    now_ts = int(time.time())
    with _user_store_lock:
        store = _load_user_store()
        user = store.get(user_key)
        if not isinstance(user, dict):
            return
        monitors = _clean_monitored_artist_list(user.get("monitored_artists") or [])
        updated = False
        for item in monitors:
            if item.get("artist_key") != artist_key:
                continue
            retry_state = _clean_release_retry_state(item.get("release_retry_state") or {})
            current = retry_state.get(release_key) if isinstance(retry_state.get(release_key), dict) else {}
            try:
                previous_failures = int(current.get("consecutive_failures") or 0)
            except Exception:
                previous_failures = 0
            consecutive_failures = previous_failures + 1
            cooldown_until = int(current.get("cooldown_until") or 0) if isinstance(current, dict) else 0
            if consecutive_failures >= _ARTIST_MONITOR_RETRY_FAILURE_LIMIT:
                cooldown_until = now_ts + _ARTIST_MONITOR_RETRY_COOLDOWN_S
                consecutive_failures = 0
            retry_state[release_key] = {
                "consecutive_failures": consecutive_failures,
                "cooldown_until": cooldown_until,
                "last_attempted_at": now_ts,
                "last_status": str(status or "").strip(),
            }
            item["release_retry_state"] = retry_state
            item["last_checked_at"] = now_ts
            item["last_error"] = f"{release_name or 'Release'} download {status or 'failed'}"
            updated = True
            break
        if not updated:
            return
        user["monitored_artists"] = monitors
        store[user_key] = user
        _save_user_store(store)


def _release_retry_state_for_entry(entry: dict, release_key: str) -> dict[str, int | str]:
    retry_state = _clean_release_retry_state(entry.get("release_retry_state") or {})
    current = retry_state.get(release_key)
    return current if isinstance(current, dict) else {}


def _artist_monitor_loop() -> None:
    while True:
        with _artist_monitor_lock:
            _artist_monitor_state["running"] = True
            _artist_monitor_state["started_at"] = _artist_monitor_state.get("started_at") or time.time()

        try:
            snapshots = _monitored_user_snapshots()
            for snapshot in snapshots:
                user_key = snapshot.get("user_key") or ""
                submitted_by = snapshot.get("submitted_by") or ""
                monitors = snapshot.get("monitors") or []

                for entry in monitors:
                    artist = str(entry.get("artist") or "").strip()
                    artist_key = str(entry.get("artist_key") or "").strip()
                    if not artist or not artist_key:
                        continue

                    try:
                        releases = _get_artist_release_candidates(artist, ARTIST_MONITOR_RELEASE_SCAN_LIMIT)
                        if not releases:
                            _set_monitored_artist_error(user_key, artist_key, "")
                            continue

                        unseen_releases: list[dict] = []
                        seen_keys = set(_clean_seen_release_keys(entry.get("seen_release_keys") or []))
                        for release in releases:
                            release_key = _artist_release_key(release)
                            if not release_key:
                                continue
                            if release_key in seen_keys:
                                break
                            unseen_releases.append(release)

                        if not unseen_releases:
                            newest = releases[0]
                            _mark_release_seen_for_user(
                                user_key,
                                artist_key,
                                _artist_release_key(newest),
                                str(newest.get("album") or "").strip(),
                                int(newest.get("published_ts") or 0),
                            )
                            continue

                        for release in reversed(unseen_releases):
                            album_name = str(release.get("album") or "").strip()
                            release_key = _artist_release_key(release)
                            if not album_name or not release_key:
                                continue

                            retry_state = _release_retry_state_for_entry(entry, release_key)
                            try:
                                cooldown_until = int(retry_state.get("cooldown_until") or 0)
                            except Exception:
                                cooldown_until = 0
                            if cooldown_until > int(time.time()):
                                continue

                            if _album_in_library(artist, album_name):
                                _mark_release_seen_for_user(
                                    user_key,
                                    artist_key,
                                    release_key,
                                    album_name,
                                    int(release.get("published_ts") or 0),
                                )
                                continue

                            if _has_active_download_job(submitted_by, artist, album_name, "album"):
                                continue

                            result = _enqueue_download_job(
                                artist,
                                album_name,
                                "album",
                                submitted_by,
                                source="artist_monitor",
                                extra_fields={
                                    "monitor_user_key": user_key,
                                    "monitor_artist": artist,
                                    "monitor_artist_key": artist_key,
                                    "monitor_release_key": release_key,
                                    "monitor_release_published_ts": int(release.get("published_ts") or 0),
                                },
                            )

                            if bool(result.get("already_in_library")):
                                _mark_release_seen_for_user(
                                    user_key,
                                    artist_key,
                                    release_key,
                                    album_name,
                                    int(release.get("published_ts") or 0),
                                )

                        _set_monitored_artist_error(user_key, artist_key, "")
                    except Exception as exc:
                        logger.warning("Artist monitor check failed for %s: %s", artist, exc)
                        _set_monitored_artist_error(user_key, artist_key, str(exc))

            with _artist_monitor_lock:
                _artist_monitor_state["last_run_at"] = time.time()
                _artist_monitor_state["last_error"] = None
        except Exception as exc:
            logger.warning("Artist monitor loop failed: %s", exc)
            with _artist_monitor_lock:
                _artist_monitor_state["last_error"] = str(exc)

        time.sleep(ARTIST_MONITOR_INTERVAL_S)


# Very small in-memory image cache: url -> (ts, bytes, content_type)
_image_cache: dict[str, tuple[float, bytes, str]] = {}
_IMAGE_CACHE_MAX = 256
_IMAGE_CACHE_TTL_SECONDS = 60 * 60 * 24


def _is_allowed_image_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return False
        host = (parsed.hostname or "").lower()
        if not host:
            return False

        # Lock this down to known image hosts we use.
        # Last.fm common hosts: lastfm.freetls.fastly.net, userserve-ak.last.fm
        if host.endswith("last.fm"):
            return True
        if host.endswith("fastly.net") and "lastfm" in host:
            return True
        # Older/alternate Last.fm CDN hosts
        if host.endswith("akamaized.net") and "lastfm" in host:
            return True

        # Spotify images are typically served from i.scdn.co (and related scdn hosts)
        # Algorithmic/radio playlist covers come from pickasso.spotifycdn.com
        if host.endswith("scdn.co"):
            return True
        if host.endswith("spotifycdn.com"):
            return True

        return False
    except Exception:
        return False


@app.route("/")
def index():
    """Serve the main web UI page."""
    return render_template("index.html")


@app.route("/api/image", methods=["GET"])
def proxy_image():
    """Proxy an allowed remote image URL through this server.

    Query params:
        u: full image URL
    """
    url = request.args.get("u", "").strip()
    if not url:
        return jsonify({"error": "Missing u parameter"}), 400
    if not _is_allowed_image_url(url):
        return jsonify({"error": "URL not allowed"}), 400

    now = time.time()
    cached = _image_cache.get(url)
    if cached:
        ts, body, content_type = cached
        if now - ts < _IMAGE_CACHE_TTL_SECONDS:
            return Response(body, content_type=content_type, headers={"Cache-Control": "public, max-age=86400"})
        else:
            _image_cache.pop(url, None)

    try:
        resp = requests.get(
            url,
            timeout=10,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
                "Referer": "https://open.spotify.com/",
                "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
            },
        )
        if resp.status_code != 200:
            return jsonify({"error": f"Upstream returned {resp.status_code}"}), 502

        body = resp.content
        content_type = resp.headers.get("Content-Type", "image/jpeg")

        # Basic cache eviction
        if len(_image_cache) >= _IMAGE_CACHE_MAX:
            # Drop oldest ~25%
            oldest = sorted(_image_cache.items(), key=lambda kv: kv[1][0])[: max(1, _IMAGE_CACHE_MAX // 4)]
            for k, _ in oldest:
                _image_cache.pop(k, None)

        _image_cache[url] = (now, body, content_type)
        return Response(body, content_type=content_type, headers={"Cache-Control": "public, max-age=86400"})

    except Exception as e:
        logger.warning(f"Image proxy error: {e}")
        return jsonify({"error": "Failed to fetch image"}), 502


@app.route("/api/preview", methods=["GET"])
def preview():
    """Get a 30-second preview URL for a track.

    Query params:
        artist: artist name
        title: track title

    Response:
        {"preview_url": "https://...", "provider": "spotify"|"itunes"}
    """
    artist = request.args.get("artist", "").strip()
    title = request.args.get("title", "").strip()

    if not artist or not title:
        return jsonify({"error": "Missing artist or title"}), 400

    # 1) Spotify (preferred) if configured
    try:
        spotify_client_id, spotify_client_secret = _get_spotify_credentials()
        if spotify_client_id and spotify_client_secret:
            spotify = SpotifyClient(client_id=spotify_client_id, client_secret=spotify_client_secret)
            info = spotify.get_track_preview(artist, title)
            if info and info.get("preview_url"):
                return jsonify({"preview_url": info["preview_url"], "provider": "spotify"})
    except Exception as e:
        logger.debug(f"Spotify preview lookup failed: {e}")

    # 2) iTunes fallback (no creds)
    url = _itunes_preview(artist, title)
    if url:
        return jsonify({"preview_url": url, "provider": "itunes"})

    return jsonify({"error": "No preview available"}), 404


@app.route("/api/search", methods=["GET"])
def search():
    """Search for tracks, albums, or artists.
    
    Query params:
        q: search query
        type: track, album, or artist (default: track)
        limit: number of results (default: 20)
    """
    query = request.args.get("q", "").strip()
    search_type = request.args.get("type", "track")
    limit = int(request.args.get("limit", 20))
    
    if not query:
        return jsonify({"error": "Missing query parameter"}), 400
    
    try:
        items: list[dict] = []

        provider = SEARCH_PROVIDER
        if provider not in {"lastfm", "spotify", "auto"}:
            provider = "lastfm"

        def _search_lastfm() -> list[dict]:
            if not LASTFM_API_KEY:
                return []
            lastfm = LastFmClient(api_key=LASTFM_API_KEY, username=LASTFM_USERNAME)
            if search_type == "track":
                return lastfm.search_tracks(query, limit=limit)
            if search_type == "album":
                return lastfm.search_albums(query, limit=limit)
            if search_type == "artist":
                return lastfm.search_artists(query, limit=limit)
            return lastfm.search_tracks(query, limit=limit)

        def _search_spotify() -> list[dict]:
            spotify_client_id, spotify_client_secret = _get_spotify_credentials()
            if not (spotify_client_id and spotify_client_secret):
                return []
            spotify = SpotifyClient(client_id=spotify_client_id, client_secret=spotify_client_secret)
            return spotify.search(query, search_type, limit=limit)

        if provider == "lastfm":
            items = _search_lastfm()
        elif provider == "spotify":
            items = _search_spotify()
        else:
            # auto: try spotify then lastfm
            items = _search_spotify() or _search_lastfm()

        if not items:
            return jsonify(
                {
                    "error": "No results (or provider not configured). Ensure LASTFM_API_KEY is set for Last.fm search.",
                    "provider": provider,
                }
            ), 500
        
        # Add library status badges where possible
        try:
            _annotate_in_library(items, search_type)
        except Exception:
            pass

        results = {
            "query": query,
            "type": search_type,
            "items": items
        }
        
        return jsonify(results)
        
    except Exception as e:
        logger.error(f"Search error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/recommendations", methods=["GET"])
def recommendations():
    """Return a feed of Last.fm recommendations for the home (Search) view.

    Query params:
        limit: number of tracks (default: 60)
        bust:  if truthy, bypass the server-side cache and rebuild the feed.

    Response:
        {"items": [ {"name": str, "artist": str, "type": "track", "cover_url": str, "in_library": bool} ... ]}
    """
    bust_cache = bool(request.args.get("bust", ""))
    try:
        limit = int(request.args.get("limit", 60))
    except Exception:
        limit = 60
    # The Web UI can show a larger feed; cap to keep latency sane.
    limit = max(1, min(limit, 200))

    if not LASTFM_API_KEY:
        return jsonify({"error": "LASTFM_API_KEY is required"}), 500

    # Recommendations are per-user: require a linked Last.fm username.
    username = _get_linked_lastfm_username()
    if not username:
        return (
            jsonify(
                {
                    "error": "lastfm_not_linked",
                    "message": "Link your Last.fm account in Settings first.",
                }
            ),
            400,
        )

    # Serve from cache unless the caller explicitly busts it.
    _rec_cache_key = username.lower().strip()
    if not bust_cache:
        _rc = _recommendations_cache.get(_rec_cache_key)
        if _rc and (time.time() - _rc[0]) < _RECOMMENDATIONS_CACHE_TTL_SECONDS:
            return jsonify({"items": _rc[1], "cached": True})

    seed_count = 25
    similar_per_seed = 5
    try:
        s = load_settings()
        seed_count = int(getattr(s, "lastfm_seed_count", seed_count) or seed_count)
        similar_per_seed = int(getattr(s, "lastfm_similar_per_seed", similar_per_seed) or similar_per_seed)
    except Exception:
        pass

    lastfm = LastFmClient(api_key=LASTFM_API_KEY, username=username)
    # Increase the candidate pool when the caller requests a large feed.
    # This keeps "up to N" achievable even after de-dupe and library-owned overlap.
    seed_count = max(1, int(seed_count))
    similar_per_seed = max(1, int(similar_per_seed))
    pool_target = min(800, limit * 3)
    min_per_seed = int((pool_target + seed_count - 1) // seed_count)
    similar_per_seed = max(similar_per_seed, min_per_seed)
    # Last.fm caps similar tracks per seed; keep within reason.
    similar_per_seed = max(1, min(similar_per_seed, 100))

    tracks = lastfm.get_recommended_tracks(
        max_tracks=limit,
        seed_count=seed_count,
        similar_per_seed=similar_per_seed,
    )

    # De-dupe preserving order
    seen: set[tuple[str, str]] = set()
    items: list[dict] = []
    for tr in tracks:
        try:
            a = (tr.artist or "").strip()
            t = (tr.title or "").strip()
        except Exception:
            continue
        if not a or not t:
            continue
        k = (_norm_artist(a), _norm_track_title(t))
        if k in seen:
            continue
        seen.add(k)
        in_lib = False
        try:
            in_lib = _track_in_library(a, t)
        except Exception:
            in_lib = False
        items.append({"name": t, "artist": a, "type": "track", "cover_url": "", "in_library": in_lib})

    # Sort so new (not in library) tracks are shown first.
    # Python sort is stable, so relative ordering within each group stays consistent.
    try:
        items.sort(key=lambda it: bool(it.get("in_library")))
    except Exception:
        pass

    # Enrich a small number of covers up-front (best-effort).
    # The UI lazily loads the rest as cards scroll into view.
    enrich_count = min(len(items), 30)

    def _enrich_one(idx: int) -> tuple[int, str, str]:
        it = items[idx]
        # Use the cached variant so repeated visits don't re-hit the Last.fm API.
        url, album_name = _cached_lastfm_track_cover_info(it.get("artist", ""), it.get("name", ""))
        return idx, url, album_name

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
            futures = [ex.submit(_enrich_one, i) for i in range(enrich_count)]
            for fut in concurrent.futures.as_completed(futures):
                try:
                    i, url, album_name = fut.result()
                    if url:
                        items[i]["cover_url"] = url
                    # Store album name so propagation can key by (artist, album).
                    if album_name:
                        items[i]["_lastfm_album"] = album_name
                except Exception:
                    continue
    except Exception:
        pass

    # Propagate covers to unenriched tracks from the same album.
    # Key by (artist, album) so tracks from a different album by the same artist
    # are never given the wrong cover.
    _album_cover: dict[tuple[str, str], str] = {}
    for it in items:
        if it.get("cover_url") and it.get("_lastfm_album"):
            ak = (_norm_artist(it.get("artist", "")), it["_lastfm_album"].lower().strip())
            if ak not in _album_cover:
                _album_cover[ak] = it["cover_url"]
    for it in items:
        if not it.get("cover_url") and it.get("_lastfm_album"):
            ak = (_norm_artist(it.get("artist", "")), it["_lastfm_album"].lower().strip())
            if ak in _album_cover:
                it["cover_url"] = _album_cover[ak]

    # Strip internal field before returning to the client.
    for it in items:
        it.pop("_lastfm_album", None)

    # Store in cache for subsequent requests.
    try:
        _recommendations_cache[_rec_cache_key] = (time.time(), items)
    except Exception:
        pass

    return jsonify({"items": items})


@app.route("/api/lastfm/track_cover", methods=["GET"])
def lastfm_track_cover():
    """Return a best-effort Last.fm album-art URL for a track.

    This is used by the WebUI to lazily hydrate cover thumbnails as items come
    into view.

    Query params:
        artist: track artist (required)
        title: track title (required)

    Response:
        {"cover_url": str, "proxy_url": str}
    """
    artist = (request.args.get("artist") or "").strip()
    title = (request.args.get("title") or "").strip()
    if not artist or not title:
        return jsonify({"cover_url": "", "proxy_url": ""})

    url, album_name = _cached_lastfm_track_cover_info(artist, title)
    if not url:
        return jsonify({"cover_url": "", "proxy_url": "", "album": ""})

    proxy = f"/api/image?u={quote(url, safe='')}"
    return jsonify({"cover_url": url, "proxy_url": proxy, "album": album_name})


@app.route("/api/artist/albums", methods=["GET"])
def artist_albums():
    """Get an artist's albums.

    Query params:
        artist: artist name (required)
        limit: number of albums (default: 50)
    """
    artist = request.args.get("artist", "").strip()
    limit = int(request.args.get("limit", 50))

    if not artist:
        return jsonify({"error": "Missing artist parameter"}), 400
    if not LASTFM_API_KEY:
        return jsonify({"error": "LASTFM_API_KEY is required"}), 500

    try:
        lastfm = LastFmClient(api_key=LASTFM_API_KEY, username=LASTFM_USERNAME)
        albums = lastfm.get_artist_albums(artist, limit=limit)
        try:
            _annotate_in_library(albums, "album")
        except Exception:
            pass
        return jsonify({"artist": artist, "items": albums})
    except Exception as e:
        logger.error(f"Artist albums error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/artist/top_tracks", methods=["GET"])
def artist_top_tracks():
    """Get an artist's most popular tracks (Last.fm Top Tracks).

    Query params:
        artist: artist name (required)
        limit: number of tracks (default: 10)
    """
    artist = (request.args.get("artist") or "").strip()
    try:
        limit = int(request.args.get("limit", 10))
    except Exception:
        limit = 10
    limit = max(1, min(limit, 30))

    if not artist:
        return jsonify({"error": "Missing artist parameter"}), 400
    if not LASTFM_API_KEY:
        return jsonify({"error": "LASTFM_API_KEY is required"}), 500

    lastfm = LastFmClient(api_key=LASTFM_API_KEY, username=LASTFM_USERNAME)
    pairs = lastfm.get_artist_top_tracks(artist, limit=limit) or []

    items: list[dict] = []
    for idx, (a, t) in enumerate(pairs, start=1):
        if not a or not t:
            continue
        try:
            in_lib = _track_in_library(a, t)
        except Exception:
            in_lib = False
        items.append(
            {
                "name": t,
                "artist": a,
                "type": "track",
                "rank": idx,
                "cover_url": "",
                "in_library": in_lib,
            }
        )

    return jsonify({"artist": artist, "items": items})


@app.route("/api/artist/monitor/status", methods=["GET"])
def artist_monitor_status():
    artist = (request.args.get("artist") or "").strip()
    if not artist:
        return jsonify({"error": "Missing artist parameter"}), 400

    key = _plex_user_key()
    if not key:
        return jsonify({"error": "Not authenticated"}), 401

    entry = _get_monitored_artist_entry_for_key(key, artist)
    return jsonify(
        {
            "artist": artist,
            "monitored": bool(entry),
            "last_seen_release_name": (entry or {}).get("last_seen_release_name") or "",
            "last_checked_at": int((entry or {}).get("last_checked_at") or 0),
            "last_error": (entry or {}).get("last_error") or "",
        }
    )


@app.route("/api/artist/monitor/toggle", methods=["POST"])
def artist_monitor_toggle():
    data = request.json or {}
    artist = (data.get("artist") or "").strip()
    monitored = bool(data.get("monitored"))
    if not artist:
        return jsonify({"error": "Missing artist"}), 400

    try:
        result = _set_artist_monitoring(artist, monitored)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 401
    except Exception as exc:
        logger.error("Artist monitor toggle error: %s", exc)
        return jsonify({"error": str(exc)}), 500

    if monitored:
        _ensure_artist_monitor_worker()
    return jsonify(result)


@app.route("/api/artist/new_release", methods=["GET"])
def artist_new_release():
    """Get the newest release (best-effort) for an artist as a track list.

    This approximates Spotify's "New releases" section by choosing the newest
    album using Last.fm album wiki published date when available.

    Query params:
        artist: artist name (required)
        album_limit: how many candidate albums to inspect (default: 8)
    """
    artist = (request.args.get("artist") or "").strip()
    try:
        album_limit = int(request.args.get("album_limit", 8))
    except Exception:
        album_limit = 8
    album_limit = max(1, min(album_limit, 15))

    if not artist:
        return jsonify({"error": "Missing artist parameter"}), 400
    if not LASTFM_API_KEY:
        return jsonify({"error": "LASTFM_API_KEY is required"}), 500

    infos = _get_artist_release_candidates(artist, album_limit)
    if not infos:
        return jsonify({"artist": artist, "album": "", "cover_url": "", "in_library": False, "items": []})

    chosen = infos[0]

    album_name = (chosen.get("album") or "").strip()
    cover_url = (chosen.get("cover_url") or "").strip()
    tracks_in = chosen.get("tracks", []) or []

    items: list[dict] = []
    for t in tracks_in:
        try:
            title = (t.get("name") or "").strip()
            a = (t.get("artist") or artist).strip()
            rank = int(t.get("rank") or 0) if isinstance(t, dict) else 0
        except Exception:
            continue
        if not title:
            continue
        try:
            in_lib = _track_in_library(a, title)
        except Exception:
            in_lib = False
        items.append(
            {
                "name": title,
                "artist": a,
                "type": "track",
                "rank": rank,
                "cover_url": cover_url,
                "album": {"name": album_name, "cover_url": cover_url},
                "in_library": in_lib,
            }
        )

    # Best-effort album in-library: all tracks present.
    album_in_lib = bool(items) and all(bool(t.get("in_library")) for t in items)

    return jsonify({"artist": artist, "album": album_name, "cover_url": cover_url, "in_library": album_in_lib, "items": items})


@app.route("/api/album/tracks", methods=["GET"])
def album_tracks():
    """Get an album's tracks.

    Query params:
        artist: artist name (required)
        album: album name (required)
    """
    artist = request.args.get("artist", "").strip()
    album = request.args.get("album", "").strip()

    if not artist or not album:
        return jsonify({"error": "Missing artist or album parameter"}), 400

    try:
        cover_url = ""
        tracks_out: list[dict] = []

        # Preferred: Last.fm tracklist (lets us show missing tracks vs expected)
        if LASTFM_API_KEY:
            try:
                lastfm = LastFmClient(api_key=LASTFM_API_KEY, username=LASTFM_USERNAME)
                ca = _clean_lookup_text(artist)
                cal = _strip_redundant_artist_prefix(ca, album)
                # Try a couple of variants to handle folder naming artifacts.
                info = lastfm.get_album_tracks_detailed(artist, album)
                if not (info.get("tracks") or []):
                    info = lastfm.get_album_tracks_detailed(ca or artist, cal or album)
            except Exception:
                info = {}

            cover_url = (info.get("cover_url") or "").strip()
            tracks_in = info.get("tracks", []) or []
            for t in tracks_in:
                name = t.get("name")
                t_artist = t.get("artist") or artist
                if not name:
                    continue
                try:
                    in_lib = _track_in_library(t_artist, name)
                except Exception:
                    in_lib = False
                tracks_out.append(
                    {
                        "name": name,
                        "artist": t_artist,
                        "type": "track",
                        "cover_url": cover_url,
                        "album": {"name": album, "cover_url": cover_url},
                        "in_library": in_lib,
                    }
                )

        # Fallback: show what's actually on disk (works without Last.fm)
        if not tracks_out:
            album_dir = (MUSIC_LIBRARY_PATH / artist / album)
            if album_dir.exists() and album_dir.is_dir():
                audio_exts = {".flac", ".mp3", ".m4a", ".aac", ".ogg", ".opus", ".wav", ".aiff", ".alac"}
                stems: list[str] = []
                for p in album_dir.rglob("*"):
                    try:
                        if not p.is_file() or p.suffix.lower() not in audio_exts:
                            continue
                        stems.append(p.stem)
                    except Exception:
                        continue
                stems.sort(key=lambda s: _norm_text(s))
                for i, stem in enumerate(stems):
                    title_guess = _strip_track_number(stem)
                    tracks_out.append(
                        {
                            "name": title_guess or stem,
                            "artist": artist,
                            "type": "track",
                            "cover_url": cover_url,
                            "album": {"name": album, "cover_url": cover_url},
                            "in_library": True,
                            "rank": i + 1,
                        }
                    )

        if tracks_out:
            # If we have an expected tracklist (Last.fm), this indicates completeness.
            # If we're using local fallback, consider it "in library" but missing is unknown.
            if LASTFM_API_KEY:
                album_in_lib = all(bool(t.get("in_library")) for t in tracks_out)
            else:
                album_in_lib = True
        else:
            try:
                album_in_lib = _album_in_library(artist, album)
            except Exception:
                album_in_lib = False

        return jsonify({"artist": artist, "album": album, "cover_url": cover_url, "in_library": album_in_lib, "items": tracks_out})
    except Exception as e:
        logger.error(f"Album tracks error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/spotify/resolve", methods=["POST"])
def spotify_resolve():
    """Resolve a Spotify share URL (track/album/playlist) into a track list.

    JSON body:
        url: Spotify share URL (https://open.spotify.com/... or spotify:type:id)

    Returns:
        type: "track" | "album" | "playlist"
        id: Spotify ID
        name: album/playlist/track title
        artist: primary artist (tracks/albums)
        cover_url: cover art URL
        total: total track count
        tracks: list of {name, artist, cover_url, spotify_id, preview_url, rank, in_library}
    """
    data = request.json or {}
    url = (data.get("url") or "").strip()
    if not url:
        return jsonify({"error": "Missing url"}), 400

    # Use explicit credentials if configured, otherwise fall back to the
    # anonymous web-player token (no API account needed).
    cid, csec = _get_spotify_credentials()
    sp = SpotifyClient(client_id=cid, client_secret=csec)
    try:
        result = sp.resolve_url(url)
    except Exception as e:
        logger.error(f"Spotify resolve error: {e}")
        return jsonify({"error": str(e)}), 500

    if "error" in result:
        return jsonify(result), 400

    # Annotate each track with in_library status (parallel)
    tracks = result.get("tracks") or []

    def _check_lib(t: dict) -> None:
        try:
            t["in_library"] = _track_in_library(t.get("artist", ""), t.get("name", ""))
        except Exception:
            t["in_library"] = False

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(_check_lib, tracks))

    return jsonify(result)


@app.route("/api/import/download", methods=["POST"])
def import_download():
    """Queue a download job from a resolved Spotify import.

    JSON body:
        tracks: list of {artist, name, cover_url, ...} track objects
        playlist_name: optional name for the import batch / Plex playlist
        create_plex_playlist: bool — if true, create a Plex playlist after download
    """
    data = request.json or {}
    tracks_in = data.get("tracks") or []
    playlist_name = (data.get("playlist_name") or "").strip()
    create_plex_playlist = bool(data.get("create_plex_playlist"))

    if not tracks_in:
        return jsonify({"error": "No tracks provided"}), 400

    all_refs: list[dict] = []
    to_download: list[dict] = []
    n_skipped = 0

    for t in tracks_in:
        artist = (t.get("artist") or "").strip()
        title = (t.get("name") or t.get("title") or "").strip()
        if not artist or not title:
            continue
        # Use the primary (first) artist for Plex lookup — Plex stores tracks
        # under the primary artist folder, not comma-joined collaborators.
        plex_artist = _primary_artist(artist)
        all_refs.append({"artist": plex_artist, "title": title})
        if _track_in_library(artist, title):
            n_skipped += 1
        else:
            to_download.append({"artist": plex_artist, "title": title})

    job_id = f"import_{int(time.time())}_{secrets.token_hex(6)}"
    job: dict = {
        "id": job_id,
        "type": "import",
        "artist": "Spotify Import",
        "title": playlist_name or "Spotify Import",
        "status": "queued",
        "created_at": time.time(),
        "started_at": None,
        "finished_at": None,
        "message": "Queued",
        "tracks": to_download,
        "total_tracks": len(to_download),
        "completed_tracks": 0,
        "failed_tracks": 0,
        "failed_tracks_list": [],
        "current_index": 0,
        "current_track": None,
        "submitted_by": _session_username_lower(),
        "skipped": n_skipped,
        "skipped_duplicates": 0,
        "last_error": "",
        "last_output": "",
        "plex_token": (session.get("plex_token") or "").strip(),
        "plex_baseurl": (session.get("plex_baseurl") or PLEX_BASEURL or "").strip(),
        # Plex playlist fields
        "plex_playlist_name": playlist_name if create_plex_playlist else "",
        "plex_playlist_all_tracks": all_refs if create_plex_playlist else [],
        "plex_playlist_cover_url": (data.get("cover_url") or "").strip() if create_plex_playlist else "",
        "plex_playlist_status": "",
    }

    with _download_lock:
        download_status[job_id] = job
        _download_queue.append(job_id)
    _ensure_download_worker()

    return jsonify({
        "success": True,
        "message": "Import queued",
        "download_id": job_id,
        "total_tracks": len(to_download),
        "skipped": n_skipped,
    }), 202


@app.route("/api/download", methods=["POST"])
def download():
    """Download a track, album, or artist.
    
    JSON body:
        artist: artist name
        title: track title (for tracks)
        album: album name (optional)
        spotify_id: Spotify ID (optional)
        type: track, album, or artist
    """
    data = request.json
    
    if not data:
        return jsonify({"error": "Missing request body"}), 400
    
    artist = data.get("artist", "")
    title = data.get("title", "")
    dl_type = data.get("type", "track")
    # 'title' comes as the Album Name or Artist Name (from 'title' variable in JS) if type is album/artist
    # In JS: downloadItem(artist, name, type) -> name becomes 'title' in JSON.
    # So if type=album, title=AlbumName. If type=artist, artist=ArtistName, title=ArtistName (redundant but fine).
    
    if not artist:
        return jsonify({"error": "Missing artist field"}), 400
    
    try:
        result = _enqueue_download_job(artist, title, dl_type, _session_username_lower())
        if result.get("already_in_library"):
            return jsonify(result), 200
        logger.info("Queued download job %s: %s - %s (%s)", result.get("download_id"), artist, title, dl_type)
        return jsonify(result), 202
    except FileNotFoundError as exc:
        return jsonify({"error": str(exc)}), 404
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        logger.error("Download enqueue error: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/downloads", methods=["GET"])
def downloads_state():
    """Return the current download queue and live progress."""
    return jsonify(_downloads_snapshot())


@app.route("/api/status/<download_id>", methods=["GET"])
def get_status(download_id):
    """Get download status by ID (only accessible to the job owner)."""
    viewer = _session_username_lower()
    if download_id in download_status:
        job = download_status[download_id]
        owner = (job.get("submitted_by") or "").strip().lower()
        if not owner or owner == viewer:
            return jsonify(_job_view(job))
        return jsonify({"error": "Access denied"}), 403
    return jsonify({"error": "Download ID not found"}), 404


@app.route("/api/health", methods=["GET"])
def health():
    """Health check endpoint."""
    return jsonify({
        "status": "healthy",
        "music_library": str(MUSIC_LIBRARY_PATH),
        "service_order": ["tidal", "qobuz", "amazon"]
    })


def run_webui(host="0.0.0.0", port=5000):
    """Run the Flask web UI server."""
    try:
        # Quiet per-request access logs by default; the UI polls frequently.
        access_log = (os.environ.get("WEBUI_ACCESS_LOG", "0") or "0").strip().lower() in {
            "1",
            "true",
            "yes",
            "y",
            "on",
        }
        if not access_log:
            logging.getLogger("werkzeug").setLevel(logging.WARNING)
            logging.getLogger("werkzeug.serving").setLevel(logging.WARNING)
    except Exception:
        pass

    _ensure_artist_monitor_worker()
    logger.info(f"Starting Web UI on {host}:{port}")
    app.run(host=host, port=port, debug=False)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_webui()
