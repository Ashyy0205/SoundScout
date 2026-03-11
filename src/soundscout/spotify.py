from __future__ import annotations

import base64
import hashlib
import hmac
import json as _json
import logging
import re
import secrets
import struct
import threading
import time
from dataclasses import dataclass

import requests

logger = logging.getLogger(__name__)

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/134.0.0.0 Safari/537.36"
)


# ---------------------------------------------------------------------------
# TOTP + 3-step anonymous session
# (No API credentials needed — ported from the Go scraper)
# ---------------------------------------------------------------------------

_SPOTIFY_TOTP_SECRETS: dict[int, list[int]] = {
    59: [123, 105, 79, 70, 110, 59, 52, 125, 60, 49, 80, 70, 89, 75, 80, 86, 63, 53, 123, 37, 117, 49, 52, 93, 77, 62, 47, 86, 48, 104, 68, 72],
    60: [79, 109, 69, 123, 90, 65, 46, 74, 94, 34, 58, 48, 70, 71, 92, 85, 122, 63, 91, 64, 87, 87],
    61: [44, 55, 47, 42, 70, 40, 34, 114, 76, 74, 50, 111, 120, 97, 75, 76, 94, 102, 43, 69, 49, 120, 118, 80, 64, 78],
}
_SPOTIFY_TOTP_VERSION = 61


# ---------------------------------------------------------------------------
# PKCE helpers (used by the web-player OAuth flow)
# ---------------------------------------------------------------------------

def _pkce_verifier() -> str:
    """Generate a random PKCE code verifier (base64url, 43 chars)."""
    raw = secrets.token_bytes(32)
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _pkce_challenge(verifier: str) -> str:
    """Generate the PKCE code challenge (S256 method)."""
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def _spotify_totp() -> tuple[str, int]:
    """Generate a TOTP code using the same algorithm as the Go scraper."""
    version = _SPOTIFY_TOTP_VERSION
    raw = _SPOTIFY_TOTP_SECRETS[version]
    transformed = bytes([b ^ ((i % 33) + 9) for i, b in enumerate(raw)])
    joined = "".join(str(b) for b in transformed)
    raw_bytes = joined.encode("utf-8")
    secret_b32 = base64.b32encode(raw_bytes).rstrip(b"=").decode("utf-8")
    padded = secret_b32 + "=" * ((-len(secret_b32)) % 8)
    key_bytes = base64.b32decode(padded.upper())
    t = int(time.time()) // 30
    msg = struct.pack(">Q", t)
    h = hmac.new(key_bytes, msg, hashlib.sha1).digest()
    offset = h[-1] & 0x0F
    code = struct.unpack(">I", h[offset : offset + 4])[0]
    code = (code & 0x7FFF_FFFF) % 1_000_000
    return f"{code:06d}", version


@dataclass
class _AnonSession:
    """Full web-player session (access token + client token + version)."""
    access_token: str
    client_token: str
    client_version: str
    expires_at: float
    client_id: str = ""  # Spotify's web-player client ID (from /api/token response)


_anon_session_cache: _AnonSession | None = None
_anon_session_lock = threading.Lock()


def _init_anon_session() -> _AnonSession:
    """Perform the full 3-step Spotify web-player session init.

    Step 1 – GET open.spotify.com → extract clientVersion
    Step 2 – GET /api/token with TOTP → accessToken, clientId, deviceId (sp_t cookie)
    Step 3 – POST clienttoken.spotify.com → clientToken
    """
    session = requests.Session()
    session.headers.update({"User-Agent": _UA})

    # Step 1: clientVersion from homepage config blob
    client_version = "1.2.50.364.g1c2b7ba3"  # fallback
    try:
        r = session.get("https://open.spotify.com", timeout=15)
        r.raise_for_status()
        m = re.search(
            r'<script id="appServerConfig" type="text/plain">([^<]+)</script>',
            r.text,
        )
        if m:
            cfg = _json.loads(base64.b64decode(m.group(1) + "=="))
            client_version = cfg.get("clientVersion") or client_version
    except Exception as exc:
        logger.debug("Spotify: could not extract clientVersion: %s", exc)

    # Step 2: accessToken + clientId + sp_t (deviceId)
    totp_code, version = _spotify_totp()
    r2 = session.get(
        "https://open.spotify.com/api/token",
        params={
            "reason": "init",
            "productType": "web-player",
            "totp": totp_code,
            "totpVer": str(version),
            "totpServer": totp_code,
        },
        headers={"Accept": "application/json", "Referer": "https://open.spotify.com/"},
        timeout=15,
    )
    r2.raise_for_status()
    tok = r2.json()

    access_token = tok.get("accessToken")
    if not access_token:
        raise RuntimeError("Spotify: accessToken missing from /api/token response")

    client_id = tok.get("clientId") or ""
    device_id = session.cookies.get("sp_t") or ""
    exp_ms = tok.get("accessTokenExpirationTimestampMs")
    expires_at = float(exp_ms) / 1000.0 - 30 if exp_ms else time.time() + 3570

    # Step 3: clientToken
    client_token = ""
    try:
        r3 = session.post(
            "https://clienttoken.spotify.com/v1/clienttoken",
            json={
                "client_data": {
                    "client_version": client_version,
                    "client_id": client_id,
                    "js_sdk_data": {
                        "device_brand": "unknown",
                        "device_model": "unknown",
                        "os": "windows",
                        "os_version": "NT 10.0",
                        "device_id": device_id,
                        "device_type": "computer",
                    },
                }
            },
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            timeout=15,
        )
        r3.raise_for_status()
        granted = r3.json().get("granted_token") or {}
        client_token = granted.get("token") or ""
    except Exception as exc:
        logger.debug("Spotify: could not obtain clientToken: %s", exc)

    logger.debug("Spotify: anonymous session initialised (clientVersion=%s, clientId=%s)", client_version, client_id)
    return _AnonSession(
        access_token=access_token,
        client_token=client_token,
        client_version=client_version,
        expires_at=expires_at,
        client_id=client_id,
    )


def _get_anon_session() -> _AnonSession:
    global _anon_session_cache
    with _anon_session_lock:
        if _anon_session_cache and time.time() < _anon_session_cache.expires_at:
            return _anon_session_cache
        _anon_session_cache = _init_anon_session()
        return _anon_session_cache


def _get_webplayer_client_id() -> str:
    """Return Spotify's web-player client ID obtained from the TOTP anonymous session.

    Tokens issued under this client ID are accepted by all Spotify internal APIs
    (spclient, api-partner) without a developer-account Premium requirement.
    """
    return _get_anon_session().client_id


# ---------------------------------------------------------------------------
# Partner GraphQL helpers
# ---------------------------------------------------------------------------

def _partner_query(sess: _AnonSession, payload: dict) -> dict:
    """POST a persisted-query payload to the Spotify partner GraphQL endpoint."""
    headers: dict[str, str] = {
        "Authorization": f"Bearer {sess.access_token}",
        "Spotify-App-Version": sess.client_version,
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Referer": "https://open.spotify.com/",
        "User-Agent": _UA,
    }
    if sess.client_token:
        headers["Client-Token"] = sess.client_token
    resp = requests.post(
        "https://api-partner.spotify.com/pathfinder/v2/query",
        json=payload,
        headers=headers,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# spclient user-library helpers
# Uses spclient.wg.spotify.com which accepts any valid Spotify Bearer token
# (including developer-app PKCE tokens) — no developer Premium required.
# ---------------------------------------------------------------------------

_SPCLIENT_BASE = "https://spclient.wg.spotify.com"


def _spclient_headers(user_token: str) -> dict[str, str]:
    """Build the headers needed for spclient user-library requests."""
    anon = _get_anon_session()
    h: dict[str, str] = {
        "Authorization": f"Bearer {user_token}",
        "App-Platform": "WebPlayer",
        "Spotify-App-Version": anon.client_version,
        "User-Agent": _UA,
    }
    if anon.client_token:
        h["Client-Token"] = anon.client_token
    return h


def _spclient_get(user_token: str, url: str, params: dict | None = None) -> dict:
    """GET a spclient endpoint with user auth headers, return parsed JSON."""
    resp = requests.get(
        url,
        headers=_spclient_headers(user_token),
        params=params or {},
        timeout=20,
    )
    if not resp.ok:
        try:
            msg = resp.json()
        except Exception:
            msg = resp.text[:200]
        label = url.replace(_SPCLIENT_BASE, "spclient")
        raise requests.HTTPError(
            f"{resp.status_code} {resp.reason} on {label} — {msg}",
            response=resp,
        )
    return resp.json()


def _pg_str(d: dict, key: str) -> str:
    v = d.get(key)
    return str(v) if v is not None else ""


def _pg_list(d: dict, key: str) -> list:
    v = d.get(key)
    return v if isinstance(v, list) else []


def _pg_dict(d: dict, key: str) -> dict:
    v = d.get(key)
    return v if isinstance(v, dict) else {}


def _pg_cover(cover_art: dict) -> str:
    """Extract a usable cover URL from a coverArt {sources:[{url,width,height}]} dict."""
    sources = _pg_list(cover_art, "sources")
    best = ""
    best_w = -1
    fallback = ""
    for s in sources:
        if not isinstance(s, dict):
            continue
        url = _pg_str(s, "url")
        if not url:
            continue
        try:
            w = int(s.get("width") or s.get("maxWidth") or 0)
        except Exception:
            w = 0
        if w > 64 and w > best_w:
            best = url
            best_w = w
        if not fallback:
            fallback = url
    return best or fallback


def _pg_artists(artists_dict: dict) -> str:
    items = _pg_list(artists_dict, "items")
    names: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        name = _pg_str(_pg_dict(item, "profile"), "name")
        if name:
            names.append(name)
    return ", ".join(names)


def _extract_playlist_cover(pl: dict) -> str:
    """Try every known path Spotify uses for playlist cover art in the partner API."""

    # Path 1: imagesV2.items[n].sources  (most common in fetchPlaylist)
    for img_key in ("imagesV2", "images"):
        img_block = pl.get(img_key)
        if isinstance(img_block, dict):
            items = img_block.get("items") or []
            for itm in (items if isinstance(items, list) else []):
                if not isinstance(itm, dict):
                    continue
                srcs = itm.get("sources") or []
                url = _pg_cover({"sources": srcs}) if isinstance(srcs, list) else ""
                if url:
                    return url
        elif isinstance(img_block, list):
            # Path 2: images is a flat list of {url, width, height}
            url = _pg_cover({"sources": img_block})
            if url:
                return url

    # Path 3: coverArt.sources  (same shape as track/album)
    url = _pg_cover(_pg_dict(pl, "coverArt"))
    if url:
        return url

    # Path 4: visualIdentity.image.sources
    for vi_key in ("visualIdentity",):
        vi = pl.get(vi_key)
        if isinstance(vi, dict):
            img = vi.get("image") or vi.get("coverArt") or {}
            if isinstance(img, dict):
                url = _pg_cover(img)
                if url:
                    return url
            # sometimes visualIdentity itself has sources
            url = _pg_cover(vi)
            if url:
                return url

    # Path 5: any top-level key whose value contains a "sources" list
    for v in pl.values():
        if isinstance(v, dict) and "sources" in v:
            url = _pg_cover(v)
            if url:
                return url

    return ""


# ---------------------------------------------------------------------------

@dataclass
class _Token:
    access_token: str
    expires_at: float


class SpotifyClient:
    def __init__(self, client_id: str = "", client_secret: str = "") -> None:
        self._client_id = (client_id or "").strip()
        self._client_secret = (client_secret or "").strip()
        self._token: _Token | None = None

    @staticmethod
    def _pick_image_url(images: list[dict] | None, *, max_px: int = 420) -> str:
        """Pick an image URL no larger than max_px where possible.

        Spotify returns images ordered largest->smallest. We prefer the largest image
        with width <= max_px to avoid downloading oversized artwork.
        """
        if not images or not isinstance(images, list):
            return ""

        best_under = ""
        best_under_w = -1
        smallest = ""
        smallest_w = 10**9

        for img in images:
            try:
                url = (img.get("url") or "").strip()
                w = img.get("width")
                w = int(w) if w is not None else None
            except Exception:
                continue

            if not url:
                continue

            if w is not None and w > 0:
                if w <= max_px and w > best_under_w:
                    best_under = url
                    best_under_w = w
                if w < smallest_w:
                    smallest = url
                    smallest_w = w
            else:
                # No width info; keep a fallback.
                if not smallest:
                    smallest = url

        return best_under or smallest or (images[0].get("url") if images else "")

    def _get_token(self) -> str:
        # If explicit credentials configured, use the Client Credentials OAuth flow.
        if self._client_id and self._client_secret:
            if self._token and time.time() < self._token.expires_at:
                return self._token.access_token

            basic = base64.b64encode(
                f"{self._client_id}:{self._client_secret}".encode("utf-8")
            ).decode("ascii")
            resp = requests.post(
                "https://accounts.spotify.com/api/token",
                headers={"Authorization": f"Basic {basic}"},
                data={"grant_type": "client_credentials"},
                timeout=15,
            )
            resp.raise_for_status()
            payload = resp.json()

            access_token = payload.get("access_token")
            expires_in = float(payload.get("expires_in", 3600))
            if not access_token:
                raise RuntimeError("Spotify token response missing access_token")

            self._token = _Token(
                access_token=access_token,
                expires_at=time.time() + expires_in - 30,
            )
            return access_token

        # No credentials — use the anonymous web-player session token
        return _get_anon_session().access_token

    def search(self, query: str, search_type: str, limit: int = 30) -> list[dict]:
        search_type = (search_type or "track").lower().strip()
        if search_type not in {"track", "album", "artist"}:
            search_type = "track"

        token = self._get_token()
        resp = requests.get(
            "https://api.spotify.com/v1/search",
            headers={"Authorization": f"Bearer {token}"},
            params={
                "q": query,
                "type": search_type,
                "limit": limit,
            },
            timeout=20,
        )
        resp.raise_for_status()
        payload = resp.json()

        # Spotify response key is plural
        key = f"{search_type}s"
        items = payload.get(key, {}).get("items", [])
        if not isinstance(items, list):
            return []

        results: list[dict] = []

        for item in items:
            try:
                if search_type == "track":
                    name = item.get("name")
                    artists = item.get("artists") or []
                    artist_name = artists[0].get("name") if artists else ""
                    album = item.get("album") or {}
                    images = album.get("images") or []
                    cover_url = self._pick_image_url(images, max_px=420)
                    url = (item.get("external_urls") or {}).get("spotify", "")
                    spotify_id = item.get("id", "")
                    preview_url = item.get("preview_url") or ""

                    if name and artist_name:
                        results.append(
                            {
                                "name": name,
                                "artist": artist_name,
                                "cover_url": cover_url,
                                "url": url,
                                "type": "track",
                                "spotify_id": spotify_id,
                                "preview_url": preview_url,
                            }
                        )

                elif search_type == "album":
                    name = item.get("name")
                    artists = item.get("artists") or []
                    artist_name = artists[0].get("name") if artists else ""
                    images = item.get("images") or []
                    cover_url = self._pick_image_url(images, max_px=420)
                    url = (item.get("external_urls") or {}).get("spotify", "")
                    spotify_id = item.get("id", "")

                    if name and artist_name:
                        results.append(
                            {
                                "name": name,
                                "artist": artist_name,
                                "cover_url": cover_url,
                                "url": url,
                                "type": "album",
                                "spotify_id": spotify_id,
                            }
                        )

                elif search_type == "artist":
                    name = item.get("name")
                    images = item.get("images") or []
                    cover_url = self._pick_image_url(images, max_px=420)
                    url = (item.get("external_urls") or {}).get("spotify", "")
                    spotify_id = item.get("id", "")

                    if name:
                        results.append(
                            {
                                "name": name,
                                "artist": name,
                                "cover_url": cover_url,
                                "url": url,
                                "type": "artist",
                                "spotify_id": spotify_id,
                            }
                        )

            except Exception as e:
                logger.debug(f"Spotify parse error: {e}")
                continue

        return results

    def get_track_preview(self, artist: str, title: str) -> dict | None:
        """Find a 30s preview URL for a track using Spotify search.

        Returns:
            {"preview_url": str, "spotify_id": str, "url": str} or None
        """
        artist = (artist or "").strip()
        title = (title or "").strip()
        if not artist or not title:
            return None

        token = self._get_token()

        # Use fielded search for better matches.
        q = f'track:"{title}" artist:"{artist}"'
        resp = requests.get(
            "https://api.spotify.com/v1/search",
            headers={"Authorization": f"Bearer {token}"},
            params={
                "q": q,
                "type": "track",
                "limit": 10,
            },
            timeout=20,
        )
        resp.raise_for_status()
        payload = resp.json()
        items = payload.get("tracks", {}).get("items", [])
        if not isinstance(items, list):
            return None

        for it in items:
            preview_url = it.get("preview_url") or ""
            if not preview_url:
                continue
            spotify_id = it.get("id", "")
            url = (it.get("external_urls") or {}).get("spotify", "")
            return {"preview_url": preview_url, "spotify_id": spotify_id, "url": url}

        return None

    # ──────────────────────────────────────────────────────────────
    # Spotify URL resolver (track / album / playlist)
    # ──────────────────────────────────────────────────────────────

    def resolve_url(self, url: str, user_token: str = "") -> dict:
        """Resolve a Spotify share URL and return its tracks.

        Supports:
            https://open.spotify.com/track/<id>
            https://open.spotify.com/album/<id>
            https://open.spotify.com/playlist/<id>
            spotify:track:<id>  /  spotify:album:<id>  /  spotify:playlist:<id>

        Pass *user_token* to enable fetching private playlists for the
        authenticated user via the partner GraphQL endpoint.

        Returns a dict with keys:
            type       – "track" | "album" | "playlist"
            id         – Spotify resource ID
            name       – Album/playlist/track title
            artist     – Primary artist (empty for playlists)
            cover_url  – Artwork URL
            total      – Number of tracks
            tracks     – list[dict]  each has name/artist/cover_url/spotify_id/preview_url(/rank)
        or {"error": "..."} on failure.
        """
        m = re.search(
            r"(?:spotify[:/]|open\.spotify\.com/)([a-z]+)[/:]([A-Za-z0-9]+)",
            url,
        )
        if not m:
            return {"error": "Invalid Spotify URL — expected a track, album, or playlist link"}

        resource_type = m.group(1).lower()
        resource_id = m.group(2)

        if resource_type == "track":
            return self._resolve_track(resource_id)
        if resource_type == "album":
            return self._resolve_album(resource_id)
        if resource_type == "playlist":
            return self._resolve_playlist(resource_id, user_token=user_token)
        return {"error": f"Unsupported Spotify resource type '{resource_type}' — only track, album and playlist are supported"}

    def _resolve_track(self, spotify_id: str) -> dict:
        sess = _get_anon_session()
        data = _partner_query(sess, {
            "variables": {"uri": f"spotify:track:{spotify_id}"},
            "operationName": "getTrack",
            "extensions": {
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": "612585ae06ba435ad26369870deaae23b5c8800a256cd8a57e08eddc25a37294",
                }
            },
        })

        td = (data.get("data") or {}).get("trackUnion") or {}
        name = _pg_str(td, "name")
        artist = _pg_artists(_pg_dict(td, "artists")) or _pg_artists(_pg_dict(td, "firstArtist"))

        # cover: visualIdentity → sources  or  albumOfTrack.coverArt.sources
        cover_url = _pg_cover(_pg_dict(td, "visualIdentity"))
        if not cover_url:
            cover_url = _pg_cover(_pg_dict(_pg_dict(td, "albumOfTrack"), "coverArt"))

        return {
            "type": "track",
            "id": spotify_id,
            "name": name,
            "artist": artist,
            "cover_url": cover_url,
            "total": 1,
            "tracks": [{
                "name": name,
                "artist": artist,
                "cover_url": cover_url,
                "spotify_id": spotify_id,
                "preview_url": "",
                "rank": 1,
            }],
        }

    def _resolve_album(self, spotify_id: str) -> dict:
        sess = _get_anon_session()
        tracks_out: list[dict] = []
        offset = 0
        limit = 300
        name = ""
        artist = ""
        cover_url = ""

        while True:
            data = _partner_query(sess, {
                "variables": {
                    "uri": f"spotify:album:{spotify_id}",
                    "locale": "",
                    "offset": offset,
                    "limit": limit,
                },
                "operationName": "getAlbum",
                "extensions": {
                    "persistedQuery": {
                        "version": 1,
                        "sha256Hash": "b9bfabef66ed756e5e13f68a942deb60bd4125ec1f1be8cc42769dc0259b4b10",
                    }
                },
            })

            album = (data.get("data") or {}).get("albumUnion") or {}
            if not album:
                break

            if not name:
                name = _pg_str(album, "name")
                artist = _pg_artists(_pg_dict(album, "artists"))
                cover_url = _pg_cover(_pg_dict(album, "coverArt"))

            tracks_v2 = _pg_dict(album, "tracksV2")
            items = _pg_list(tracks_v2, "items")
            total_count = int(tracks_v2.get("totalCount") or 0)

            for item in items:
                if not isinstance(item, dict):
                    continue
                t = _pg_dict(item, "track")
                if not t:
                    continue
                t_name = _pg_str(t, "name")
                if not t_name:
                    continue
                t_artist = _pg_artists(_pg_dict(t, "artists")) or artist
                t_cover = _pg_cover(_pg_dict(_pg_dict(t, "albumOfTrack"), "coverArt")) or cover_url
                t_id = _pg_str(t, "id")
                if not t_id:
                    uri = _pg_str(t, "uri")
                    t_id = uri.split(":")[-1] if ":" in uri else uri
                tracks_out.append({
                    "name": t_name,
                    "artist": t_artist,
                    "cover_url": t_cover,
                    "spotify_id": t_id,
                    "preview_url": "",
                    "rank": len(tracks_out) + 1,
                })

            fetched = offset + len(items)
            if len(items) < limit or fetched >= total_count:
                break
            offset = fetched

        return {
            "type": "album",
            "id": spotify_id,
            "name": name,
            "artist": artist,
            "cover_url": cover_url,
            "total": len(tracks_out),
            "tracks": tracks_out,
        }

    def _resolve_playlist(self, spotify_id: str, user_token: str = "") -> dict:
        sess = _get_anon_session()
        tracks_out: list[dict] = []
        offset = 0
        limit = 300
        name = ""
        cover_url = ""

        while True:
            data = _partner_query(sess, {
                "variables": {
                    "uri": f"spotify:playlist:{spotify_id}",
                    "offset": offset,
                    "limit": limit,
                    "enableWatchFeedEntrypoint": False,
                },
                "operationName": "fetchPlaylist",
                "extensions": {
                    "persistedQuery": {
                        "version": 1,
                        "sha256Hash": "bb67e0af06e8d6f52b531f97468ee4acd44cd0f82b988e15c2ea47b1148efc77",
                    }
                },
            })

            pl = (data.get("data") or {}).get("playlistV2") or {}
            if not pl:
                break

            if not name:
                name = _pg_str(pl, "name")
                logger.debug("Spotify playlist keys: %s", list(pl.keys()))
                cover_url = _extract_playlist_cover(pl)
                logger.debug("Spotify playlist cover_url: %r", cover_url)

            content = _pg_dict(pl, "content")
            items = _pg_list(content, "items")
            total_count = int(content.get("totalCount") or 0)

            for item in items:
                if not isinstance(item, dict):
                    continue
                t = _pg_dict(_pg_dict(item, "itemV2"), "data")
                if not t:
                    continue
                if t.get("__typename") in {"PodcastEpisode", "Episode", "NotFound"}:
                    continue
                t_name = _pg_str(t, "name")
                if not t_name:
                    continue
                t_artist = _pg_artists(_pg_dict(t, "artists"))
                if not t_artist:
                    continue
                t_cover = _pg_cover(_pg_dict(_pg_dict(t, "albumOfTrack"), "coverArt")) or cover_url
                t_id = _pg_str(t, "id")
                if not t_id:
                    uri = _pg_str(t, "uri")
                    t_id = uri.split(":")[-1] if ":" in uri else uri
                tracks_out.append({
                    "name": t_name,
                    "artist": t_artist,
                    "cover_url": t_cover,
                    "spotify_id": t_id,
                    "preview_url": "",
                    "rank": len(tracks_out) + 1,
                })

            fetched = offset + len(items)
            if len(items) < limit or fetched >= total_count:
                break
            offset = fetched

        return {
            "type": "playlist",
            "id": spotify_id,
            "name": name or "Spotify Playlist",
            "artist": "",
            "cover_url": cover_url,
            "total": len(tracks_out),
            "tracks": tracks_out,
        }

    # ── OAuth2 PKCE helpers ────────────────────────────────────────────────

    @staticmethod
    def exchange_oauth_code(code: str, redirect_uri: str, client_id: str, code_verifier: str) -> dict:
        """Exchange a PKCE authorization code for access + refresh tokens."""
        resp = requests.post(
            "https://accounts.spotify.com/api/token",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect_uri,
                "client_id": client_id,
                "code_verifier": code_verifier,
            },
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def refresh_oauth_token(refresh_token: str, client_id: str) -> dict:
        """Refresh a PKCE access token using the refresh token."""
        resp = requests.post(
            "https://accounts.spotify.com/api/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": client_id,
            },
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()

    # ── User data (require a user OAuth token) ────────────────────────────

    def get_user_playlists(self, user_token: str, user_id: str = "") -> list[dict]:
        """Fetch the authenticated user's playlists.

        Tries spclient rootlist first; falls back to Web API /v1/me/playlists
        (which works with playlist-read-private scope for dev-mode apps).
        """
        # spclient rootlist always 403s for developer PKCE tokens (RBAC blocked).
        # Raise immediately so the except handler falls through to the Web API.
        try:
            raise Exception("spclient disabled — RBAC always 403s")
            resolved_id = user_id or "me"  # noqa: unreachable
            data = _spclient_get(
                user_token,
                f"{_SPCLIENT_BASE}/playlist/v2/user/{resolved_id}/rootlist",
                {
                    "decorate": "revision,length,attributes,timestamp,owner",
                    "market": "from_token",
                    "offset": 0,
                    "limit": 300,
                },
            )
            logger.info("get_user_playlists: rootlist top-level keys=%s", list(data.keys()))
            contents = data.get("contents") or {}
            logger.info("get_user_playlists: contents keys=%s, items=%d",
                        list(contents.keys()), len(contents.get("items") or []))
            items = contents.get("items") or []
            meta_items = contents.get("metaItems") or []
            if items:
                logger.info("get_user_playlists: first item keys=%s", list(items[0].keys()) if isinstance(items[0], dict) else items[0])
            results: list[dict] = []

            for idx, item in enumerate(items):
                if not isinstance(item, dict):
                    continue
                uri = (item.get("uri") or "").strip()
                if not uri or ":playlist:" not in uri:
                    continue
                pid = uri.split(":")[-1]
                if not pid:
                    continue

                attrs = item.get("attributes") or {}
                name = (attrs.get("name") or "").strip()

                total_tracks = 0
                if idx < len(meta_items) and isinstance(meta_items[idx], dict):
                    total_tracks = int(meta_items[idx].get("length") or 0)

                cover_url = ""
                try:
                    meta = _partner_query(_get_anon_session(), {
                        "variables": {
                            "uri": f"spotify:playlist:{pid}",
                            "offset": 0,
                            "limit": 1,
                            "enableWatchFeedEntrypoint": False,
                        },
                        "operationName": "fetchPlaylist",
                        "extensions": {
                            "persistedQuery": {
                                "version": 1,
                                "sha256Hash": "bb67e0af06e8d6f52b531f97468ee4acd44cd0f82b988e15c2ea47b1148efc77",
                            }
                        },
                    })
                    pl = (meta.get("data") or {}).get("playlistV2") or {}
                    if not name:
                        name = _pg_str(pl, "name")
                    if not total_tracks:
                        total_tracks = int((pl.get("content") or {}).get("totalCount") or 0)
                    cover_url = _extract_playlist_cover(pl)
                except Exception as _me:
                    logger.debug("get_user_playlists: cover fetch failed for %s: %s", pid, _me)

                if not name:
                    name = f"Playlist {pid[:8]}"

                results.append({
                    "id": pid,
                    "name": name,
                    "cover_url": cover_url,
                    "total": total_tracks,
                    "url": f"https://open.spotify.com/playlist/{pid}",
                })

            return results

        except Exception as _spc_err:
            logger.warning("get_user_playlists: spclient failed (%s), falling back to Web API", _spc_err)

        # ── Attempt 2: Web API /v1/me/playlists ───────────────────────────────
        results = []
        url: str | None = "https://api.spotify.com/v1/me/playlists?limit=50"
        headers = {"Authorization": f"Bearer {user_token}"}
        while url:
            for _attempt in range(5):
                r = requests.get(url, headers=headers, timeout=15)
                if r.status_code == 429:
                    wait = min(int(r.headers.get("Retry-After", "3")), 5)
                    logger.debug("get_user_playlists: 429 on me/playlists, waiting %ds", wait)
                    time.sleep(wait)
                    continue
                if r.status_code == 403:
                    try:
                        err_body = r.json()
                        err_msg = (err_body.get("error") or {}).get("message") or ""
                    except Exception:
                        err_msg = r.text or ""
                    logger.error(
                        "get_user_playlists: 403 from Web API — Spotify error: %r",
                        err_msg,
                    )
                    if "not registered for this application" in err_msg.lower():
                        raise Exception(f"not_registered_user: {err_msg}")
                    if "insufficient client scope" in err_msg.lower():
                        raise Exception("scope_error: Spotify token is missing the playlist-read-private scope.")
                r.raise_for_status()
                break
            else:
                r.raise_for_status()  # exhausted retries — let caller handle it
            body = r.json()
            for pl in body.get("items") or []:
                if not pl or not pl.get("id"):
                    continue
                images = pl.get("images") or []
                cover_url = images[0]["url"] if images else ""
                results.append({
                    "id": pl["id"],
                    "name": pl.get("name") or "",
                    "cover_url": cover_url,
                    "total": (pl.get("tracks") or {}).get("total") or 0,
                    "url": pl.get("external_urls", {}).get("spotify") or f"https://open.spotify.com/playlist/{pl['id']}",
                })
            url = body.get("next")
        logger.info("get_user_playlists: Web API returned %d playlists", len(results))

        # /v1/me/playlists returns tracks.total=0 under developer quota restrictions.
        # Use the anonymous partner GraphQL fetchPlaylist query (limit=1) to get totalCount
        # — same source that _resolve_playlist uses, no user token required.
        needs_count = [p for p in results if not p["total"]]
        logger.info("get_user_playlists: %d/%d playlists need count fetch via partner API", len(needs_count), len(results))
        if needs_count:
            import concurrent.futures as _cf
            def _fetch_count(p: dict) -> int:
                try:
                    meta = _partner_query(_get_anon_session(), {
                        "variables": {
                            "uri": f"spotify:playlist:{p['id']}",
                            "offset": 0,
                            "limit": 1,
                            "enableWatchFeedEntrypoint": False,
                        },
                        "operationName": "fetchPlaylist",
                        "extensions": {
                            "persistedQuery": {
                                "version": 1,
                                "sha256Hash": "bb67e0af06e8d6f52b531f97468ee4acd44cd0f82b988e15c2ea47b1148efc77",
                            }
                        },
                    })
                    pl = (meta.get("data") or {}).get("playlistV2") or {}
                    count = int((pl.get("content") or {}).get("totalCount") or 0)
                    # Also backfill cover if missing
                    if not p.get("cover_url"):
                        p["cover_url"] = _extract_playlist_cover(pl)
                    return count
                except Exception as _ce:
                    logger.warning("get_user_playlists: partner count fetch error for %s: %s", p['id'], _ce)
                return 0
            with _cf.ThreadPoolExecutor(max_workers=3) as pool:
                counts = list(pool.map(_fetch_count, needs_count))
            for p, cnt in zip(needs_count, counts):
                p["total"] = cnt
            logger.info("get_user_playlists: after count fetch, sample totals: %s",
                        [(p["name"], p["total"]) for p in results[:5]])

        # For any still at 0 (private playlists the anon session can't see),
        # fall back to user-token /v1/playlists/{id}.  Run sequentially so we
        # don't trigger a 429 immediately after the partner-API burst; honour
        # Retry-After if we do get rate-limited.
        still_zero = [p for p in results if not p["total"]]
        if still_zero:
            logger.info(
                "get_user_playlists: %d playlists still 0 after partner fetch, trying user-token fallback",
                len(still_zero),
            )
            for p in still_zero:
                try:
                    # limit=1 (not 0 — invalid), no fields filter.
                    # Returns paging object with top-level `total`.
                    rc = requests.get(
                        f"https://api.spotify.com/v1/playlists/{p['id']}/tracks",
                        headers=headers,
                        params={"limit": "1"},
                        timeout=10,
                    )
                    if rc.status_code == 429:
                        wait = min(int(rc.headers.get("Retry-After", "2")), 5)
                        logger.debug("get_user_playlists: 429 on %s, waiting %ds", p["id"], wait)
                        time.sleep(wait)
                        # one retry after backoff
                        rc = requests.get(
                            f"https://api.spotify.com/v1/playlists/{p['id']}/tracks",
                            headers=headers,
                            params={"limit": "1"},
                            timeout=10,
                        )
                    if rc.ok:
                        p["total"] = int(rc.json().get("total") or 0)
                        if p["total"]:
                            logger.info(
                                "get_user_playlists: user-token fallback '%s' -> total=%d",
                                p["name"], p["total"],
                            )
                        else:
                            logger.info(
                                "get_user_playlists: user-token fallback '%s' -> still 0 body=%s",
                                p["name"], rc.text[:200],
                            )
                    elif rc.status_code == 403:
                        # Dev-mode quota block — mark as None so UI shows 'private' badge.
                        p["total"] = None
                        logger.debug("get_user_playlists: 403 on '%s' (dev quota — marking private)", p["name"])
                    else:
                        logger.info(
                            "get_user_playlists: user-token fallback '%s' -> HTTP %d body=%s",
                            p["name"], rc.status_code, rc.text[:200],
                        )
                except Exception as _ce:
                    logger.debug("get_user_playlists: user-token fallback error %s: %s", p["id"], _ce)
                time.sleep(0.15)
            private_count = sum(1 for p in results if p["total"] is None)
            still_zero_names = [p["name"] for p in results if p["total"] == 0]
            if private_count:
                logger.info("get_user_playlists: %d playlists marked private (dev quota)", private_count)
            if still_zero_names:
                logger.info("get_user_playlists: still truly 0: %s", still_zero_names)

        return results

    def get_user_liked_tracks(self, user_token: str, user_id: str = "") -> list[dict]:
        """Fetch all saved/liked tracks.

        Tries spclient collection-web first; falls back to Web API /v1/me/tracks.
        Returns stubs with spotify_id only; name/artist resolved at import time.
        """
        # spclient collection-web always 403s for developer PKCE tokens (RBAC).
        try:
            raise Exception("spclient disabled — RBAC always 403s")
            resolved_id = user_id or "me"  # noqa: unreachable
            results: list[dict] = []
            offset = 0
            limit = 50
            while True:
                data = _spclient_get(
                    user_token,
                    f"{_SPCLIENT_BASE}/collection-web/v4/user/{resolved_id}/collection",
                    {
                        "paging.offset": offset,
                        "paging.limit": limit,
                        "filters": "Song",
                        "sortOrder": "DATE_ADDED_DESC",
                    },
                )
                items = data.get("item") or data.get("items") or []
                total_count = int(
                    data.get("count") or data.get("total") or
                    (data.get("paging") or {}).get("total") or 0
                )
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    uri = (item.get("uri") or "").strip()
                    if not uri:
                        continue
                    spotify_id = uri.split(":")[-1] if ":" in uri else uri
                    results.append({"name": "", "artist": "", "cover_url": "", "spotify_id": spotify_id})
                fetched = offset + len(items)
                if not items or (total_count and fetched >= total_count):
                    break
                offset = fetched
            return results
        except Exception as _spc_err:
            logger.warning("get_user_liked_tracks: spclient failed (%s), falling back to Web API", _spc_err)

        # ── Attempt 2: Web API /v1/me/tracks ─────────────────────────────────
        results = []
        url: str | None = "https://api.spotify.com/v1/me/tracks?limit=50"
        headers = {"Authorization": f"Bearer {user_token}"}
        while url:
            for _attempt in range(5):
                r = requests.get(url, headers=headers, timeout=15)
                if r.status_code == 429:
                    wait = min(int(r.headers.get("Retry-After", "3")), 5)
                    logger.debug("get_user_liked_tracks: 429 on me/tracks, waiting %ds", wait)
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                break
            else:
                r.raise_for_status()
            body = r.json()
            for entry in body.get("items") or []:
                track = (entry or {}).get("track") or {}
                if not track.get("id"):
                    continue
                artists = track.get("artists") or []
                artist_str = ", ".join(a["name"] for a in artists if a.get("name"))
                images = (track.get("album") or {}).get("images") or []
                cover_url = images[0]["url"] if images else ""
                results.append({
                    "name": track.get("name") or "",
                    "artist": artist_str,
                    "cover_url": cover_url,
                    "spotify_id": track["id"],
                })
            url = body.get("next")
        logger.info("get_user_liked_tracks: Web API returned %d tracks", len(results))
        return results

    def get_user_liked_count(self, user_token: str, user_id: str = "") -> int:
        """Return total liked-track count.

        Tries spclient collection-web first; falls back to Web API /v1/me/tracks.
        """
        # spclient always 403s for developer PKCE tokens (RBAC blocked).
        try:
            raise Exception("spclient disabled — RBAC always 403s")
            resolved_id = user_id or "me"  # noqa: unreachable
            data = _spclient_get(
                user_token,
                f"{_SPCLIENT_BASE}/collection-web/v4/user/{resolved_id}/collection",
                {"paging.offset": 0, "paging.limit": 1, "filters": "Song"},
            )
            total = (
                data.get("count") or data.get("total") or
                (data.get("paging") or {}).get("total") or 0
            )
            return int(total)
        except Exception as _spc_err:
            logger.debug("spclient liked count failed (%s), trying Web API", _spc_err)

        # ── Attempt 2: Web API /v1/me/tracks ─────────────────────────────────
        try:
            for _attempt in range(5):
                r = requests.get(
                    "https://api.spotify.com/v1/me/tracks?limit=1",
                    headers={"Authorization": f"Bearer {user_token}"},
                    timeout=10,
                )
                if r.status_code == 429:
                    wait = min(int(r.headers.get("Retry-After", "3")), 5)
                    logger.debug("get_user_liked_count: 429 on me/tracks, waiting %ds", wait)
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                return int(r.json().get("total") or 0)
        except Exception as _e:
            logger.debug("Web API liked count failed: %s", _e)
        return 0