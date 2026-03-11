from __future__ import annotations

import logging
import re
import unicodedata
import os
import socket
from urllib.parse import urlparse

import requests
import xml.etree.ElementTree as ET

from plexapi.server import PlexServer
from plexapi.exceptions import Unauthorized

from .models import Track

logger = logging.getLogger(__name__)


class PlexClient:
    def __init__(self, baseurl: str, token: str, music_library: str) -> None:
        token = (token or "").strip()
        baseurl = (baseurl or "").strip()
        if not token or not baseurl:
            raise ValueError("Missing Plex base URL or token")

        client_id = (
            os.environ.get("PLEX_OAUTH_CLIENT_ID")
            or os.environ.get("PLEX_CLIENT_ID")
            or os.environ.get("X_PLEX_CLIENT_IDENTIFIER")
            or ""
        ).strip()
        if not client_id:
            client_id = f"soundscout-{socket.gethostname()}"

        verify_ssl = (os.environ.get("PLEX_VERIFY_SSL", "1") or "1").strip().lower() not in {
            "0",
            "false",
            "no",
            "n",
            "off",
        }

        def _make_session() -> requests.Session:
            sess = requests.Session()
            sess.verify = verify_ssl
            # Don't force an Accept header; Plex APIs default to XML and plexapi expects that.
            # Also don't set X-Plex-Token here; plexapi attaches it per-request.
            sess.headers.update(
                {
                    "X-Plex-Client-Identifier": client_id,
                    "X-Plex-Product": "SoundScout",
                    "X-Plex-Version": "1.0",
                    "X-Plex-Device": "SoundScout",
                    "X-Plex-Device-Name": "SoundScout",
                    "X-Plex-Platform": "SoundScout",
                }
            )
            return sess

        def _try_get_machine_identifier() -> str:
            """Best-effort machineIdentifier from PMS /identity.

            /identity is typically available without auth and helps us select the
            correct server from plex.tv resources when the account has multiple.
            """

            try:
                resp = requests.get(
                    f"{baseurl.rstrip('/')}/identity",
                    headers={
                        "X-Plex-Client-Identifier": client_id,
                        "X-Plex-Product": "SoundScout",
                    },
                    timeout=10,
                    verify=verify_ssl,
                )
                if resp.status_code != 200:
                    return ""
                root = ET.fromstring(resp.text or "")
                if not hasattr(root, "attrib"):
                    return ""
                return (root.attrib.get("machineIdentifier") or "").strip()
            except Exception:
                return ""

        def _plex_tv_resource_connections(machine_identifier: str) -> list[tuple[str, str]]:
            """Discover candidate server base URLs via plex.tv resources.

            Returns a list of (uri, access_token) pairs.

            Important: For shared-library users, the account token used for plex.tv
            may not be accepted directly by the Plex Media Server. In that case,
            plex.tv's resources response includes a server-scoped accessToken that
            *is* accepted by PMS. We prefer that when present.
            """

            try:
                resp = requests.get(
                    "https://plex.tv/api/v2/resources",
                    params={"includeHttps": "1", "includeRelay": "1"},
                    headers={
                        "Accept": "application/json",
                        "X-Plex-Client-Identifier": client_id,
                        "X-Plex-Product": "SoundScout",
                        "X-Plex-Token": token,
                    },
                    timeout=15,
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.warning("Failed fetching plex.tv resources: %s", e)
                return []

            if not isinstance(data, list):
                return []

            parsed = urlparse(baseurl)
            wanted_host = (parsed.hostname or "").strip().lower()
            wanted_port = parsed.port or 32400

            def _conn_score(c: dict, access_token: str) -> tuple[int, int, str, str]:
                uri = (c.get("uri") or "").strip().rstrip("/")
                is_https = 1 if uri.startswith("https://") else 0
                is_local = 1 if bool(c.get("local")) else 0
                is_relay = 1 if bool(c.get("relay")) else 0
                # Prefer local + https + non-relay.
                score = is_local * 100 + is_https * 10 + (0 if is_relay else 5)
                return (score, is_https, uri, access_token)

            candidates: list[tuple[int, int, str, str]] = []

            machine_identifier = (machine_identifier or "").strip()

            for item in data:
                if not isinstance(item, dict):
                    continue

                provides = (item.get("provides") or "").strip().lower()
                if "server" not in provides:
                    continue

                if machine_identifier:
                    item_id = (
                        (item.get("clientIdentifier") or "")
                        or (item.get("machineIdentifier") or "")
                    ).strip()
                    if item_id and item_id != machine_identifier:
                        continue

                access_token = (item.get("accessToken") or "").strip()

                conns = item.get("connections")
                if not isinstance(conns, list) or not conns:
                    continue

                # Try to pick the right server by matching the originally provided baseurl.
                matched = False
                for c in conns:
                    if not isinstance(c, dict):
                        continue
                    uri = (c.get("uri") or "").strip().rstrip("/")
                    if not uri.startswith("http"):
                        continue
                    u = urlparse(uri)
                    host = (u.hostname or "").strip().lower()
                    port = u.port or 32400
                    address = (c.get("address") or "").strip().lower()
                    if wanted_host and (
                        (host == wanted_host and port == wanted_port)
                        or (address == wanted_host and port == wanted_port)
                    ):
                        matched = True
                        break

                if not matched:
                    continue

                for c in conns:
                    if not isinstance(c, dict):
                        continue
                    uri = (c.get("uri") or "").strip().rstrip("/")
                    if not uri.startswith("http"):
                        continue
                    candidates.append(_conn_score(c, access_token))

            # If we didn't find a match by host/port, fall back to *any* PMS connection URIs.
            if not candidates:
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    provides = (item.get("provides") or "").strip().lower()
                    if "server" not in provides:
                        continue

                    if machine_identifier:
                        item_id = (
                            (item.get("clientIdentifier") or "")
                            or (item.get("machineIdentifier") or "")
                        ).strip()
                        if item_id and item_id != machine_identifier:
                            continue

                    access_token = (item.get("accessToken") or "").strip()

                    conns = item.get("connections")
                    if not isinstance(conns, list) or not conns:
                        continue
                    for c in conns:
                        if not isinstance(c, dict):
                            continue
                        uri = (c.get("uri") or "").strip().rstrip("/")
                        if not uri.startswith("http"):
                            continue
                        candidates.append(_conn_score(c, access_token))

            candidates_sorted = sorted(candidates, key=lambda t: (-t[0], -t[1], t[2]))
            deduped: list[tuple[str, str]] = []
            seen: set[tuple[str, str]] = set()
            for _score, _https, uri, access_token in candidates_sorted:
                key = (uri, access_token)
                if key in seen:
                    continue
                seen.add(key)
                deduped.append(key)
            return deduped

        # 1) Try the provided baseurl.
        session = _make_session()
        try:
            self.server = PlexServer(baseurl, token, session=session, timeout=20)
            self.music_section = self.server.library.section(music_library)
            return
        except Unauthorized as e:
            logger.warning(
                "Plex unauthorized for baseurl=%s; will try plex.tv resource connections: %s",
                baseurl,
                e,
            )
            last_exc: Exception = e
        except Exception as e:
            # Only fall back for clear auth errors.
            msg = str(e).lower()
            if "401" not in msg and "unauthorized" not in msg:
                raise
            logger.warning(
                "Plex auth error for baseurl=%s; will try plex.tv resource connections: %s",
                baseurl,
                e,
            )
            last_exc = e

        # 2) Fall back to plex.tv-discovered base URLs (prefer https plex.direct).
        machine_id = _try_get_machine_identifier()
        if machine_id:
            logger.info("Plex server machineIdentifier=%s", machine_id)

        candidates = _plex_tv_resource_connections(machine_id)
        if candidates:
            preview = ", ".join([c[0] for c in candidates[:5]])
            suffix = "" if len(candidates) <= 5 else f" (+{len(candidates) - 5} more)"
            logger.info("Plex plex.tv candidate baseurls: %s%s", preview, suffix)
        if not candidates:
            logger.warning(
                "No plex.tv resource connections found for this token (baseurl=%s, machine_id=%s)",
                baseurl,
                machine_id or "(unknown)",
            )

        had_resource_access_token = any(bool((t or "").strip()) for _u, t in candidates)

        for uri, access_token in candidates[:12]:
            try:
                session = _make_session()
                logger.info("Trying Plex baseurl=%s", uri)

                access_token = (access_token or "").strip()
                user_token = (token or "").strip()

                # Prefer resource-scoped accessToken when present, but fall back
                # to the user's account token if needed.
                tokens_to_try: list[str] = []
                if access_token:
                    tokens_to_try.append(access_token)
                if user_token and user_token not in tokens_to_try:
                    tokens_to_try.append(user_token)

                last_try_exc: Exception | None = None
                for t in tokens_to_try:
                    try:
                        self.server = PlexServer(uri, t, session=session, timeout=20)
                        self.music_section = self.server.library.section(music_library)
                        logger.info("Using Plex baseurl=%s (from plex.tv resources)", uri)
                        return
                    except Exception as e:
                        last_try_exc = e
                        continue

                if last_try_exc is not None:
                    raise last_try_exc
            except requests.exceptions.SSLError as e:
                logger.warning(
                    "SSL error connecting to Plex baseurl=%s (consider PLEX_VERIFY_SSL=0 if this is a local/self-signed URL): %s",
                    uri,
                    e,
                )
                last_exc = e
                continue
            except Exception as e:
                logger.debug("Plex connect failed for candidate baseurl=%s: %s", uri, e)
                last_exc = e
                continue

        if had_resource_access_token:
            logger.warning(
                "Plex connection failed even with plex.tv resource accessToken candidates (shared-user path). "
                "This usually indicates network/DNS/SSL reachability issues to the discovered URIs rather than a missing share."
            )

        raise last_exc

    @staticmethod
    def _norm(text: str) -> str:
        s = unicodedata.normalize("NFKC", text or "")
        # Remove ascii enforcing to allow CJK and other scripts
        s = s.lower().strip()
        # Drop common noise like feat/remaster/live annotations
        s = re.sub(r"\s*\(?(feat\.|featuring|ft\.).*?\)?", "", s, flags=re.IGNORECASE)
        s = re.sub(r"\s*\(?(remaster(ed)?|live|mono|stereo|radio edit|edit|version|deluxe).*?\)?", "", s, flags=re.IGNORECASE)
        s = re.sub(r"\s*[-–—:]\s*(remaster(ed)?|live|mono|stereo|radio edit|edit|version|deluxe).*?$", "", s, flags=re.IGNORECASE)
        # Remove punctuation but keep word characters (including unicode letters)
        # Using \w includes [a-zA-Z0-9_] plus many unicode chars, but might exclude some.
        # Ideally we just strip punctuation and keep everything else.
        s = re.sub(r"[^\w\s]", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    @classmethod
    def _score_match(cls, wanted: Track, item) -> int:
        try:
            item_title = (getattr(item, "title", "") or "").strip()
            item_artist = (getattr(item, "grandparentTitle", "") or "").strip()
        except Exception:
            return 0

        wt = cls._norm(wanted.title)
        it = cls._norm(item_title)
        ia = cls._norm(item_artist)

        score = 0
        if wt and it:
            if wt == it:
                score += 3
            elif wt in it or it in wt:
                score += 2

        # Try each comma-split artist so "beabadoobee, Laufey" still matches "beabadoobee"
        artist_parts = [cls._norm(p) for p in (wanted.artist or "").split(",") if p.strip()]
        if ia and artist_parts:
            for wa in artist_parts:
                if not wa:
                    continue
                if wa == ia:
                    score += 3
                    break
                elif wa in ia or ia in wa:
                    score += 2
                    break
        return score

    def find_track(self, track: Track):
        """Best-effort track lookup in Plex music library.

        Strategy:
        - search for tracks by title (track-only)
        - search for tracks by artist + title
        - SEARCH BY ARTIST + Recency Fallback (for language mismatches)
        - score candidates using normalized title/artist
        """
        wanted_title = (track.title or "").strip()
        wanted_artist = (track.artist or "").strip()
        if not wanted_title or not wanted_artist:
            return None

        candidates = []
        
        # 1. Exact/Fuzzy Title Search
        try:
            candidates.extend(self.music_section.search(title=wanted_title, libtype="track"))
        except Exception:
            pass

        # 2. Combined Query Search
        if not candidates:
            query = f"{wanted_artist} {wanted_title}".strip()
            try:
                candidates.extend(self.music_section.search(query, libtype="track"))
            except Exception:
                pass
        
        # 3. Fallback: Search by Artist and look for RECENTLY ADDED tracks
        # This handles the case where we just downloaded "Sky Restaurant" but we are looking for "スカイレストラン"
        # If we find a track by "Hi-Fi Set" added in the last 24 hours, it's probably the one.
        try:
            # OPTION A: Search via Artist object (current approach)
            # "check the 30 most recently added song for the same artist name"
            artist_candidates = self.music_section.search(title=wanted_artist, libtype="artist")
            
            # If nothing found, try simpler normalization approach for artist name lookup?
            # Plex search is usually good, but let's just stick to what we found.
            
            if artist_candidates:
                # We take the first artist match
                artist = artist_candidates[0]
                
                # We want recently added tracks from this artist.
                # Just asking for tracks() might return them in album order or release date order.
                # We explicitly request sort="addedAt:desc" to get the newest ones.
                recent_tracks = artist.tracks(sort="addedAt:desc", limit=50)
                
                # Check timestamps on these tracks. 
                # (Ideally we just grab them if they are recent enough, regardless of title match? 
                #  User says "match it like that". We'll add them to candidates and let scoring sort it out,
                #  but the scoring needs to be aware that we *expect* title mismatch).
                
                import datetime
                yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
                
                for t in recent_tracks:
                     if t.addedAt and t.addedAt > yesterday:
                         candidates.append(t)
            
            # OPTION B: Search RECENTLY ADDED globally (if Option A failed or invalid artist match)
            # This catches cases where Plex hasn't fully indexed the Artist string yet but the track is there.
            if not candidates:
                 recently_added = self.music_section.recentlyAdded(maxResults=50, libtype='track')
                 for t in recently_added:
                     # Check if fuzzy artist matches
                     t_artist = self._norm(getattr(t, "grandparentTitle", "") or "")
                     w_artist = self._norm(wanted_artist)
                     if w_artist and t_artist and (w_artist in t_artist or t_artist in w_artist):
                         candidates.append(t)

        except Exception as e:
            logger.debug("Plex artist fallback search failed: %s", e)


        best_item = None
        best_score = 0
        
        # Deduplicate candidates by ratingKey
        seen_keys = set()
        unique_candidates = []
        for x in candidates:
             if x.ratingKey not in seen_keys:
                 unique_candidates.append(x)
                 seen_keys.add(x.ratingKey)

        for item in unique_candidates:
            if getattr(item, "type", None) != "track":
                continue
                
            score = self._score_match(track, item)
            
            # Special Boost: If the track is RECENTLY ADDED (last 24h) and Artist matches, 
            # we give it a massive score boost because we probably just downloaded it.
            import datetime
            yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
            
            # Check Artist Match specifically for the boost
            item_artist = self._norm(getattr(item, "grandparentTitle", "") or "")
            wanted_artist_norm = self._norm(wanted_artist)
            
            artist_match = (wanted_artist_norm and item_artist) and (wanted_artist_norm in item_artist or item_artist in wanted_artist_norm)
            
            if artist_match and item.addedAt and item.addedAt > yesterday:
                # It's by the right artist and we just added it. 99% chance it's our file.
                score += 10

            if score > best_score:
                best_score = score
                best_item = item

        # Lower threshold to 3 normally, but simple artist+recency boost takes it to 10+
        if best_score >= 3:
            return best_item

        return None

    def rate_tracks(self, items: list, rating: int = 10) -> tuple[int, int]:
        """Rate a list of Plex track items.

        rating: 1-10 where 10 = 5 stars, 0 clears the rating.
        Returns (success_count, fail_count).
        """
        ok = fail = 0
        for item in items:
            try:
                item.rate(rating)
                ok += 1
            except Exception as e:
                logger.warning("Failed to rate '%s': %s", getattr(item, "title", "?"), e)
                fail += 1
        return ok, fail

    def upsert_playlist(self, name: str, items: list, cover_url: str = "") -> None:
        existing = None
        try:
            for pl in self.server.playlists():
                if pl.title == name:
                    existing = pl
                    break
        except Exception as e:
            logger.warning("Failed listing playlists: %s", e)

        if existing is not None:
            try:
                existing.delete()
                logger.info("Deleted existing playlist '%s'", name)
            except Exception as e:
                logger.warning("Failed deleting existing playlist '%s': %s", name, e)

        if not items:
            logger.warning("No Plex items matched; skipping playlist creation")
            return

        new_pl = self.server.createPlaylist(name, items=items)
        logger.info("Created playlist '%s' with %d items", name, len(items))

        if cover_url and new_pl:
            try:
                new_pl.uploadPoster(url=cover_url)
                logger.info("Set cover art for playlist '%s'", name)
            except Exception as _ce:
                logger.warning("Could not set playlist cover art: %s", _ce)

    def get_recently_added(self, n: int) -> list:
        """Return the N most recently added tracks from the music library, sorted newest-first."""
        try:
            return self.music_section.search(libtype="track", sort="addedAt:desc", maxresults=n)
        except Exception as e:
            logger.warning("get_recently_added failed: %s", e)
            return []

    def update_library(self) -> bool:
        """Trigger a library refresh/scan.

        Returns True if the request succeeded, False otherwise.

        Note: Shared-library users commonly lack permission to refresh libraries,
        which results in a 403. In those cases we want to continue the pipeline
        without waiting.
        """

        try:
            self.music_section.update()
            return True
        except Exception as e:
            logger.warning("Plex library update() failed: %s", e)
            return False
