from __future__ import annotations

import datetime as dt
import logging
import concurrent.futures
from typing import Iterable

import requests

from .models import Track

logger = logging.getLogger(__name__)


class LastFmClient:
    def __init__(self, api_key: str, username: str) -> None:
        self.api_key = api_key
        self.username = username

    @staticmethod
    def _is_placeholder_image(url: str | None) -> bool:
        if not url:
            return True
        return "2a96cbd8b46e442fc41c2b86b821562f" in url

    @staticmethod
    def _best_image_url(images: list[dict] | None) -> str:
        if not images or not isinstance(images, list):
            return ""
        for size in ["extralarge", "large", "medium"]:
            for img in images:
                if img.get("size") == size and img.get("#text"):
                    return img.get("#text")
        # Fallback: last non-empty
        for img in reversed(images):
            if img.get("#text"):
                return img.get("#text")
        return ""

    def _artist_top_album_image(self, artist_name: str) -> str:
        """Fallback image source for artists: use top album cover."""
        try:
            params = {
                "method": "artist.getTopAlbums",
                "artist": artist_name,
                "api_key": self.api_key,
                "format": "json",
                "autocorrect": 1,
                "limit": 3,
            }
            resp = requests.get("https://ws.audioscrobbler.com/2.0/", params=params, timeout=5)
            if resp.status_code != 200:
                return ""
            data = resp.json()
            albums = data.get("topalbums", {}).get("album", [])
            if isinstance(albums, dict):
                albums = [albums]
            if not isinstance(albums, list) or not albums:
                return ""

            for album in albums:
                url = self._best_image_url(album.get("image", []))
                if url and not self._is_placeholder_image(url):
                    return url
            return ""
        except Exception:
            return ""

    def _enrich_result(self, item: dict, item_type: str) -> dict:
        """Enrich a result with better metadata/cover art."""
        try:
            name = item.get("name")
            artist = item.get("artist")
            
            params = {
                "api_key": self.api_key,
                "format": "json",
                "autocorrect": 1
            }
            
            if item_type == "track":
                params["method"] = "track.getInfo"
                params["track"] = name
                params["artist"] = artist
            elif item_type == "album":
                params["method"] = "album.getInfo"
                params["album"] = name
                params["artist"] = artist
            elif item_type == "artist":
                params["method"] = "artist.getInfo"
                params["artist"] = name
                
            resp = requests.get("https://ws.audioscrobbler.com/2.0/", params=params, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                root = data.get(item_type, {})
                
                # Extract image
                images = root.get("image", [])
                
                # For tracks, try falling back to album image if track has no image
                if item_type == "track" and not images:
                    images = root.get("album", {}).get("image", [])

                best_url = self._best_image_url(images)

                # Artist images are frequently missing/placeholder on Last.fm.
                # Fallback to top-album cover art so artist cards have a real thumbnail.
                if item_type == "artist" and self._is_placeholder_image(best_url):
                    best_url = self._artist_top_album_image(name)

                if best_url and not self._is_placeholder_image(best_url):
                    item["cover_url"] = best_url
                        
        except Exception:
            pass
            
        return item

    def _search_generic(self, query: str, method: str, root_key: str, sub_key: str, item_type: str, limit: int) -> list[dict]:
        """Generic search helper."""
        params = {
            "method": method,
            "api_key": self.api_key,
            "format": "json",
            "limit": limit
        }
        # Last.fm param name matches item type except for tracks/albums sometimes
        if item_type == "track": params["track"] = query
        elif item_type == "album": params["album"] = query
        elif item_type == "artist": params["artist"] = query
        
        try:
            resp = requests.get("https://ws.audioscrobbler.com/2.0/", params=params, timeout=30)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as e:
            logger.warning(f"Failed to search {item_type}: {e}")
            return []

        results = []
        # Safely traverse: results -> root_key -> sub_key -> list
        try:
            items_raw = payload.get("results", {}).get(root_key, {}).get(sub_key, [])
            if not isinstance(items_raw, list):
                items_raw = [items_raw] # Single result sometimes not a list
        except Exception:
            items_raw = []
        
        for item in items_raw:
            try:
                name = item.get("name")
                # Artist search doesn't have 'artist' field in the item, it IS the artist
                artist = item.get("artist") if item_type != "artist" else name
                
                if item_type == "artist":
                    # For artist search, the name is the name
                    pass
                
                # Initial placeholder image
                cover_url = ""
                images = item.get("image", [])
                if images and isinstance(images, list):
                    for img in images:
                        if img.get("size") == "extralarge" and img.get("#text"):
                            cover_url = img.get("#text")
                            break
                    if not cover_url and images:
                        cover_url = images[-1].get("#text", "")

                # Don't propagate Last.fm's generic placeholder image
                if cover_url and "2a96cbd8b46e442fc41c2b86b821562f" in cover_url:
                    cover_url = ""
                
                entry = {
                    "name": name,
                    "cover_url": cover_url,
                    "url": item.get("url", ""),
                    "type": item_type
                }
                if item_type != "artist":
                    entry["artist"] = artist
                else:
                    entry["artist"] = name # For consistency in UI card

                if name:
                    results.append(entry)
            except Exception:
                continue
        
        # Enrich top results
        to_enrich = results[:15]
        rest = results[15:]
        
        enriched = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_item = {executor.submit(self._enrich_result, item.copy(), item_type): item for item in to_enrich}
            for future in concurrent.futures.as_completed(future_to_item):
                try:
                    res = future.result()
                    enriched.append(res)
                except Exception:
                    enriched.append(future_to_item[future])
        
        # Re-sort to preserve order? The ThreadPool execution order is not guaranteed.
        # Map enriched back to names
        enriched_map = { (i["name"], i["artist"]): i for i in enriched }
        
        final = []
        for original in to_enrich:
            final.append(enriched_map.get((original["name"], original["artist"]), original))
            
        final.extend(rest)
        return final

    def search_tracks(self, query: str, limit: int = 30) -> list[dict]:
        return self._search_generic(query, "track.search", "trackmatches", "track", "track", limit)

    def search_albums(self, query: str, limit: int = 30) -> list[dict]:
        return self._search_generic(query, "album.search", "albummatches", "album", "album", limit)

    def search_artists(self, query: str, limit: int = 30) -> list[dict]:
        return self._search_generic(query, "artist.search", "artistmatches", "artist", "artist", limit)

    def get_album_tracks(self, artist: str, album: str) -> list[tuple[str, str]]:
        """Get list of (artist, title) tuples for an album."""
        params = {
            "method": "album.getInfo",
            "api_key": self.api_key,
            "artist": artist,
            "album": album,
            "format": "json",
            "autocorrect": 1
        }
        try:
            resp = requests.get("https://ws.audioscrobbler.com/2.0/", params=params, timeout=10)
            data = resp.json()
            track_list = data.get("album", {}).get("tracks", {}).get("track", [])
            
            result = []
            if isinstance(track_list, dict): # Single track album?
                track_list = [track_list]
                
            for t in track_list:
                t_name = t.get("name")
                # Artist in track is usually a dict key 'name' or just string in some endpoints
                t_artist = t.get("artist", {})
                if isinstance(t_artist, dict):
                    t_artist_name = t_artist.get("name")
                else:
                    t_artist_name = t_artist
                
                # If artist name missing in track, use album artist
                if not t_artist_name: 
                    t_artist_name = artist
                    
                if t_name:
                    result.append((t_artist_name, t_name))
            return result
        except Exception as e:
            logger.error(f"Failed to get album tracks: {e}")
            return []

    def get_album_tracks_detailed(self, artist: str, album: str) -> dict:
        """Get album metadata and detailed tracks for navigation views.

        Returns a dict:
            {
              "artist": str,
              "album": str,
              "cover_url": str,
              "tracks": [ {"name": str, "artist": str} ... ]
            }
        """
        params = {
            "method": "album.getInfo",
            "api_key": self.api_key,
            "artist": artist,
            "album": album,
            "format": "json",
            "autocorrect": 1,
        }
        try:
            resp = requests.get("https://ws.audioscrobbler.com/2.0/", params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            album_obj = data.get("album", {}) or {}

            # Best-effort "published" timestamp. Last.fm often stores this in wiki metadata.
            published = ""
            published_ts = 0
            try:
                wiki = album_obj.get("wiki") if isinstance(album_obj, dict) else None
                if isinstance(wiki, dict):
                    published = str(wiki.get("published") or "").strip()
                # Example formats observed:
                # - "12 Oct 2018, 00:00"
                # - "12 Oct 2018, 00:00 +0000"
                # We'll parse the date part only for robustness.
                if published:
                    date_part = published.split(",", 1)[0].strip()
                    try:
                        d = dt.datetime.strptime(date_part, "%d %b %Y").date()
                        published_ts = int(dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc).timestamp())
                    except Exception:
                        published_ts = 0
            except Exception:
                published = ""
                published_ts = 0

            images = album_obj.get("image", [])
            cover_url = self._best_image_url(images)
            if self._is_placeholder_image(cover_url):
                cover_url = ""

            track_list = album_obj.get("tracks", {}).get("track", [])
            if isinstance(track_list, dict):
                track_list = [track_list]

            tracks: list[dict] = []
            for idx, t in enumerate(track_list or [], start=1):
                t_name = t.get("name")
                t_artist = t.get("artist", {})
                if isinstance(t_artist, dict):
                    t_artist_name = t_artist.get("name")
                else:
                    t_artist_name = t_artist

                rank = idx
                attr = t.get("@attr") if isinstance(t, dict) else None
                if isinstance(attr, dict):
                    try:
                        rank = int(attr.get("rank") or rank)
                    except Exception:
                        rank = idx

                if not t_artist_name:
                    t_artist_name = artist
                if t_name:
                    tracks.append({"name": t_name, "artist": t_artist_name, "rank": rank})

            return {
                "artist": artist,
                "album": album,
                "cover_url": cover_url,
                "tracks": tracks,
                "published": published,
                "published_ts": published_ts,
            }
        except Exception as e:
            logger.error(f"Failed to get album info: {e}")
            return {"artist": artist, "album": album, "cover_url": "", "tracks": [], "published": "", "published_ts": 0}

    def get_artist_albums(self, artist: str, limit: int = 50) -> list[dict]:
        """Get an artist's albums (Top Albums) for navigation views."""
        params = {
            "method": "artist.getTopAlbums",
            "api_key": self.api_key,
            "artist": artist,
            "format": "json",
            "autocorrect": 1,
            "limit": limit,
        }
        try:
            resp = requests.get("https://ws.audioscrobbler.com/2.0/", params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            albums = data.get("topalbums", {}).get("album", [])
            if isinstance(albums, dict):
                albums = [albums]

            out: list[dict] = []
            for a in albums or []:
                name = a.get("name")
                if not name:
                    continue

                images = a.get("image", [])
                cover_url = self._best_image_url(images)
                if self._is_placeholder_image(cover_url):
                    cover_url = ""

                out.append(
                    {
                        "name": name,
                        "artist": artist,
                        "cover_url": cover_url,
                        "type": "album",
                        "url": a.get("url", ""),
                    }
                )
            return out
        except Exception as e:
            logger.error(f"Failed to get artist albums: {e}")
            return []

    def get_artist_top_tracks(self, artist: str, limit: int = 10) -> list[tuple[str, str]]:
        """Get list of (artist, title) tuples for an artist's top tracks."""
        params = {
            "method": "artist.getTopTracks",
            "api_key": self.api_key,
            "artist": artist,
            "format": "json",
            "autocorrect": 1,
            "limit": limit
        }
        try:
            resp = requests.get("https://ws.audioscrobbler.com/2.0/", params=params, timeout=10)
            data = resp.json()
            track_list = data.get("toptracks", {}).get("track", [])
            
            result = []
            if isinstance(track_list, dict):
                track_list = [track_list]
                
            for t in track_list:
                t_name = t.get("name")
                t_artist = t.get("artist", {})
                if isinstance(t_artist, dict):
                    t_artist_name = t_artist.get("name")
                else:
                    t_artist_name = t_artist
                
                if not t_artist_name: t_artist_name = artist
                    
                if t_name:
                    result.append((t_artist_name, t_name))
            return result
        except Exception as e:
            logger.error(f"Failed to get artist top tracks: {e}")
            return []


    def get_top_tracks_recent(self, limit: int = 50) -> list[Track]:
        """Fetch user's top tracks over a slightly longer recent period (1 month) to act as backup seeds."""
        params = {
            "method": "user.getTopTracks",
            "user": self.username,
            "api_key": self.api_key,
            "format": "json",
            "period": "1month",
            "limit": limit,
        }
        
        try:
            resp = requests.get("https://ws.audioscrobbler.com/2.0/", params=params, timeout=30)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as e:
            logger.warning("Failed to fetch top tracks for seed boost: %s", e)
            return []

        tracks_raw = (
            payload.get("toptracks", {}).get("track", [])
            if isinstance(payload, dict)
            else []
        )
        tracks = []
        for item in tracks_raw:
            try:
                title = str(item.get("name", "")).strip()
                artist = str(item.get("artist", {}).get("name", "")).strip()
                if title and artist:
                    tracks.append(Track(artist=artist, title=title))
            except Exception:
                continue
        return tracks

    def get_last_week_tracks(self, max_tracks: int) -> list[Track]:
        """Fetch last week's listening chart (best approximation of 'weekly discovery')."""
        # Last.fm weekly chart endpoints require unix timestamps.
        now = dt.datetime.now(dt.timezone.utc)
        start = (now - dt.timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = now

        params = {
            "method": "user.getWeeklyTrackChart",
            "user": self.username,
            "api_key": self.api_key,
            "format": "json",
            "from": int(start.timestamp()),
            "to": int(end.timestamp()),
            "limit": max_tracks,
        }

        resp = requests.get("https://ws.audioscrobbler.com/2.0/", params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()

        tracks_raw = (
            payload.get("weeklytrackchart", {}).get("track", [])
            if isinstance(payload, dict)
            else []
        )
        if not tracks_raw:
            logger.warning("Last.fm returned no tracks for the last week window")
            return []

        tracks: list[Track] = []
        for item in tracks_raw:
            try:
                title = str(item.get("name", "")).strip()
                artist = str(item.get("artist", {}).get("#text", "")).strip()
                playcount = item.get("playcount")
                playcount_int = int(playcount) if playcount is not None else None
            except Exception:
                continue

            if title and artist:
                tracks.append(Track(artist=artist, title=title, playcount=playcount_int))

        # Deduplicate preserving order
        seen: set[tuple[str, str]] = set()
        unique: list[Track] = []
        for t in tracks:
            key = (t.artist.lower(), t.title.lower())
            if key in seen:
                continue
            seen.add(key)
            unique.append(t)

        return unique[:max_tracks]

    def get_recommended_tracks(
        self,
        *,
        max_tracks: int,
        seed_count: int = 25,
        similar_per_seed: int = 5,
    ) -> list[Track]:
        """Generate a recommendation-style list using Last.fm similar tracks.

        Last.fm does not expose an official "Discover Weekly" track recommendation feed via API.
        This approximates it by taking your recent-week plays as seeds and calling `track.getSimilar`.
        """
        seeds = self.get_last_week_tracks(max_tracks=max(1, seed_count))
        if not seeds:
            return []

        # Get more seeds to increase variety
        seed_keys = {(t.artist.lower(), t.title.lower()) for t in seeds}
        
        # Pull extra seeds if needed to meet demand
        if len(seeds) < seed_count:
             logger.info("Boosting seed count from top tracks...")
             extra_seeds = self.get_top_tracks_recent(limit=seed_count)
             for s in extra_seeds:
                 k = (s.artist.lower(), s.title.lower())
                 if k not in seed_keys:
                     seeds.append(s)
                     seed_keys.add(k)
        
        # Cap seeds at requested count
        seeds = seeds[:seed_count]

        recommendations: list[Track] = []
        seen: set[tuple[str, str]] = set(seed_keys)

        for seed in seeds:
            params = {
                "method": "track.getSimilar",
                "artist": seed.artist,
                "track": seed.title,
                "api_key": self.api_key,
                "format": "json",
                "limit": similar_per_seed,
                "autocorrect": 1,
            }

            try:
                resp = requests.get("https://ws.audioscrobbler.com/2.0/", params=params, timeout=30)
                resp.raise_for_status()
                payload = resp.json()
            except Exception as e:
                logger.debug("track.getSimilar failed for %s — %s: %s", seed.artist, seed.title, e)
                continue

            tracks_raw = (
                payload.get("similartracks", {}).get("track", [])
                if isinstance(payload, dict)
                else []
            )
            if isinstance(tracks_raw, dict):
                tracks_raw = [tracks_raw]

            for item in tracks_raw:
                try:
                    title = str(item.get("name", "")).strip()
                    artist_obj = item.get("artist", {})
                    if isinstance(artist_obj, dict):
                        artist = str(artist_obj.get("name", "")).strip()
                    else:
                        artist = str(artist_obj).strip()
                except Exception:
                    continue

                if not title or not artist:
                    continue

                key = (artist.lower(), title.lower())
                if key in seen:
                    continue
                seen.add(key)
                recommendations.append(Track(artist=artist, title=title))
                if len(recommendations) >= max_tracks:
                    return recommendations

        return recommendations[:max_tracks]


    def get_top_artists_recent(self, limit: int = 10) -> list[str]:
        """Get user's top artists over the last month."""
        params = {
            "method": "user.getTopArtists",
            "user": self.username,
            "api_key": self.api_key,
            "format": "json",
            "period": "1month",
            "limit": limit,
        }
        try:
            resp = requests.get("https://ws.audioscrobbler.com/2.0/", params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            artists = data.get("topartists", {}).get("artist", [])
            if isinstance(artists, dict):
                artists = [artists]
            return [a["name"] for a in (artists or []) if a.get("name")]
        except Exception as e:
            logger.warning("Failed to get top artists: %s", e)
            return []

    def get_similar_artists(self, artist: str, limit: int = 10) -> list[str]:
        """Get artists similar to the given artist using artist.getSimilar."""
        params = {
            "method": "artist.getSimilar",
            "artist": artist,
            "api_key": self.api_key,
            "format": "json",
            "autocorrect": 1,
            "limit": limit,
        }
        try:
            resp = requests.get("https://ws.audioscrobbler.com/2.0/", params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            similar = data.get("similarartists", {}).get("artist", [])
            if isinstance(similar, dict):
                similar = [similar]
            return [a["name"] for a in (similar or []) if a.get("name")]
        except Exception as e:
            logger.warning("Failed to get similar artists for %s: %s", artist, e)
            return []

    def get_global_chart_tracks(self, limit: int = 50) -> list[dict]:
        """Get the global top tracks from chart.getTopTracks (no auth required)."""
        params = {
            "method": "chart.getTopTracks",
            "api_key": self.api_key,
            "format": "json",
            "limit": limit,
        }
        try:
            resp = requests.get("https://ws.audioscrobbler.com/2.0/", params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            tracks = data.get("tracks", {}).get("track", [])
            if isinstance(tracks, dict):
                tracks = [tracks]
            result = []
            for t in (tracks or []):
                name = t.get("name", "")
                artist_obj = t.get("artist", {})
                artist = artist_obj.get("name", "") if isinstance(artist_obj, dict) else str(artist_obj)
                if name and artist:
                    result.append({"name": name, "artist": artist, "type": "track", "cover_url": ""})
            return result
        except Exception as e:
            logger.warning("Failed to get global chart: %s", e)
            return []

    def get_geo_top_tracks(self, country: str = "United Kingdom", limit: int = 50) -> list[dict]:
        """Get top tracks for a country using geo.getTopTracks (no auth required)."""
        params = {
            "method": "geo.getTopTracks",
            "country": country,
            "api_key": self.api_key,
            "format": "json",
            "limit": limit,
        }
        try:
            resp = requests.get("https://ws.audioscrobbler.com/2.0/", params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            tracks = data.get("tracks", {}).get("track", [])
            if isinstance(tracks, dict):
                tracks = [tracks]
            result = []
            for t in (tracks or []):
                name = t.get("name", "")
                artist_obj = t.get("artist", {})
                artist = artist_obj.get("name", "") if isinstance(artist_obj, dict) else str(artist_obj)
                if name and artist:
                    result.append({"name": name, "artist": artist, "type": "track", "cover_url": ""})
            return result
        except Exception as e:
            logger.warning("Failed to get geo chart (%s): %s", country, e)
            return []


def summarize(tracks: Iterable[Track]) -> str:
    lines = [f"{t.artist} — {t.title}" for t in tracks]
    return "\n".join(lines)
