"""Library organiser — scans a music library and normalises folder/file names
to match SoundScout's naming scheme (Artist / Album / Title.ext).

Algorithm
---------
1. Walk ``library_path`` at two levels: Artist → Album.
2. For each album folder, strip a trailing ``(YYYY)`` year suffix to get the
   *canonical* album name.
3. If the canonical name differs from the current folder name this folder is
   "dirty" and needs to be renamed/merged.
4. When a correctly-named target folder already exists the dirty folder is
   *merged* into it (tracks moved, then source folder removed when empty).
5. Track filenames are normalised to just the bare title by stripping known
   prefixes (artist, album, leading track-number) deduced from the folder names.
"""

from __future__ import annotations

import logging
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

AUDIO_EXTENSIONS = {".flac", ".mp3", ".m4a", ".aac", ".ogg", ".opus", ".wav", ".alac"}

# Matches a trailing "(YYYY)" year annotation on album folder names.
_YEAR_SUFFIX_RE = re.compile(r"\s*\(\d{4}\)\s*$")

# Matches a leading track-number prefix in filenames, e.g. "01. ", "02 - ", "3 ".
_TRACK_NUM_RE = re.compile(r"^\d{1,3}[\s.\-]+")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class TrackChange:
    old_name: str  # filename only (for display)
    new_name: str  # filename only (for display)
    old_path: str  # absolute path
    new_path: str  # absolute path
    conflict: bool = False  # True → target already exists; this track will be skipped


@dataclass
class AlbumChange:
    artist: str      # artist folder name (for display)
    old_album: str   # current album folder name (for display)
    new_album: str   # canonical (target) album folder name (for display)
    old_folder: str  # absolute path of source folder
    new_folder: str  # absolute path of target folder
    is_merge: bool   # True when target folder already existed at scan time
    track_changes: list[TrackChange] = field(default_factory=list)

    @property
    def folder_rename_needed(self) -> bool:
        return self.old_folder != self.new_folder

    @property
    def tracks_to_change(self) -> int:
        return sum(
            1
            for t in self.track_changes
            if not t.conflict and t.old_path != t.new_path
        )

    @property
    def tracks_skipped(self) -> int:
        return sum(1 for t in self.track_changes if t.conflict)


@dataclass
class ScanResult:
    album_changes: list[AlbumChange] = field(default_factory=list)
    # Albums that are already perfectly named (folder + all tracks correct).
    already_clean: int = 0


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _strip_year_suffix(name: str) -> str:
    """Remove a trailing ``(YYYY)`` year suffix from an album folder name."""
    return _YEAR_SUFFIX_RE.sub("", name).strip()


def _is_audio(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() in AUDIO_EXTENSIONS


def _extract_title(stem: str, artist: str, album: str) -> str:
    """Derive a clean track title from a filename stem.

    Tries several heuristic patterns in order, using the known *artist* and
    *album* names (from the containing folder names) as anchors:

    1. ``{artist} - {album} - {nn} - {title}``
    2. ``{artist} - {album} - {title}``
    3. ``{artist} - {nn} - {title}``
    4. ``{artist} - {title}``
    5. ``{title} - {artist}``  (reversed order)
    6. Leading track number only  (``01. Title``, ``01 - Title``)

    Returns the original *stem* unchanged if no pattern matches (already clean).
    """

    def _esc(n: str) -> str:
        return re.escape(n.strip())

    # 1. Artist - Album - NN - Title
    m = re.match(
        r"^" + _esc(artist) + r"\s*-\s*" + _esc(album) + r"\s*-\s*\d{1,3}\s*-\s*",
        stem,
        re.IGNORECASE,
    )
    if m:
        result = stem[m.end():]
        if result:
            return result

    # 2. Artist - Album - Title
    m = re.match(
        r"^" + _esc(artist) + r"\s*-\s*" + _esc(album) + r"\s*-\s*",
        stem,
        re.IGNORECASE,
    )
    if m:
        result = stem[m.end():]
        if result:
            return result

    # 3. Artist - NN - Title
    m = re.match(
        r"^" + _esc(artist) + r"\s*-\s*\d{1,3}\s*-\s*",
        stem,
        re.IGNORECASE,
    )
    if m:
        result = stem[m.end():]
        if result:
            return result

    # 4. Artist - Title
    m = re.match(r"^" + _esc(artist) + r"\s*-\s*", stem, re.IGNORECASE)
    if m:
        result = stem[m.end():]
        if result:
            return result

    # 5. Title - Artist  (reversed)
    m = re.search(r"\s*-\s*" + _esc(artist) + r"\s*$", stem, re.IGNORECASE)
    if m:
        result = stem[: m.start()].strip()
        if result:
            return result

    # 6. Leading track number
    m = _TRACK_NUM_RE.match(stem)
    if m:
        result = stem[m.end():]
        if result:
            return result

    return stem  # already clean


def _compute_target_name(f: Path, artist: str, album: str) -> str:
    """Return the normalised filename (title + lowercased extension) for *f*."""
    title = _extract_title(f.stem, artist, album)
    return title + f.suffix.lower()


def _collect_track_changes(
    source_dir: Path,
    target_dir: Path,
    artist: str,
    album: str,
) -> list[TrackChange]:
    """Build a ``TrackChange`` list for every audio file in *source_dir*.

    *target_dir* is where each file will land (may equal *source_dir* for
    in-place renames).
    """
    changes: list[TrackChange] = []
    for f in sorted(source_dir.iterdir()):
        if not _is_audio(f):
            continue
        target_name = _compute_target_name(f, artist, album)
        new_path = target_dir / target_name
        # Skip if source and destination are the same file.
        if new_path == f:
            continue
        changes.append(
            TrackChange(
                old_name=f.name,
                new_name=target_name,
                old_path=str(f),
                new_path=str(new_path),
                conflict=new_path.exists() and new_path != f,
            )
        )
    return changes


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def scan_library(library_path: Path) -> ScanResult:
    """Walk *library_path* and return a :class:`ScanResult` describing every
    folder/file rename or merge that is needed to normalise the library.

    Expected library structure::

        library_path/
            Artist Name/
                Album Name (2023)/    ← dirty: needs renaming → "Album Name"
                    Artist Name - Album Name - 05 - Track.flac
                Album Name/           ← clean folder (may still have dirty tracks)
                    Track.flac

    Disc-per-subfolder structures (``CD1/``, ``Disc 1/`` etc.) are not touched;
    only the first two directory levels below *library_path* are inspected.
    """
    result = ScanResult()

    if not library_path.exists() or not library_path.is_dir():
        return result

    for artist_dir in sorted(library_path.iterdir()):
        if not artist_dir.is_dir():
            continue

        artist_name = artist_dir.name

        # Group album folders by their canonical (year-stripped) name so we
        # can detect duplicates such as "Album" + "Album (2023)".
        canon_map: dict[str, list[Path]] = {}
        for album_dir in sorted(artist_dir.iterdir()):
            if not album_dir.is_dir():
                continue
            canon = _strip_year_suffix(album_dir.name)
            canon_map.setdefault(canon, []).append(album_dir)

        for canon_name, album_dirs in canon_map.items():
            target_path = artist_dir / canon_name
            clean_dirs = [d for d in album_dirs if d.name == canon_name]
            dirty_dirs = [d for d in album_dirs if d.name != canon_name]

            # --- Already-correctly-named folders: scan tracks only ---
            for d in clean_dirs:
                changes = _collect_track_changes(d, d, artist_name, canon_name)
                if changes:
                    result.album_changes.append(
                        AlbumChange(
                            artist=artist_name,
                            old_album=d.name,
                            new_album=d.name,
                            old_folder=str(d),
                            new_folder=str(d),
                            is_merge=False,
                            track_changes=changes,
                        )
                    )
                else:
                    # Folder name correct *and* all track names correct.
                    result.already_clean += 1

            # --- Dirty (year-suffixed) folders: rename or merge ---
            # is_merge is True if a correctly-named folder already exists
            # (either a pre-existing clean dir or a sibling dirty folder that
            # processed first will create it).
            is_merge = target_path.exists() or bool(clean_dirs)
            for d in dirty_dirs:
                changes = _collect_track_changes(d, target_path, artist_name, canon_name)
                result.album_changes.append(
                    AlbumChange(
                        artist=artist_name,
                        old_album=d.name,
                        new_album=canon_name,
                        old_folder=str(d),
                        new_folder=str(target_path),
                        is_merge=is_merge,
                        track_changes=changes,
                    )
                )

    return result


def apply_changes(scan: ScanResult, dry_run: bool = False) -> dict:
    """Apply the normalisation changes described in *scan*.

    Parameters
    ----------
    scan:
        Result returned by :func:`scan_library`.
    dry_run:
        When ``True``, count changes but do not touch the filesystem.

    Returns
    -------
    dict
        Summary with keys ``files_renamed``, ``files_moved``, ``files_skipped``,
        ``folders_renamed``, ``folders_merged``, ``dry_run``, ``errors``.
    """
    files_renamed = 0   # in-place renames within the same folder
    files_moved = 0     # moved to a different (merged/renamed) folder
    files_skipped = 0   # no change needed, or conflict with an existing file
    folders_renamed = 0
    folders_merged = 0
    errors: list[str] = []

    for ac in scan.album_changes:
        old_folder = Path(ac.old_folder)
        new_folder = Path(ac.new_folder)
        same_folder = old_folder == new_folder

        # Create the target folder when it's different from the source.
        if not same_folder and not dry_run:
            try:
                new_folder.mkdir(parents=True, exist_ok=True)
            except Exception as exc:
                errors.append(f"mkdir '{ac.new_album}': {exc}")
                continue

        # Move/rename each track file.
        for tc in ac.track_changes:
            old_p = Path(tc.old_path)
            new_p = Path(tc.new_path)

            if old_p == new_p:
                files_skipped += 1
                continue

            if tc.conflict:
                logger.info("Skipping '%s' — target '%s' already exists", tc.old_name, tc.new_name)
                files_skipped += 1
                continue

            try:
                if not dry_run:
                    new_p.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(old_p), str(new_p))
                if same_folder:
                    files_renamed += 1
                else:
                    files_moved += 1
            except Exception as exc:
                errors.append(f"move '{tc.old_name}': {exc}")

        # Remove the source folder when it differs from the target (after all
        # audio has been moved away or skipped).
        if not same_folder:
            if ac.is_merge:
                folders_merged += 1
            else:
                folders_renamed += 1

            if not dry_run and old_folder.exists():
                try:
                    remaining_audio = [f for f in old_folder.iterdir() if _is_audio(f)]
                    if not remaining_audio:
                        shutil.rmtree(str(old_folder), ignore_errors=True)
                except Exception as exc:
                    errors.append(f"rmdir '{ac.old_album}': {exc}")

    return {
        "files_renamed": files_renamed,
        "files_moved": files_moved,
        "files_skipped": files_skipped,
        "folders_renamed": folders_renamed,
        "folders_merged": folders_merged,
        "dry_run": dry_run,
        "errors": errors,
    }
