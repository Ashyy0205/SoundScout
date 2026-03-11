from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Track:
    artist: str
    title: str
    playcount: int | None = None
