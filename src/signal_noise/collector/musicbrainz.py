"""MusicBrainz open music database stats.

Tracks total releases and artists in the open music encyclopedia.
Growth reflects music metadata curation and industry cataloging.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_API_URL = "https://musicbrainz.org/ws/2"
_HEADERS = {"User-Agent": "signal-noise/1.0 (time series research project)"}


def _make_musicbrainz_collector(
    name: str, display_name: str, entity: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://musicbrainz.org/doc/MusicBrainz_API",
            domain="sentiment",
            category="attention",
        )

        def fetch(self) -> pd.DataFrame:
            resp = requests.get(
                f"{_API_URL}/{entity}",
                params={"query": "*", "limit": "1", "fmt": "json"},
                headers=_HEADERS,
                timeout=self.config.request_timeout,
            )
            resp.raise_for_status()
            count = resp.json().get("count")
            if count is None:
                raise RuntimeError(f"No MusicBrainz count for {entity}")
            now = pd.Timestamp.now(tz="UTC").normalize()
            return pd.DataFrame([{"date": now, "value": float(count)}])

    _Collector.__name__ = f"MusicBrainz_{name}"
    _Collector.__qualname__ = f"MusicBrainz_{name}"
    return _Collector


_SIGNALS: list[tuple[str, str, str]] = [
    ("musicbrainz_releases", "MusicBrainz Total Releases", "release"),
    ("musicbrainz_artists", "MusicBrainz Total Artists", "artist"),
    ("musicbrainz_recordings", "MusicBrainz Total Recordings", "recording"),
]


def get_musicbrainz_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_musicbrainz_collector(name, display, entity)
        for name, display, entity in _SIGNALS
    }
