"""Flathub Linux app distribution stats.

Tracks total downloads, app count, and verified apps on the
primary Flatpak app store. Growth reflects Linux desktop adoption.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_API_URL = "https://flathub.org/api/v2/stats"


def _make_flathub_collector(
    name: str, display_name: str, field: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://flathub.org/",
            domain="technology",
            category="developer",
        )

        def fetch(self) -> pd.DataFrame:
            resp = requests.get(_API_URL, timeout=self.config.request_timeout)
            resp.raise_for_status()
            val = resp.json().get("totals", {}).get(field)
            if val is None:
                raise RuntimeError(f"No Flathub data for {field}")
            now = pd.Timestamp.now(tz="UTC").normalize()
            return pd.DataFrame([{"date": now, "value": float(val)}])

    _Collector.__name__ = f"Flathub_{name}"
    _Collector.__qualname__ = f"Flathub_{name}"
    return _Collector


_SIGNALS: list[tuple[str, str, str]] = [
    ("flathub_downloads", "Flathub Total Downloads", "downloads"),
    ("flathub_apps", "Flathub App Count", "number_of_apps"),
    ("flathub_verified", "Flathub Verified Apps", "verified_apps"),
]


def get_flathub_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_flathub_collector(name, display, field)
        for name, display, field in _SIGNALS
    }
