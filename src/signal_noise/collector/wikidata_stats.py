"""Wikidata and Wikipedia siteinfo stats collectors.

Tracks total items, edits, and active users across Wikimedia
projects. Growth rates reflect knowledge creation velocity
and community engagement.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_HEADERS = {"User-Agent": "signal-noise/1.0 (time series research project)"}


def _fetch_siteinfo(base_url: str, timeout: int) -> dict:
    resp = requests.get(
        f"{base_url}/w/api.php",
        params={"action": "query", "meta": "siteinfo", "siprop": "statistics", "format": "json"},
        headers=_HEADERS,
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json().get("query", {}).get("statistics", {})


def _make_wiki_collector(
    name: str, display_name: str, base_url: str, field: str,
    domain: str = "technology", category: str = "internet",
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url=f"{base_url}/w/api.php",
            domain=domain,
            category=category,
        )

        def fetch(self) -> pd.DataFrame:
            stats = _fetch_siteinfo(base_url, timeout=self.config.request_timeout)
            val = stats.get(field)
            if val is None:
                raise RuntimeError(f"No {field} in siteinfo for {name}")
            now = pd.Timestamp.now(tz="UTC").normalize()
            return pd.DataFrame([{"date": now, "value": float(val)}])

    _Collector.__name__ = f"Wiki_{name}"
    _Collector.__qualname__ = f"Wiki_{name}"
    return _Collector


_SIGNALS: list[tuple[str, str, str, str]] = [
    ("wikidata_items", "Wikidata Total Items", "https://www.wikidata.org", "articles"),
    ("wikidata_edits", "Wikidata Total Edits", "https://www.wikidata.org", "edits"),
    ("enwiki_articles", "English Wikipedia Articles", "https://en.wikipedia.org", "articles"),
    ("enwiki_edits", "English Wikipedia Total Edits", "https://en.wikipedia.org", "edits"),
    ("enwiki_active_users", "English Wikipedia Active Users", "https://en.wikipedia.org", "activeusers"),
    ("jawiki_articles", "Japanese Wikipedia Articles", "https://ja.wikipedia.org", "articles"),
    ("commons_files", "Wikimedia Commons Total Files", "https://commons.wikimedia.org", "images"),
    ("enwiktionary_entries", "English Wiktionary Entries", "https://en.wiktionary.org", "articles"),
]


def get_wikidata_stats_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_wiki_collector(name, display, url, field)
        for name, display, url, field in _SIGNALS
    }
