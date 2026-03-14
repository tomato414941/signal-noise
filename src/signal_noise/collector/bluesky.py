"""Bluesky / AT Protocol network stats via bsky-search.jazco.io.

Tracks daily post volume, active posters, likes, and follows
on the Bluesky decentralized social network.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_STATS_URL = "https://bsky-search.jazco.io/stats"

_cache: dict | None = None
_cache_ts: float = 0.0


def _fetch_stats(timeout: int = 30) -> dict:
    global _cache, _cache_ts
    import time
    now = time.monotonic()
    if _cache is not None and (now - _cache_ts) < 600:
        return _cache
    resp = requests.get(_STATS_URL, timeout=timeout)
    resp.raise_for_status()
    _cache = resp.json()
    _cache_ts = now
    return _cache


def _build_daily_df(stats: dict, field: str) -> pd.DataFrame:
    daily = stats.get("daily_data", [])
    if not daily:
        raise RuntimeError(f"No Bluesky daily data for {field}")
    rows = []
    for d in daily:
        date_str = d.get("date")
        val = d.get(field)
        if date_str and val is not None:
            rows.append({"date": pd.Timestamp(date_str, tz="UTC"), "value": float(val)})
    if not rows:
        raise RuntimeError(f"No Bluesky data for field {field}")
    return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


def _make_bluesky_collector(
    name: str, display_name: str, field: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://bsky-search.jazco.io/",
            domain="sentiment",
            category="attention",
        )

        def fetch(self) -> pd.DataFrame:
            stats = _fetch_stats(timeout=self.config.request_timeout)
            return _build_daily_df(stats, field)

    _Collector.__name__ = f"Bluesky_{name}"
    _Collector.__qualname__ = f"Bluesky_{name}"
    return _Collector


_SIGNALS: list[tuple[str, str, str]] = [
    ("bsky_daily_posts", "Bluesky Daily Posts", "num_posts"),
    ("bsky_daily_posters", "Bluesky Daily Active Posters", "num_posters"),
    ("bsky_daily_likes", "Bluesky Daily Likes", "num_likes"),
    ("bsky_daily_follows", "Bluesky Daily Follows", "num_follows"),
    ("bsky_total_users", "Bluesky Total Users", "total_users"),
]


class BlueskyTotalUsersCollector(BaseCollector):
    meta = CollectorMeta(
        name="bsky_total_users",
        display_name="Bluesky Total Users",
        update_frequency="daily",
        api_docs_url="https://bsky-search.jazco.io/",
        domain="sentiment",
        category="attention",
    )

    def fetch(self) -> pd.DataFrame:
        stats = _fetch_stats(timeout=self.config.request_timeout)
        total = stats.get("total_users")
        if not total:
            raise RuntimeError("No Bluesky total_users")
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(total)}])


def get_bluesky_collectors() -> dict[str, type[BaseCollector]]:
    collectors = {}
    for name, display, field in _SIGNALS:
        if name == "bsky_total_users":
            continue
        collectors[name] = _make_bluesky_collector(name, display, field)
    return collectors
