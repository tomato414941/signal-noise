"""FediDB Fediverse network stats.

Tracks total users, monthly active users, total posts, and instance
count across the entire Fediverse (Mastodon, Misskey, Lemmy, etc.).
Provides a macro view of decentralized social network adoption.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_API_URL = "https://api.fedidb.org/v1/stats"


def _make_fedidb_collector(
    name: str, display_name: str, field: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://fedidb.org/",
            domain="sentiment",
            category="attention",
        )

        def fetch(self) -> pd.DataFrame:
            resp = requests.get(_API_URL, timeout=self.config.request_timeout)
            resp.raise_for_status()
            data = resp.json()
            val = data.get(field)
            if val is None:
                raise RuntimeError(f"No FediDB data for {field}")
            now = pd.Timestamp.now(tz="UTC").normalize()
            return pd.DataFrame([{"date": now, "value": float(val)}])

    _Collector.__name__ = f"FediDB_{name}"
    _Collector.__qualname__ = f"FediDB_{name}"
    return _Collector


_SIGNALS: list[tuple[str, str, str]] = [
    ("fediverse_total_users", "Fediverse Total Users", "total_users"),
    ("fediverse_mau", "Fediverse Monthly Active Users", "monthly_active_users"),
    ("fediverse_total_posts", "Fediverse Total Posts", "total_statuses"),
    ("fediverse_instances", "Fediverse Total Instances", "total_instances"),
]


def get_fedidb_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_fedidb_collector(name, display, field)
        for name, display, field in _SIGNALS
    }
