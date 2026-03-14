"""Mastodon instance stats collectors.

Tracks user count, status count, and connected domains for
major Mastodon instances. Provides a direct view of
decentralized social network growth per server.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


def _make_mastodon_collector(
    name: str, display_name: str, instance: str, field: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url=f"https://{instance}/api/v1/instance",
            domain="sentiment",
            category="attention",
        )

        def fetch(self) -> pd.DataFrame:
            resp = requests.get(
                f"https://{instance}/api/v1/instance",
                timeout=self.config.request_timeout,
            )
            resp.raise_for_status()
            val = resp.json().get("stats", {}).get(field)
            if val is None:
                raise RuntimeError(f"No {field} for {instance}")
            now = pd.Timestamp.now(tz="UTC").normalize()
            return pd.DataFrame([{"date": now, "value": float(val)}])

    _Collector.__name__ = f"Mastodon_{name}"
    _Collector.__qualname__ = f"Mastodon_{name}"
    return _Collector


_SIGNALS: list[tuple[str, str, str, str]] = [
    ("mastodon_social_users", "Mastodon.social Users", "mastodon.social", "user_count"),
    ("mastodon_social_statuses", "Mastodon.social Total Statuses", "mastodon.social", "status_count"),
    ("mastodon_social_domains", "Mastodon.social Connected Domains", "mastodon.social", "domain_count"),
    ("fosstodon_users", "Fosstodon Users", "fosstodon.org", "user_count"),
]


def get_mastodon_stats_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_mastodon_collector(name, display, instance, field)
        for name, display, instance, field in _SIGNALS
    }
