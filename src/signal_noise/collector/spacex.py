"""SpaceX launch and Starlink satellite stats.

Tracks cumulative launches, upcoming launch count, and active
Starlink satellites. Reflects commercial space industry velocity.
"""
from __future__ import annotations

import time

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_API_URL = "https://api.spacexdata.com/v4"

_launches_cache: list | None = None
_launches_cache_ts: float = 0.0


def _fetch_launches(timeout: int = 30) -> list[dict]:
    global _launches_cache, _launches_cache_ts
    now = time.monotonic()
    if _launches_cache is not None and (now - _launches_cache_ts) < 600:
        return _launches_cache
    resp = requests.get(f"{_API_URL}/launches", timeout=timeout)
    resp.raise_for_status()
    _launches_cache = resp.json()
    _launches_cache_ts = now
    return _launches_cache


class SpaceXTotalLaunchesCollector(BaseCollector):
    meta = CollectorMeta(
        name="spacex_total_launches",
        display_name="SpaceX Total Launches",
        update_frequency="daily",
        api_docs_url="https://github.com/r-spacex/SpaceX-API",
        domain="technology",
        category="space",
    )

    def fetch(self) -> pd.DataFrame:
        launches = _fetch_launches(timeout=self.config.request_timeout)
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(len(launches))}])


class SpaceXSuccessRateCollector(BaseCollector):
    meta = CollectorMeta(
        name="spacex_success_rate",
        display_name="SpaceX Launch Success Rate",
        update_frequency="daily",
        api_docs_url="https://github.com/r-spacex/SpaceX-API",
        domain="technology",
        category="space",
    )

    def fetch(self) -> pd.DataFrame:
        launches = _fetch_launches(timeout=self.config.request_timeout)
        completed = [l for l in launches if l.get("success") is not None]
        if not completed:
            raise RuntimeError("No SpaceX launch data")
        rate = sum(1 for l in completed if l["success"]) / len(completed)
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": round(rate, 4)}])


class SpaceXUpcomingCollector(BaseCollector):
    meta = CollectorMeta(
        name="spacex_upcoming",
        display_name="SpaceX Upcoming Launches",
        update_frequency="daily",
        api_docs_url="https://github.com/r-spacex/SpaceX-API",
        domain="technology",
        category="space",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            f"{_API_URL}/launches/upcoming",
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(len(resp.json()))}])


class StarlinkActiveCollector(BaseCollector):
    meta = CollectorMeta(
        name="starlink_active",
        display_name="Starlink Active Satellites",
        update_frequency="daily",
        api_docs_url="https://github.com/r-spacex/SpaceX-API",
        domain="technology",
        category="space",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            f"{_API_URL}/starlink",
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        sats = resp.json()
        active = sum(1 for s in sats if s.get("latitude") is not None)
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(active)}])
