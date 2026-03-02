from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector._cache import SharedAPICache

_eonet_cache = SharedAPICache(ttl=3600)

_EONET_BASE = "https://eonet.gsfc.nasa.gov/api/v3"


def _get_events(timeout: int = 30) -> list[dict]:
    def _fetch() -> list[dict]:
        resp = requests.get(
            f"{_EONET_BASE}/events",
            params={"status": "open", "limit": 500},
            timeout=timeout,
        )
        resp.raise_for_status()
        return resp.json().get("events", [])
    return _eonet_cache.get_or_fetch("events", _fetch)


def _count_category(events: list[dict], cat_id: str) -> int:
    return sum(
        1 for e in events
        if any(c["id"] == cat_id for c in e.get("categories", []))
    )


class EONETWildfireCollector(BaseCollector):
    meta = CollectorMeta(
        name="eonet_wildfires",
        display_name="NASA EONET: Active Wildfires",
        update_frequency="daily",
        api_docs_url="https://eonet.gsfc.nasa.gov/docs/v3",
        domain="geophysical",
        category="seismic",
    )

    def fetch(self) -> pd.DataFrame:
        events = _get_events(timeout=max(self.config.request_timeout, 30))
        count = _count_category(events, "wildfires")
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [float(count)]})


class EONETStormCollector(BaseCollector):
    meta = CollectorMeta(
        name="eonet_storms",
        display_name="NASA EONET: Active Severe Storms",
        update_frequency="daily",
        api_docs_url="https://eonet.gsfc.nasa.gov/docs/v3",
        domain="geophysical",
        category="seismic",
    )

    def fetch(self) -> pd.DataFrame:
        events = _get_events(timeout=max(self.config.request_timeout, 30))
        count = _count_category(events, "severeStorms")
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [float(count)]})


class EONETVolcanoCollector(BaseCollector):
    meta = CollectorMeta(
        name="eonet_volcanoes",
        display_name="NASA EONET: Active Volcanoes",
        update_frequency="daily",
        api_docs_url="https://eonet.gsfc.nasa.gov/docs/v3",
        domain="geophysical",
        category="seismic",
    )

    def fetch(self) -> pd.DataFrame:
        events = _get_events(timeout=max(self.config.request_timeout, 30))
        count = _count_category(events, "volcanoes")
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [float(count)]})


class EONETTotalCollector(BaseCollector):
    meta = CollectorMeta(
        name="eonet_total",
        display_name="NASA EONET: Total Active Events",
        update_frequency="daily",
        api_docs_url="https://eonet.gsfc.nasa.gov/docs/v3",
        domain="geophysical",
        category="seismic",
    )

    def fetch(self) -> pd.DataFrame:
        events = _get_events(timeout=max(self.config.request_timeout, 30))
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [float(len(events))]})
