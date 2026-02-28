from __future__ import annotations


import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector._cache import SharedAPICache

_opensky_cache = SharedAPICache(ttl=120)


def _fetch_opensky_states() -> list:
    resp = requests.get(
        "https://opensky-network.org/api/states/all",
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json().get("states", [])


class OpenSkyTotalCollector(BaseCollector):
    """Total aircraft currently in flight worldwide (OpenSky Network)."""

    meta = CollectorMeta(
        name="opensky_total",
        display_name="OpenSky: Aircraft In Flight (Total)",
        update_frequency="hourly",
        api_docs_url="https://openskynetwork.github.io/opensky-api/",
        domain="infrastructure",
        category="aviation",
    )

    def fetch(self) -> pd.DataFrame:
        states = _opensky_cache.get_or_fetch("states_all", _fetch_opensky_states)
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [float(len(states))]})


class OpenSkyUSCollector(BaseCollector):
    """Aircraft currently in flight with US origin (OpenSky Network)."""

    meta = CollectorMeta(
        name="opensky_us",
        display_name="OpenSky: US Aircraft In Flight",
        update_frequency="hourly",
        api_docs_url="https://openskynetwork.github.io/opensky-api/",
        domain="infrastructure",
        category="aviation",
    )

    def fetch(self) -> pd.DataFrame:
        states = _opensky_cache.get_or_fetch("states_all", _fetch_opensky_states)
        us_count = sum(1 for s in states if s[2] == "United States")
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [float(us_count)]})


class FR24TotalCollector(BaseCollector):
    """Total aircraft tracked by FlightRadar24."""

    meta = CollectorMeta(
        name="fr24_total",
        display_name="FlightRadar24: Total Aircraft Tracked",
        update_frequency="hourly",
        api_docs_url="https://www.flightradar24.com/",
        domain="infrastructure",
        category="aviation",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            "https://data-cloud.flightradar24.com/zones/fcgi/feed.js",
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        total = data.get("full_count", 0)

        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [float(total)]})
