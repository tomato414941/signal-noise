from __future__ import annotations

from collections import Counter

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta


class OpenSkyTotalCollector(BaseCollector):
    """Total aircraft currently in flight worldwide (OpenSky Network)."""

    meta = SourceMeta(
        name="opensky_total",
        display_name="OpenSky: Aircraft In Flight (Total)",
        update_frequency="hourly",
        data_type="aviation",
        api_docs_url="https://openskynetwork.github.io/opensky-api/",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            "https://opensky-network.org/api/states/all",
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        states = data.get("states", [])
        total = len(states)

        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [float(total)]})


class OpenSkyUSCollector(BaseCollector):
    """Aircraft currently in flight with US origin (OpenSky Network)."""

    meta = SourceMeta(
        name="opensky_us",
        display_name="OpenSky: US Aircraft In Flight",
        update_frequency="hourly",
        data_type="aviation",
        api_docs_url="https://openskynetwork.github.io/opensky-api/",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            "https://opensky-network.org/api/states/all",
            timeout=60,
        )
        resp.raise_for_status()
        states = resp.json().get("states", [])
        us_count = sum(1 for s in states if s[2] == "United States")

        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [float(us_count)]})


class FR24TotalCollector(BaseCollector):
    """Total aircraft tracked by FlightRadar24."""

    meta = SourceMeta(
        name="fr24_total",
        display_name="FlightRadar24: Total Aircraft Tracked",
        update_frequency="hourly",
        data_type="aviation",
        api_docs_url="https://www.flightradar24.com/",
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
