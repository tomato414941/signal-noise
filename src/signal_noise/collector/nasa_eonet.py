from __future__ import annotations

from collections import Counter

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta


class EONETWildfireCollector(BaseCollector):
    """Count of currently active wildfire events worldwide (NASA EONET)."""

    meta = SourceMeta(
        name="eonet_wildfires",
        display_name="NASA EONET: Active Wildfires",
        update_frequency="daily",
        api_docs_url="https://eonet.gsfc.nasa.gov/docs/v3",
        domain="geophysical",
        category="seismic",
    )

    def fetch(self) -> pd.DataFrame:
        url = "https://eonet.gsfc.nasa.gov/api/v3/events?status=open&limit=1000"
        resp = requests.get(url, timeout=self.config.request_timeout)
        resp.raise_for_status()
        events = resp.json().get("events", [])

        count = sum(
            1 for e in events
            if any(c["id"] == "wildfires" for c in e.get("categories", []))
        )

        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [float(count)]})


class EONETStormCollector(BaseCollector):
    """Count of currently active severe storm events (NASA EONET)."""

    meta = SourceMeta(
        name="eonet_storms",
        display_name="NASA EONET: Active Severe Storms",
        update_frequency="daily",
        api_docs_url="https://eonet.gsfc.nasa.gov/docs/v3",
        domain="geophysical",
        category="seismic",
    )

    def fetch(self) -> pd.DataFrame:
        url = "https://eonet.gsfc.nasa.gov/api/v3/events?status=open&limit=1000"
        resp = requests.get(url, timeout=self.config.request_timeout)
        resp.raise_for_status()
        events = resp.json().get("events", [])

        count = sum(
            1 for e in events
            if any(c["id"] == "severeStorms" for c in e.get("categories", []))
        )

        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [float(count)]})


class EONETVolcanoCollector(BaseCollector):
    """Count of currently active volcanic events (NASA EONET)."""

    meta = SourceMeta(
        name="eonet_volcanoes",
        display_name="NASA EONET: Active Volcanoes",
        update_frequency="daily",
        api_docs_url="https://eonet.gsfc.nasa.gov/docs/v3",
        domain="geophysical",
        category="seismic",
    )

    def fetch(self) -> pd.DataFrame:
        url = "https://eonet.gsfc.nasa.gov/api/v3/events?status=open&limit=1000"
        resp = requests.get(url, timeout=self.config.request_timeout)
        resp.raise_for_status()
        events = resp.json().get("events", [])

        count = sum(
            1 for e in events
            if any(c["id"] == "volcanoes" for c in e.get("categories", []))
        )

        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [float(count)]})


class EONETTotalCollector(BaseCollector):
    """Total count of all active natural events (NASA EONET)."""

    meta = SourceMeta(
        name="eonet_total",
        display_name="NASA EONET: Total Active Events",
        update_frequency="daily",
        api_docs_url="https://eonet.gsfc.nasa.gov/docs/v3",
        domain="geophysical",
        category="seismic",
    )

    def fetch(self) -> pd.DataFrame:
        url = "https://eonet.gsfc.nasa.gov/api/v3/events?status=open&limit=1000"
        resp = requests.get(url, timeout=self.config.request_timeout)
        resp.raise_for_status()
        events = resp.json().get("events", [])

        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [float(len(events))]})
