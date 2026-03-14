"""RIPE Atlas internet measurement infrastructure collectors.

Tracks active hardware probes and measurement count worldwide.
Drops in active probes correlate with power outages, natural disasters,
and political internet shutdowns.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_ATLAS_URL = "https://atlas.ripe.net/api/v2"


class RIPEAtlasActiveProbesCollector(BaseCollector):
    meta = CollectorMeta(
        name="ripe_atlas_active_probes",
        display_name="RIPE Atlas Active Probes",
        update_frequency="daily",
        api_docs_url="https://atlas.ripe.net/docs/apis/",
        domain="technology",
        category="internet",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            f"{_ATLAS_URL}/probes/",
            params={"status": "1", "page_size": "1", "format": "json"},
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        count = resp.json().get("count")
        if count is None:
            raise RuntimeError("No RIPE Atlas probe count")
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(count)}])


class RIPEAtlasActiveMeasurementsCollector(BaseCollector):
    meta = CollectorMeta(
        name="ripe_atlas_active_measurements",
        display_name="RIPE Atlas Active Measurements",
        update_frequency="daily",
        api_docs_url="https://atlas.ripe.net/docs/apis/",
        domain="technology",
        category="internet",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            f"{_ATLAS_URL}/measurements/",
            params={"status": "2", "page_size": "1", "format": "json"},
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        count = resp.json().get("count")
        if count is None:
            raise RuntimeError("No RIPE Atlas measurement count")
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(count)}])
