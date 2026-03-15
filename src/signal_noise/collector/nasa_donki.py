"""NASA DONKI (Space Weather Database Of Notifications, Knowledge, Information).

Tracks space weather events: CMEs, solar flares, geomagnetic storms.
Uses DEMO_KEY — rate limited to 30 req/hour.
https://api.nasa.gov/
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_BASE = "https://api.nasa.gov/DONKI"
_API_KEY = "DEMO_KEY"


def _donki_count(endpoint: str, days: int = 30, *, timeout: int = 30) -> int:
    end = pd.Timestamp.now(tz="UTC").normalize()
    start = end - pd.Timedelta(days=days)
    resp = requests.get(
        f"{_BASE}/{endpoint}",
        params={
            "api_key": _API_KEY,
            "startDate": start.strftime("%Y-%m-%d"),
            "endDate": end.strftime("%Y-%m-%d"),
        },
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()
    return len(data) if isinstance(data, list) else 0


class DONKICMECollector(BaseCollector):
    meta = CollectorMeta(
        name="donki_cme_count",
        display_name="NASA DONKI CME Events (30d)",
        update_frequency="daily",
        api_docs_url="https://api.nasa.gov/",
        domain="environment",
        category="space_weather",
    )

    def fetch(self) -> pd.DataFrame:
        count = _donki_count("CME", timeout=self.config.request_timeout)
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(count)}])


class DONKIFlareCollector(BaseCollector):
    meta = CollectorMeta(
        name="donki_flare_count",
        display_name="NASA DONKI Solar Flare Events (30d)",
        update_frequency="daily",
        api_docs_url="https://api.nasa.gov/",
        domain="environment",
        category="space_weather",
    )

    def fetch(self) -> pd.DataFrame:
        count = _donki_count("FLR", timeout=self.config.request_timeout)
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(count)}])


class DONKIStormCollector(BaseCollector):
    meta = CollectorMeta(
        name="donki_geostorm_count",
        display_name="NASA DONKI Geomagnetic Storms (30d)",
        update_frequency="daily",
        api_docs_url="https://api.nasa.gov/",
        domain="environment",
        category="space_weather",
    )

    def fetch(self) -> pd.DataFrame:
        count = _donki_count("GST", timeout=self.config.request_timeout)
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(count)}])


class DONKISEPCollector(BaseCollector):
    meta = CollectorMeta(
        name="donki_sep_count",
        display_name="NASA DONKI Solar Energetic Particles (30d)",
        update_frequency="daily",
        api_docs_url="https://api.nasa.gov/",
        domain="environment",
        category="space_weather",
    )

    def fetch(self) -> pd.DataFrame:
        count = _donki_count("SEP", timeout=self.config.request_timeout)
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(count)}])
