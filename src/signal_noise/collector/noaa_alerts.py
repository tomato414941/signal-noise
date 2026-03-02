"""NOAA active weather alerts count — hourly snapshot signals.

Snapshot signals — cannot be backfilled.
API: https://www.weather.gov/documentation/services-web-api
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector._cache import SharedAPICache

_noaa_cache = SharedAPICache(ttl=120)

_ALERTS_COUNT_URL = "https://api.weather.gov/alerts/active/count"
_HEADERS = {"User-Agent": "signal-noise/1.0 (github.com/tomato414941/signal-noise)"}


def _fetch_alert_counts() -> dict:
    resp = requests.get(_ALERTS_COUNT_URL, headers=_HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    return {
        "total": int(data["total"]),
        "land": int(data["land"]),
        "marine": int(data["marine"]),
    }


class NOAAAlertsTotalCollector(BaseCollector):
    """Total number of active NOAA weather alerts (US)."""

    meta = CollectorMeta(
        name="noaa_alerts_total",
        display_name="NOAA Alerts (Total)",
        update_frequency="hourly",
        api_docs_url="https://www.weather.gov/documentation/services-web-api",
        domain="environment",
        category="weather",
    )

    def fetch(self) -> pd.DataFrame:
        data = _noaa_cache.get_or_fetch("alert_counts", _fetch_alert_counts)
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [float(data["total"])]})


class NOAAAlertLandCollector(BaseCollector):
    """Number of active NOAA land weather alerts (US)."""

    meta = CollectorMeta(
        name="noaa_alerts_land",
        display_name="NOAA Alerts (Land)",
        update_frequency="hourly",
        api_docs_url="https://www.weather.gov/documentation/services-web-api",
        domain="environment",
        category="weather",
    )

    def fetch(self) -> pd.DataFrame:
        data = _noaa_cache.get_or_fetch("alert_counts", _fetch_alert_counts)
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [float(data["land"])]})


class NOAAAlertMarineCollector(BaseCollector):
    """Number of active NOAA marine weather alerts (US)."""

    meta = CollectorMeta(
        name="noaa_alerts_marine",
        display_name="NOAA Alerts (Marine)",
        update_frequency="hourly",
        api_docs_url="https://www.weather.gov/documentation/services-web-api",
        domain="environment",
        category="weather",
    )

    def fetch(self) -> pd.DataFrame:
        data = _noaa_cache.get_or_fetch("alert_counts", _fetch_alert_counts)
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [float(data["marine"])]})
