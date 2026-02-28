"""ISS (International Space Station) real-time position and crew count.

Snapshot signals — cannot be backfilled.
APIs: http://api.open-notify.org (free, no key)
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector._cache import SharedAPICache

_iss_cache = SharedAPICache(ttl=30)


class ISSLatitudeCollector(BaseCollector):
    """Current ISS latitude (-90 to 90)."""

    meta = CollectorMeta(
        name="iss_latitude",
        display_name="ISS Latitude",
        update_frequency="hourly",
        api_docs_url="http://api.open-notify.org",
        domain="infrastructure",
        category="space",
    )

    def fetch(self) -> pd.DataFrame:
        data = _iss_cache.get_or_fetch("position", _fetch_iss_position)
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [data["lat"]]})


class ISSLongitudeCollector(BaseCollector):
    """Current ISS longitude (-180 to 180)."""

    meta = CollectorMeta(
        name="iss_longitude",
        display_name="ISS Longitude",
        update_frequency="hourly",
        api_docs_url="http://api.open-notify.org",
        domain="infrastructure",
        category="space",
    )

    def fetch(self) -> pd.DataFrame:
        data = _iss_cache.get_or_fetch("position", _fetch_iss_position)
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [data["lon"]]})


class ISSCrewCountCollector(BaseCollector):
    """Number of people currently in space (ISS + Tiangong)."""

    meta = CollectorMeta(
        name="iss_crew_count",
        display_name="People In Space",
        update_frequency="hourly",
        api_docs_url="http://api.open-notify.org",
        domain="infrastructure",
        category="space",
    )

    def fetch(self) -> pd.DataFrame:
        data = _iss_cache.get_or_fetch("astros", _fetch_astros)
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [float(data)]})


def _fetch_iss_position() -> dict:
    resp = requests.get("http://api.open-notify.org/iss-now.json", timeout=15)
    resp.raise_for_status()
    pos = resp.json()["iss_position"]
    return {"lat": float(pos["latitude"]), "lon": float(pos["longitude"])}


def _fetch_astros() -> int:
    resp = requests.get("http://api.open-notify.org/astros.json", timeout=15)
    resp.raise_for_status()
    return resp.json()["number"]
