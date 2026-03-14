"""USGS Earthquake Hazards Program collectors.

Tracks daily earthquake counts and maximum magnitude at various thresholds.
Uses the FDSNWS event query API.  No API key required.

Docs: https://earthquake.usgs.gov/fdsnws/event/1/
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

_BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

# (min_magnitude, name_suffix, display_suffix)
_THRESHOLDS: list[tuple[float, str, str]] = [
    (2.5, "m25", "M2.5+"),
    (4.5, "m45", "M4.5+"),
    (5.5, "m55", "M5.5+"),
]

_LOOKBACK_DAYS = 90


def _make_earthquake_count_collector(
    min_mag: float, suffix: str, display_suffix: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=f"usgs_eq_count_{suffix}",
            display_name=f"USGS Earthquake Count ({display_suffix})",
            update_frequency="daily",
            api_docs_url="https://earthquake.usgs.gov/fdsnws/event/1/",
            domain="environment",
            category="seismic",
        )

        def fetch(self) -> pd.DataFrame:
            end = datetime.now(UTC)
            start = end - timedelta(days=_LOOKBACK_DAYS)
            resp = requests.get(
                _BASE_URL,
                params={
                    "format": "geojson",
                    "starttime": start.strftime("%Y-%m-%d"),
                    "endtime": end.strftime("%Y-%m-%d"),
                    "minmagnitude": min_mag,
                },
                timeout=self.config.request_timeout,
            )
            resp.raise_for_status()
            features = resp.json().get("features", [])

            daily: dict[str, list[float]] = {}
            for f in features:
                ts = f["properties"].get("time")
                mag = f["properties"].get("mag")
                if ts is None or mag is None:
                    continue
                day = pd.to_datetime(ts, unit="ms", utc=True).strftime("%Y-%m-%d")
                daily.setdefault(day, []).append(float(mag))

            rows = [
                {"date": pd.to_datetime(day, utc=True), "value": float(len(mags))}
                for day, mags in daily.items()
            ]
            if not rows:
                raise RuntimeError(f"No earthquake data for {display_suffix}")
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"USGSEqCount_{suffix}"
    _Collector.__qualname__ = f"USGSEqCount_{suffix}"
    return _Collector


def _make_earthquake_max_mag_collector(
    min_mag: float, suffix: str, display_suffix: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=f"usgs_eq_maxmag_{suffix}",
            display_name=f"USGS Max Magnitude ({display_suffix})",
            update_frequency="daily",
            api_docs_url="https://earthquake.usgs.gov/fdsnws/event/1/",
            domain="environment",
            category="seismic",
        )

        def fetch(self) -> pd.DataFrame:
            end = datetime.now(UTC)
            start = end - timedelta(days=_LOOKBACK_DAYS)
            resp = requests.get(
                _BASE_URL,
                params={
                    "format": "geojson",
                    "starttime": start.strftime("%Y-%m-%d"),
                    "endtime": end.strftime("%Y-%m-%d"),
                    "minmagnitude": min_mag,
                    "orderby": "magnitude",
                },
                timeout=self.config.request_timeout,
            )
            resp.raise_for_status()
            features = resp.json().get("features", [])

            daily: dict[str, float] = {}
            for f in features:
                ts = f["properties"].get("time")
                mag = f["properties"].get("mag")
                if ts is None or mag is None:
                    continue
                day = pd.to_datetime(ts, unit="ms", utc=True).strftime("%Y-%m-%d")
                if day not in daily or float(mag) > daily[day]:
                    daily[day] = float(mag)

            rows = [
                {"date": pd.to_datetime(day, utc=True), "value": mag}
                for day, mag in daily.items()
            ]
            if not rows:
                raise RuntimeError(f"No earthquake data for {display_suffix}")
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"USGSEqMaxMag_{suffix}"
    _Collector.__qualname__ = f"USGSEqMaxMag_{suffix}"
    return _Collector


def get_usgs_earthquake_collectors() -> dict[str, type[BaseCollector]]:
    collectors: dict[str, type[BaseCollector]] = {}
    for min_mag, suffix, display in _THRESHOLDS:
        collectors[f"usgs_eq_count_{suffix}"] = _make_earthquake_count_collector(
            min_mag, suffix, display,
        )
        collectors[f"usgs_eq_maxmag_{suffix}"] = _make_earthquake_max_mag_collector(
            min_mag, suffix, display,
        )
    return collectors
