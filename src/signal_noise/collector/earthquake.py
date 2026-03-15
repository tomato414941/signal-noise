from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector._cache import SharedAPICache

_usgs_feed_cache = SharedAPICache(ttl=120)


class EarthquakeCountCollector(BaseCollector):
    """Daily count of M4.5+ earthquakes worldwide (USGS)."""

    meta = CollectorMeta(
        name="earthquake_count",
        display_name="Daily M4.5+ Earthquake Count",
        update_frequency="daily",
        api_docs_url="https://earthquake.usgs.gov/fdsnws/event/1/",
        domain="environment",
        category="seismic",
    )

    URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

    def fetch(self) -> pd.DataFrame:
        end = pd.Timestamp.now(tz="UTC")
        start = end - pd.Timedelta(days=365 * 2)
        resp = requests.get(
            self.URL,
            params={
                "format": "geojson",
                "starttime": start.strftime("%Y-%m-%d"),
                "endtime": end.strftime("%Y-%m-%d"),
                "minmagnitude": 4.5,
            },
            timeout=60,
        )
        resp.raise_for_status()
        features = resp.json()["features"]

        events = []
        for f in features:
            ts = pd.to_datetime(f["properties"]["time"], unit="ms", utc=True)
            events.append(ts.normalize())

        if not events:
            return pd.DataFrame(columns=["date", "value"])

        sr = pd.Series(events, name="date")
        daily = sr.value_counts().reset_index()
        daily.columns = ["date", "value"]
        daily["value"] = daily["value"].astype(float)
        return daily.sort_values("date").reset_index(drop=True)


class _USGSHourlyBase(BaseCollector):
    """Base for USGS real-time feed snapshot collectors.

    Uses GeoJSON summary feeds (free, no key).
    These are snapshot signals — value at collection time cannot be backfilled.
    """

    _feed_url: str = ""

    def _get_features(self) -> list[dict]:
        def _fetch() -> list[dict]:
            resp = requests.get(
                self._feed_url,
                timeout=self.config.request_timeout,
            )
            resp.raise_for_status()
            return resp.json().get("features", [])

        return _usgs_feed_cache.get_or_fetch(self._feed_url, _fetch)


class USGSQuakeCount24hCollector(_USGSHourlyBase):
    """Total earthquake count (all magnitudes) worldwide in the last 24h.

    Hourly snapshot of current global seismic activity level.
    """

    _feed_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"

    meta = CollectorMeta(
        name="usgs_quake_count_24h",
        display_name="USGS Earthquakes (24h count)",
        update_frequency="hourly",
        api_docs_url="https://earthquake.usgs.gov/earthquakes/feed/",
        domain="environment",
        category="seismic",
    )

    def fetch(self) -> pd.DataFrame:
        features = self._get_features()
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [float(len(features))]})


class USGSQuakeM45Count24hCollector(_USGSHourlyBase):
    """Count of M4.5+ earthquakes worldwide in the last 24h."""

    _feed_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.geojson"

    meta = CollectorMeta(
        name="usgs_quake_m45_count_24h",
        display_name="USGS M4.5+ Quakes (24h)",
        update_frequency="hourly",
        api_docs_url="https://earthquake.usgs.gov/earthquakes/feed/",
        domain="environment",
        category="seismic",
    )

    def fetch(self) -> pd.DataFrame:
        features = self._get_features()
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [float(len(features))]})


class USGSQuakeMaxMag24hCollector(_USGSHourlyBase):
    """Maximum earthquake magnitude worldwide in the last 24h.

    Captures the largest seismic event; spikes indicate major earthquakes.
    """

    _feed_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"

    meta = CollectorMeta(
        name="usgs_quake_max_mag_24h",
        display_name="USGS Max Magnitude (24h)",
        update_frequency="hourly",
        api_docs_url="https://earthquake.usgs.gov/earthquakes/feed/",
        domain="environment",
        category="seismic",
    )

    def fetch(self) -> pd.DataFrame:
        features = self._get_features()
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        max_mag = 0.0
        for f in features:
            mag = f.get("properties", {}).get("mag")
            if mag is not None and mag > max_mag:
                max_mag = float(mag)
        return pd.DataFrame({"timestamp": [ts], "value": [max_mag]})


class USGSQuakeFeltReports24hCollector(_USGSHourlyBase):
    """Total felt reports across all earthquakes in the last 24h."""

    _feed_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"

    meta = CollectorMeta(
        name="usgs_quake_felt_reports_24h",
        display_name="USGS Felt Reports (24h)",
        update_frequency="hourly",
        api_docs_url="https://earthquake.usgs.gov/earthquakes/feed/",
        domain="environment",
        category="seismic",
    )

    def fetch(self) -> pd.DataFrame:
        features = self._get_features()
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        total_felt = 0.0
        for f in features:
            felt = f.get("properties", {}).get("felt")
            if felt is not None:
                total_felt += float(felt)
        return pd.DataFrame({"timestamp": [ts], "value": [total_felt]})


class USGSQuakeSignificantCollector(_USGSHourlyBase):
    """Count of significant earthquakes worldwide in the last 24h.

    USGS classifies events as significant based on magnitude, PAGER alert,
    and felt reports. Typically 0-5 per day.
    """

    _feed_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_day.geojson"

    meta = CollectorMeta(
        name="usgs_quake_significant",
        display_name="USGS Significant Quakes (24h)",
        update_frequency="hourly",
        api_docs_url="https://earthquake.usgs.gov/earthquakes/feed/",
        domain="environment",
        category="seismic",
    )

    def fetch(self) -> pd.DataFrame:
        features = self._get_features()
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [float(len(features))]})


class USGSQuakeM25Count24hCollector(_USGSHourlyBase):
    """Count of M2.5+ earthquakes worldwide in the last 24h.

    Lower threshold captures moderate seismic activity patterns.
    """

    _feed_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson"

    meta = CollectorMeta(
        name="usgs_quake_m25_count_24h",
        display_name="USGS M2.5+ Quakes (24h)",
        update_frequency="hourly",
        api_docs_url="https://earthquake.usgs.gov/earthquakes/feed/",
        domain="environment",
        category="seismic",
    )

    def fetch(self) -> pd.DataFrame:
        features = self._get_features()
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [float(len(features))]})


class USGSQuakeM10Count24hCollector(_USGSHourlyBase):
    """Count of M1.0+ earthquakes worldwide in the last 24h.

    Captures micro-seismic activity; useful for detecting swarm patterns.
    """

    _feed_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/1.0_day.geojson"

    meta = CollectorMeta(
        name="usgs_quake_m10_count_24h",
        display_name="USGS M1.0+ Quakes (24h)",
        update_frequency="hourly",
        api_docs_url="https://earthquake.usgs.gov/earthquakes/feed/",
        domain="environment",
        category="seismic",
    )

    def fetch(self) -> pd.DataFrame:
        features = self._get_features()
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [float(len(features))]})


class USGSQuakeSignificant7dCollector(_USGSHourlyBase):
    """Count of significant earthquakes worldwide in the last 7 days.

    Wider window captures major seismic events that may be missed in 24h snapshots.
    """

    _feed_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_week.geojson"

    meta = CollectorMeta(
        name="usgs_quake_significant_7d",
        display_name="USGS Significant Quakes (7d)",
        update_frequency="hourly",
        api_docs_url="https://earthquake.usgs.gov/earthquakes/feed/",
        domain="environment",
        category="seismic",
    )

    def fetch(self) -> pd.DataFrame:
        features = self._get_features()
        ts = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame({"timestamp": [ts], "value": [float(len(features))]})
