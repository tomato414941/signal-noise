"""Transport for London (TfL) bike sharing stats.

Tracks available bikes and docking stations across London's
Santander Cycles network. Availability reflects urban mobility
patterns and commuter activity.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_API_URL = "https://api.tfl.gov.uk/BikePoint"


class TfLBikeStationsCollector(BaseCollector):
    meta = CollectorMeta(
        name="tfl_bike_stations",
        display_name="London TfL Bike Stations",
        update_frequency="daily",
        api_docs_url="https://api.tfl.gov.uk/",
        domain="economy",
        category="tourism",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(_API_URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        points = resp.json()
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(len(points))}])


class TfLBikesAvailableCollector(BaseCollector):
    meta = CollectorMeta(
        name="tfl_bikes_available",
        display_name="London TfL Bikes Available",
        update_frequency="hourly",
        api_docs_url="https://api.tfl.gov.uk/",
        domain="economy",
        category="tourism",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(_API_URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        points = resp.json()
        total = sum(
            int(p.get("value", 0))
            for bp in points
            for p in bp.get("additionalProperties", [])
            if p.get("key") == "NbBikes"
        )
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(total)}])
