"""OpenStreetMap replication state collector.

Tracks the daily replication sequence number, which increments
with each batch of map edits. Growth rate reflects global
mapping community activity.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_STATE_URL = "https://planet.openstreetmap.org/replication/day/state.txt"


class OSMSequenceNumberCollector(BaseCollector):
    meta = CollectorMeta(
        name="osm_daily_sequence",
        display_name="OpenStreetMap Daily Replication Sequence",
        update_frequency="daily",
        api_docs_url="https://wiki.openstreetmap.org/wiki/Planet.osm/diffs",
        domain="technology",
        category="internet",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(_STATE_URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        seq = None
        for line in resp.text.strip().split("\n"):
            if line.startswith("sequenceNumber="):
                seq = int(line.split("=")[1].strip())
                break
        if seq is None:
            raise RuntimeError("No OSM sequence number found")
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(seq)}])
