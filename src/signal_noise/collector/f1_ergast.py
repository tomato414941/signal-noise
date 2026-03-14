"""Formula 1 race stats via Ergast/Jolpi API.

Tracks the number of races in the current F1 season and
cumulative race count across all seasons.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_API_URL = "https://api.jolpi.ca/ergast/f1"


class F1CurrentSeasonRacesCollector(BaseCollector):
    meta = CollectorMeta(
        name="f1_season_races",
        display_name="F1 Current Season Races",
        update_frequency="weekly",
        api_docs_url="https://api.jolpi.ca/ergast/",
        domain="sentiment",
        category="attention",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            f"{_API_URL}/current.json",
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        races = resp.json().get("MRData", {}).get("RaceTable", {}).get("Races", [])
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(len(races))}])


class F1TotalRacesCollector(BaseCollector):
    meta = CollectorMeta(
        name="f1_total_races",
        display_name="F1 All-Time Total Races",
        update_frequency="weekly",
        api_docs_url="https://api.jolpi.ca/ergast/",
        domain="sentiment",
        category="attention",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            f"{_API_URL}/races.json",
            params={"limit": "1", "offset": "0"},
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        total = resp.json().get("MRData", {}).get("total")
        if total is None:
            raise RuntimeError("No F1 total race count")
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(total)}])
