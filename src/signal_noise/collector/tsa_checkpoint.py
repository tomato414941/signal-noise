from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class TSACheckpointCollector(BaseCollector):
    """TSA daily checkpoint travel numbers (proxy for US air travel demand)."""

    meta = CollectorMeta(
        name="tsa_traveler_count",
        display_name="TSA Daily Checkpoint Travelers",
        update_frequency="daily",
        api_docs_url="https://www.tsa.gov/travel/passenger-volumes",
        domain="infrastructure",
        category="aviation",
    )

    URL = "https://www.tsa.gov/travel/passenger-volumes"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        # TSA publishes data in an HTML table; parse it
        tables = pd.read_html(resp.text)
        if not tables:
            raise RuntimeError("No TSA checkpoint data")
        df = tables[0]
        # Expected columns: Date, current year numbers, comparison year
        date_col = df.columns[0]
        value_col = df.columns[1]
        result = df[[date_col, value_col]].dropna().copy()
        result.columns = ["date", "value"]
        result["date"] = pd.to_datetime(result["date"], utc=True)
        result["value"] = result["value"].astype(str).str.replace(",", "").astype(float)
        return result.sort_values("date").reset_index(drop=True)
