"""US data.gov open data catalog stats.

Tracks total datasets published on the US federal open data
platform. Growth reflects government data transparency efforts.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_API_URL = "https://catalog.data.gov/api/3/action/package_search"


class DataGovDatasetCountCollector(BaseCollector):
    meta = CollectorMeta(
        name="datagov_datasets",
        display_name="US data.gov Total Datasets",
        update_frequency="weekly",
        api_docs_url="https://catalog.data.gov/",
        domain="society",
        category="governance",
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(
            _API_URL,
            params={"rows": "0"},
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        count = resp.json().get("result", {}).get("count")
        if count is None:
            raise RuntimeError("No data.gov dataset count")
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": float(count)}])
