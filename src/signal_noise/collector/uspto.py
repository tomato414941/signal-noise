"""USPTO PatentsView API — patent grant and application counts."""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_PATENTSVIEW_URL = "https://api.patentsview.org/patents/query"


class USPTOPatentGrantsCollector(BaseCollector):
    """USPTO PatentsView — weekly US patent grants count."""

    meta = CollectorMeta(
        name="uspto_patent_grants",
        display_name="USPTO Weekly Patent Grants",
        update_frequency="weekly",
        api_docs_url="https://patentsview.org/apis/api-endpoints",
        domain="creativity",
        category="patents",
    )

    def fetch(self) -> pd.DataFrame:
        since = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=365)).strftime("%Y-%m-%d")
        payload = {
            "q": {"_gte": {"patent_date": since}},
            "f": ["patent_date"],
            "o": {"per_page": 1},
        }
        resp = requests.post(
            _PATENTSVIEW_URL,
            json=payload,
            timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        total = data.get("total_patent_count", 0)
        now = pd.Timestamp.now(tz="UTC").normalize()
        return pd.DataFrame([{"date": now, "value": total}])


class USPTOPatentAppsCollector(BaseCollector):
    """USPTO PatentsView — recent patent application volume."""

    meta = CollectorMeta(
        name="uspto_patent_apps",
        display_name="USPTO Patent Applications (Annual)",
        update_frequency="yearly",
        api_docs_url="https://patentsview.org/apis/api-endpoints",
        domain="creativity",
        category="patents",
    )

    URL = (
        "https://api.worldbank.org/v2/country/USA/indicator/IP.PAT.RESD"
        "?format=json&per_page=100&date=2000:2030"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        payload = resp.json()
        if len(payload) < 2 or not payload[1]:
            raise RuntimeError("No World Bank patent data")
        rows = []
        for entry in payload[1]:
            if entry.get("value") is None:
                continue
            try:
                year = int(entry["date"])
                dt = pd.Timestamp(year=year, month=1, day=1, tz="UTC")
                rows.append({"date": dt, "value": float(entry["value"])})
            except (KeyError, ValueError, TypeError):
                continue
        if not rows:
            raise RuntimeError("No patent application data")
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
