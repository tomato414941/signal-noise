"""USPTO patent and application counts via World Bank indicators."""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class USPTOPatentGrantsCollector(BaseCollector):
    """US patent grants (annual) via World Bank indicator."""

    meta = CollectorMeta(
        name="uspto_patent_grants",
        display_name="US Patent Grants (Annual)",
        update_frequency="yearly",
        api_docs_url="https://data.worldbank.org/indicator/IP.PAT.RESD",
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
            raise RuntimeError("No World Bank patent grant data")
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
            raise RuntimeError("No patent grant data")
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


class USPTOPatentAppsCollector(BaseCollector):
    """Global patent applications by non-residents (annual) via World Bank."""

    meta = CollectorMeta(
        name="uspto_patent_apps",
        display_name="Global Patent Applications Non-Resident (Annual)",
        update_frequency="yearly",
        api_docs_url="https://data.worldbank.org/indicator/IP.PAT.NRES",
        domain="creativity",
        category="patents",
    )

    URL = (
        "https://api.worldbank.org/v2/country/WLD/indicator/IP.PAT.NRES"
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
