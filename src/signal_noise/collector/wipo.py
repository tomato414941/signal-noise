from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class WIPOPatentFilingsCollector(BaseCollector):
    """WIPO — annual PCT international patent filings (global total)."""

    meta = CollectorMeta(
        name="wipo_patent_filings",
        display_name="WIPO Global PCT Patent Filings",
        update_frequency="yearly",
        api_docs_url="https://www.wipo.int/ipstats/",
        domain="creativity",
        category="patents",
    )

    URL = (
        "https://www3.wipo.int/ipstats/keysearch/indicator"
        "?indicator=2&type=1&format=json"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict):
            data = data.get("dataSets", data.get("data", []))
        if not data:
            raise RuntimeError("No WIPO patent filing data available")
        rows = []
        if isinstance(data, list):
            for entry in data:
                try:
                    year = int(entry.get("year", entry.get("Year", 0)))
                    val = float(entry.get("value", entry.get("Value", 0)))
                    if year > 0:
                        dt = pd.Timestamp(year=year, month=1, day=1, tz="UTC")
                        rows.append({"date": dt, "value": val})
                except (ValueError, TypeError):
                    continue
        if not rows:
            raise RuntimeError("No parseable WIPO data")
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
