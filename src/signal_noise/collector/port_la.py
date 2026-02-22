from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class PortOfLACollector(BaseCollector):
    """Port of Los Angeles monthly container throughput (TEUs)."""

    meta = CollectorMeta(
        name="port_la_teus",
        display_name="Port of LA Monthly TEU Volume",
        update_frequency="monthly",
        api_docs_url="https://www.portoflosangeles.org/business/statistics",
        domain="infrastructure",
        category="logistics",
    )

    URL = "https://data.lacity.org/api/views/2cma-xas7/rows.json?accessType=DOWNLOAD"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        rows_data = data.get("data", [])
        cols = [c["fieldName"] for c in data.get("meta", {}).get("view", {}).get("columns", [])]
        if not rows_data:
            raise RuntimeError("No Port of LA data")
        rows = []
        for row in rows_data:
            try:
                # Find date and TEU columns
                entry = dict(zip(cols, row))
                date_str = entry.get("date") or entry.get("month") or str(row[8])
                teus = float(entry.get("teus") or entry.get("total_teus") or row[9])
                dt = pd.to_datetime(date_str, utc=True)
                rows.append({"date": dt.normalize(), "value": teus})
            except (KeyError, ValueError, TypeError, IndexError):
                continue
        if not rows:
            raise RuntimeError("No parseable Port of LA data")
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
