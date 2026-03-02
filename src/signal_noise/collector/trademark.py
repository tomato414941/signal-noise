from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class USPTOTrademarkAppsCollector(BaseCollector):
    """USPTO — monthly US trademark application filings."""

    meta = CollectorMeta(
        name="uspto_trademark_apps",
        display_name="USPTO Monthly Trademark Applications",
        update_frequency="monthly",
        api_docs_url="https://developer.uspto.gov/api-catalog",
        domain="creativity",
        category="patents",
    )

    URL = "https://tsdrapi.uspto.gov/ts/cd/casestatus/search"

    def fetch(self) -> pd.DataFrame:
        now = pd.Timestamp.now(tz="UTC")
        rows = []
        for months_ago in range(12):
            target = now - pd.DateOffset(months=months_ago)
            start = target.replace(day=1).strftime("%Y-%m-%d")
            end_dt = (target.replace(day=1) + pd.DateOffset(months=1) - pd.Timedelta(days=1))
            end = end_dt.strftime("%Y-%m-%d")
            params = {
                "filingDateFrom": start,
                "filingDateTo": end,
                "start": 0,
                "rows": 0,
            }
            try:
                resp = requests.get(self.URL, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                count = data.get("response", {}).get("numFound", 0)
                rows.append({
                    "date": pd.to_datetime(start, utc=True),
                    "value": count,
                })
            except Exception:
                continue
        if not rows:
            raise RuntimeError("No USPTO trademark data")
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
