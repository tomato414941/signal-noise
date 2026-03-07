from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

GBIF_LOOKBACK_DAYS = 7


class GBIFOccurrenceCollector(BaseCollector):
    """GBIF — daily global biodiversity occurrence record count."""

    meta = CollectorMeta(
        name="gbif_occurrence_count",
        display_name="GBIF Daily Occurrence Records",
        update_frequency="daily",
        api_docs_url="https://www.gbif.org/developer/occurrence",
        domain="environment",
        category="biodiversity",
    )

    URL = "https://api.gbif.org/v1/occurrence/search"

    def fetch(self) -> pd.DataFrame:
        now = pd.Timestamp.now(tz="UTC")
        rows = []
        for days_ago in range(GBIF_LOOKBACK_DAYS):
            date = (now - pd.Timedelta(days=days_ago)).strftime("%Y-%m-%d")
            params = {
                "eventDate": date,
                "limit": 0,
            }
            resp = requests.get(self.URL, params=params, timeout=self.config.request_timeout)
            resp.raise_for_status()
            count = resp.json().get("count", 0)
            rows.append({
                "date": pd.to_datetime(date, utc=True),
                "value": count,
            })
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
