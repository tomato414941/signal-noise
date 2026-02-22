from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class SubmarineCableCollector(BaseCollector):
    """Cumulative count of submarine internet cables by ready-for-service year.

    Uses TeleGeography's public cable dataset to build a
    cumulative timeline of undersea cable deployments.
    """

    meta = CollectorMeta(
        name="submarine_cable_count",
        display_name="Submarine Internet Cables (cumulative)",
        update_frequency="monthly",
        api_docs_url="https://www.submarinecablemap.com/",
        domain="infrastructure",
        category="internet",
    )

    URL = (
        "https://raw.githubusercontent.com/telegeography/www.submarinecablemap.com"
        "/master/web/public/api/v3/cable/all.json"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        cables = resp.json()
        year_counts: dict[int, int] = {}
        for cable in cables:
            rfs = cable.get("rfs")
            if rfs and isinstance(rfs, (int, float)):
                year = int(rfs)
                year_counts[year] = year_counts.get(year, 0) + 1
        if not year_counts:
            raise RuntimeError("No submarine cable data")
        rows = []
        cumulative = 0
        for year in sorted(year_counts):
            cumulative += year_counts[year]
            rows.append({
                "date": pd.Timestamp(f"{year}-01-01", tz="UTC"),
                "value": float(cumulative),
            })
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
