from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class LibrariesIOCollector(BaseCollector):
    """Libraries.io — recently published platform package count (daily snapshot)."""

    meta = CollectorMeta(
        name="librariesio_new_packages",
        display_name="Libraries.io New Package Releases",
        update_frequency="daily",
        api_docs_url="https://libraries.io/api",
        domain="developer",
        category="developer",
    )

    URL = "https://libraries.io/api/search?sort=created_at&order=desc&per_page=100"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        packages = resp.json()
        if not packages:
            raise RuntimeError("No Libraries.io data")
        date_counts: dict[str, int] = {}
        for p in packages:
            created = (p.get("created_at") or "")[:10]
            if created:
                date_counts[created] = date_counts.get(created, 0) + 1
        rows = [
            {"date": pd.Timestamp(d, tz="UTC"), "value": float(c)}
            for d, c in date_counts.items()
        ]
        if not rows:
            raise RuntimeError("No parseable Libraries.io data")
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
