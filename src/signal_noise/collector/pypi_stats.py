from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class PyPIDownloadsCollector(BaseCollector):
    """PyPI daily download count for numpy (proxy for Python ecosystem activity).

    Uses the pypistats.org API which provides recent daily
    download statistics aggregated across mirrors.
    """

    meta = CollectorMeta(
        name="pypi_numpy_downloads",
        display_name="PyPI numpy Daily Downloads",
        update_frequency="daily",
        api_docs_url="https://pypistats.org/api/",
        domain="developer",
        category="developer",
    )

    URL = "https://pypistats.org/api/packages/numpy/overall?mirrors=true"

    def fetch(self) -> pd.DataFrame:
        headers = {"User-Agent": "signal-noise/0.1"}
        resp = requests.get(
            self.URL, headers=headers, timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        items = resp.json().get("data", [])
        rows = []
        for item in items:
            if item.get("category") == "without_mirrors":
                continue
            try:
                rows.append({
                    "date": pd.Timestamp(item["date"], tz="UTC"),
                    "value": float(item["downloads"]),
                })
            except (KeyError, ValueError, TypeError):
                continue
        if not rows:
            raise RuntimeError("No PyPI download data")
        df = pd.DataFrame(rows)
        daily = df.groupby("date")["value"].sum().reset_index()
        return daily.sort_values("date").reset_index(drop=True)
