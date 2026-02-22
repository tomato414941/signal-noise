from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class CratesIODownloadsCollector(BaseCollector):
    """crates.io daily download count for serde (proxy for Rust ecosystem activity).

    Uses the crates.io API to fetch per-version daily downloads
    for the serde crate, then aggregates by date.
    """

    meta = CollectorMeta(
        name="crates_serde_downloads",
        display_name="crates.io serde Daily Downloads",
        update_frequency="daily",
        api_docs_url="https://crates.io/policies",
        domain="developer",
        category="developer",
    )

    URL = "https://crates.io/api/v1/crates/serde/downloads"

    def fetch(self) -> pd.DataFrame:
        headers = {"User-Agent": "signal-noise/0.1 (research project)"}
        resp = requests.get(
            self.URL, headers=headers, timeout=self.config.request_timeout,
        )
        resp.raise_for_status()
        entries = resp.json().get("version_downloads", [])
        rows = []
        for entry in entries:
            try:
                rows.append({
                    "date": pd.Timestamp(entry["date"], tz="UTC"),
                    "value": float(entry["downloads"]),
                })
            except (KeyError, ValueError, TypeError):
                continue
        if not rows:
            raise RuntimeError("No crates.io download data")
        df = pd.DataFrame(rows)
        daily = df.groupby("date")["value"].sum().reset_index()
        return daily.sort_values("date").reset_index(drop=True)
