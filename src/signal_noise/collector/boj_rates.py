from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class BOJPolicyRateCollector(BaseCollector):
    """Bank of Japan policy rate (overnight call rate)."""

    meta = CollectorMeta(
        name="boj_policy_rate",
        display_name="BOJ Policy Rate (%)",
        update_frequency="daily",
        api_docs_url="https://www.stat-search.boj.or.jp/",
        domain="financial",
        category="rates",
    )

    URL = (
        "https://www.stat-search.boj.or.jp/ssi/mtshtml/fm02_m_1.csv"
    )

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        rows = []
        for line in resp.text.strip().split("\n"):
            parts = line.strip().split(",")
            if len(parts) >= 2:
                try:
                    date_str = parts[0].strip().strip('"')
                    val = float(parts[1].strip().strip('"'))
                    dt = pd.Timestamp(date_str, tz="UTC")
                    rows.append({"date": dt, "value": val})
                except (ValueError, TypeError):
                    continue
        if not rows:
            raise RuntimeError("No BOJ rate data")
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
