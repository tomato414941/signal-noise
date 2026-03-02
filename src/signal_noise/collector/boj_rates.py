from __future__ import annotations

from io import StringIO

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class BOJPolicyRateCollector(BaseCollector):
    """Bank of Japan policy rate (monthly) via FRED CSV."""

    meta = CollectorMeta(
        name="boj_policy_rate",
        display_name="BOJ Policy Rate (%)",
        update_frequency="monthly",
        api_docs_url="https://fred.stlouisfed.org/series/IRSTCB01JPM156N",
        domain="markets",
        category="rates",
    )

    URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=IRSTCB01JPM156N"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        if resp.text.strip().startswith("<!"):
            raise RuntimeError("FRED returned HTML instead of CSV")
        raw = pd.read_csv(StringIO(resp.text))
        if raw.shape[1] < 2:
            raise RuntimeError("No BOJ rate data from FRED")
        df = raw.iloc[:, :2].copy()
        df.columns = ["date", "value"]
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["value"])
        df["date"] = pd.to_datetime(df["date"], utc=True)
        return df.sort_values("date").reset_index(drop=True)
