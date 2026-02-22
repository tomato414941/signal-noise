from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class CBOEVIXCollector(BaseCollector):
    """CBOE VIX volatility index (current value snapshot)."""

    meta = CollectorMeta(
        name="vix_close",
        display_name="CBOE VIX Volatility Index",
        update_frequency="daily",
        api_docs_url="https://www.cboe.com/tradable_products/vix/",
        domain="sentiment",
        category="sentiment",
    )

    URL = "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        from io import StringIO
        raw = pd.read_csv(StringIO(resp.text))
        col_map = {c: c.strip().upper() for c in raw.columns}
        raw.rename(columns=col_map, inplace=True)
        close_col = "CLOSE" if "CLOSE" in raw.columns else "VIX CLOSE"
        date_col = "DATE" if "DATE" in raw.columns else raw.columns[0]
        df = raw[[date_col, close_col]].dropna().copy()
        df.columns = ["date", "value"]
        df["date"] = pd.to_datetime(df["date"], utc=True)
        df["value"] = df["value"].astype(float)
        return df.sort_values("date").reset_index(drop=True)
