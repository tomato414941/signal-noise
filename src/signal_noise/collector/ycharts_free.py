from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class FedBalanceSheetCollector(BaseCollector):
    """Fed balance sheet total assets (weekly, from FRED public CSV)."""

    meta = CollectorMeta(
        name="fed_balance_sheet",
        display_name="Fed Balance Sheet Total Assets",
        update_frequency="weekly",
        api_docs_url="https://fred.stlouisfed.org/series/WALCL",
        domain="macro",
        category="fiscal",
    )

    URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=WALCL"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        from io import StringIO
        raw = pd.read_csv(StringIO(resp.text))
        if raw.shape[1] < 2:
            raise RuntimeError("No FRED balance sheet data")
        df = raw.iloc[:, :2].dropna().copy()
        df.columns = ["date", "value"]
        df["date"] = pd.to_datetime(df["date"], utc=True)
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna()
        return df.sort_values("date").reset_index(drop=True)
