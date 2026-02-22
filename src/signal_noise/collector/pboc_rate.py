from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class PBOCLPRCollector(BaseCollector):
    """PBOC Loan Prime Rate (1-year LPR) from public data."""

    meta = CollectorMeta(
        name="pboc_lpr_1y",
        display_name="PBOC 1Y Loan Prime Rate (%)",
        update_frequency="monthly",
        api_docs_url="https://www.pboc.gov.cn/",
        domain="financial",
        category="rates",
    )

    URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=CHNLPR1Y"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        from io import StringIO
        raw = pd.read_csv(StringIO(resp.text))
        if raw.shape[1] < 2:
            raise RuntimeError("No PBOC LPR data")
        df = raw.iloc[:, :2].dropna().copy()
        df.columns = ["date", "value"]
        df["date"] = pd.to_datetime(df["date"], utc=True)
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna()
        return df.sort_values("date").reset_index(drop=True)
