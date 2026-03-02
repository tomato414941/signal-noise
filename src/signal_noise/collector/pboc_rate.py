from __future__ import annotations

from io import StringIO

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class PBOCLPRCollector(BaseCollector):
    """China central bank discount rate (monthly) via FRED CSV.

    Uses FRED series INTDSRCNM193N (Central Bank Discount Rate for China)
    as the original CHNLPR1Y series is unavailable.
    """

    meta = CollectorMeta(
        name="pboc_lpr_1y",
        display_name="PBOC Discount Rate (%)",
        update_frequency="monthly",
        api_docs_url="https://fred.stlouisfed.org/series/INTDSRCNM193N",
        domain="markets",
        category="rates",
    )

    URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=INTDSRCNM193N"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        if resp.text.strip().startswith("<!"):
            raise RuntimeError("FRED returned HTML instead of CSV")
        raw = pd.read_csv(StringIO(resp.text))
        if raw.shape[1] < 2:
            raise RuntimeError("No PBOC rate data from FRED")
        df = raw.iloc[:, :2].copy()
        df.columns = ["date", "value"]
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["value"])
        df["date"] = pd.to_datetime(df["date"], utc=True)
        return df.sort_values("date").reset_index(drop=True)
