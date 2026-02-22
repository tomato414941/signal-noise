from __future__ import annotations

import io

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class FreightosBDICollector(BaseCollector):
    """US trade balance on goods (monthly) from FRED as a freight proxy.

    The Freightos API no longer offers free public access.
    This uses the FRED BOPGSTB series (balance on goods and
    services trade, monthly) as a proxy for global container
    freight activity.
    """

    meta = CollectorMeta(
        name="freightos_bdi",
        display_name="US Goods Trade Balance (FRED, monthly)",
        update_frequency="daily",
        api_docs_url="https://fred.stlouisfed.org/series/BOPGSTB",
        domain="infrastructure",
        category="logistics",
    )

    URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=BOPGSTB&cosd=2015-01-01"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text))
        if df.empty:
            raise RuntimeError("No FRED trade data")
        df.columns = ["date", "value"]
        df["date"] = pd.to_datetime(df["date"], utc=True)
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        result = df.dropna().copy()
        if result.empty:
            raise RuntimeError("No parseable FRED trade data")
        # Use absolute value for magnitude
        result["value"] = result["value"].abs()
        return result.sort_values("date").reset_index(drop=True)
