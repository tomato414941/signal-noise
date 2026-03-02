from __future__ import annotations

import io

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class TSACheckpointCollector(BaseCollector):
    """US airline load factor from FRED (proxy for air travel demand).

    The TSA passenger volumes page blocks automated requests.
    This uses the FRED LOADFACTOR series (US domestic airline
    passenger load factor, monthly %) as a proxy for air travel demand.
    """

    meta = CollectorMeta(
        name="tsa_traveler_count",
        display_name="US Airline Load Factor (%, monthly)",
        update_frequency="monthly",
        api_docs_url="https://fred.stlouisfed.org/series/LOADFACTOR",
        domain="technology",
        category="aviation",
    )

    URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=LOADFACTOR&cosd=2010-01-01"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text))
        if df.empty:
            raise RuntimeError("No FRED LOADFACTOR data")
        df.columns = ["date", "value"]
        df["date"] = pd.to_datetime(df["date"], utc=True)
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        result = df.dropna().copy()
        if result.empty:
            raise RuntimeError("No parseable FRED LOADFACTOR data")
        return result.sort_values("date").reset_index(drop=True)
