from __future__ import annotations

import io

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class PortOfLACollector(BaseCollector):
    """US monthly imports volume from FRED as a container logistics proxy.

    The original Port of LA Socrata dataset (2cma-xas7) is no longer
    available. This uses the US imports of goods volume (monthly, USD)
    from FRED (XTIMVA01USM667S) as a proxy for port container throughput.
    """

    meta = CollectorMeta(
        name="port_la_teus",
        display_name="US Monthly Imports Volume (FRED)",
        update_frequency="monthly",
        api_docs_url="https://fred.stlouisfed.org/series/XTIMVA01USM667S",
        domain="infrastructure",
        category="logistics",
    )

    URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=XTIMVA01USM667S&cosd=2010-01-01"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text))
        if df.empty:
            raise RuntimeError("No FRED imports data")
        df.columns = ["date", "value"]
        df["date"] = pd.to_datetime(df["date"], utc=True)
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        result = df.dropna().copy()
        if result.empty:
            raise RuntimeError("No parseable FRED imports data")
        return result.sort_values("date").reset_index(drop=True)
