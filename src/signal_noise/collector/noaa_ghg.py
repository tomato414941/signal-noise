from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class CH4GlobalCollector(BaseCollector):
    """Global monthly CH4 (methane) concentration from NOAA GML."""

    meta = CollectorMeta(
        name="ch4_monthly_global",
        display_name="CH4 Monthly Global Average (ppb)",
        update_frequency="monthly",
        api_docs_url="https://gml.noaa.gov/ccgg/trends_ch4/",
        domain="environment",
        category="climate",
    )

    URL = "https://gml.noaa.gov/webdata/ccgg/trends/ch4/ch4_mm_gl.txt"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        rows = []
        for line in resp.text.strip().split("\n"):
            if line.startswith("#") or not line.strip():
                continue
            parts = line.split()
            if len(parts) >= 4:
                try:
                    year, month = int(parts[0]), int(parts[1])
                    value = float(parts[3])
                    if value > 0:
                        dt = pd.Timestamp(year=year, month=month, day=1, tz="UTC")
                        rows.append({"date": dt, "value": value})
                except (ValueError, TypeError):
                    continue
        if not rows:
            raise RuntimeError("No CH4 data")
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


class N2OGlobalCollector(BaseCollector):
    """Global monthly N2O (nitrous oxide) concentration from NOAA GML."""

    meta = CollectorMeta(
        name="n2o_monthly_global",
        display_name="N2O Monthly Global Average (ppb)",
        update_frequency="monthly",
        api_docs_url="https://gml.noaa.gov/ccgg/trends_n2o/",
        domain="environment",
        category="climate",
    )

    URL = "https://gml.noaa.gov/webdata/ccgg/trends/n2o/n2o_mm_gl.txt"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        rows = []
        for line in resp.text.strip().split("\n"):
            if line.startswith("#") or not line.strip():
                continue
            parts = line.split()
            if len(parts) >= 4:
                try:
                    year, month = int(parts[0]), int(parts[1])
                    value = float(parts[3])
                    if value > 0:
                        dt = pd.Timestamp(year=year, month=month, day=1, tz="UTC")
                        rows.append({"date": dt, "value": value})
                except (ValueError, TypeError):
                    continue
        if not rows:
            raise RuntimeError("No N2O data")
        return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
