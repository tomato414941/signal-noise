from __future__ import annotations

from io import StringIO

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class ECBDepositRateCollector(BaseCollector):
    """ECB deposit facility rate (daily) via SDMX CSV API."""

    meta = CollectorMeta(
        name="ecb_deposit_rate",
        display_name="ECB Deposit Facility Rate (%)",
        update_frequency="daily",
        api_docs_url="https://data.ecb.europa.eu/",
        domain="markets",
        category="rates",
    )

    URL = (
        "https://data-api.ecb.europa.eu/service/data/FM/D.U2.EUR.4F.KR.DFR.LEV"
        "?format=csvdata&startPeriod=2000-01-01"
    )

    def fetch(self) -> pd.DataFrame:
        headers = {"Accept": "text/csv"}
        resp = requests.get(
            self.URL, headers=headers, timeout=60
        )
        resp.raise_for_status()
        raw = pd.read_csv(StringIO(resp.text))
        # CSV columns include TIME_PERIOD and OBS_VALUE
        if "TIME_PERIOD" not in raw.columns or "OBS_VALUE" not in raw.columns:
            raise RuntimeError("Unexpected ECB CSV format")
        df = raw[["TIME_PERIOD", "OBS_VALUE"]].dropna().copy()
        df.columns = ["date", "value"]
        df["date"] = pd.to_datetime(df["date"], utc=True)
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna()
        return df.sort_values("date").reset_index(drop=True)
