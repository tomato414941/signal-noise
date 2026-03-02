from __future__ import annotations

import io

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class OECDCLICollector(BaseCollector):
    """OECD Composite Leading Indicator (G7 aggregate, amplitude adjusted).

    Uses the OECD SDMX API (CSV format) for the DF_CLI dataflow.
    The OECD aggregate CLI is not published; G7 is used as the
    closest available proxy.
    """

    meta = CollectorMeta(
        name="oecd_cli",
        display_name="OECD Composite Leading Indicator (G7)",
        update_frequency="monthly",
        api_docs_url="https://data.oecd.org/leadind/composite-leading-indicator-cli.htm",
        domain="economy",
        category="economic",
    )

    URL = (
        "https://sdmx.oecd.org/public/rest/data/"
        "OECD.SDD.STES,DSD_STES@DF_CLI,4.1/"
        "G7.M.LI.IX._Z.AA.IX._Z.H"
        "?startPeriod=2000-01"
    )

    def fetch(self) -> pd.DataFrame:
        headers = {"Accept": "application/vnd.sdmx.data+csv"}
        resp = requests.get(self.URL, headers=headers, timeout=60)
        resp.raise_for_status()
        raw = pd.read_csv(io.StringIO(resp.text))
        if raw.empty:
            raise RuntimeError("No OECD CLI data")
        rows = []
        for _, row in raw.iterrows():
            try:
                period = str(row["TIME_PERIOD"])
                val = float(row["OBS_VALUE"])
                dt = pd.Timestamp(period + "-01", tz="UTC")
                rows.append({"date": dt, "value": val})
            except (KeyError, ValueError, TypeError):
                continue
        if not rows:
            raise RuntimeError("No parseable OECD CLI data")
        df = pd.DataFrame(rows)
        return df.sort_values("date").reset_index(drop=True)
