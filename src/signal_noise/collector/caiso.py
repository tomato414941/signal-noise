from __future__ import annotations

import io
import zipfile
from datetime import datetime, timedelta, UTC

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

_OASIS_URL = "https://oasis.caiso.com/oasisapi/SingleZip"


def _make_caiso_collector(
    hub: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://oasis.caiso.com/",
            domain="economy",
            category="energy",
        )

        def fetch(self) -> pd.DataFrame:
            end = datetime.now(UTC)
            start = end - timedelta(days=30)
            params = {
                "resultformat": "6",
                "queryname": "PRC_HUB_LMP",
                "version": "3",
                "market_run_id": "DAM",
                "startdatetime": start.strftime("%Y%m%dT07:00-0000"),
                "enddatetime": end.strftime("%Y%m%dT07:00-0000"),
            }
            resp = requests.get(
                _OASIS_URL, params=params,
                timeout=self.config.request_timeout,
            )
            resp.raise_for_status()

            z = zipfile.ZipFile(io.BytesIO(resp.content))
            csv_name = [n for n in z.namelist() if n.endswith(".csv")][0]
            raw = pd.read_csv(z.open(csv_name))

            # Filter for target hub and LMP type
            mask = (raw["HUB"].str.upper() == hub.upper()) & (raw["LMP_TYPE"] == "LMP")
            filtered = raw.loc[mask].copy()
            if filtered.empty:
                raise RuntimeError(f"No CAISO LMP data for hub {hub}")

            filtered["date"] = pd.to_datetime(filtered["OPR_DT"], utc=True)
            daily = (
                filtered.groupby("date")["MW"]
                .mean()
                .reset_index()
                .rename(columns={"MW": "value"})
            )
            return daily.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"CAISO_{name}"
    _Collector.__qualname__ = f"CAISO_{name}"
    return _Collector


CAISO_HUBS = [
    ("TH_NP15_GEN-APND", "caiso_lmp_np15", "CAISO Day-Ahead LMP: NP15"),
    ("TH_SP15_GEN-APND", "caiso_lmp_sp15", "CAISO Day-Ahead LMP: SP15"),
    ("TH_ZP26_GEN-APND", "caiso_lmp_zp26", "CAISO Day-Ahead LMP: ZP26"),
]


def get_caiso_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_caiso_collector(*t) for t in CAISO_HUBS}
