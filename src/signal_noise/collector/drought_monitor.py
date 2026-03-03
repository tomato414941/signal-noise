from __future__ import annotations

import io
from datetime import datetime

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

# (severity_level, collector_name, display_name, description)
DROUGHT_LEVELS: list[tuple[str, str, str, str]] = [
    ("None", "drought_none_pct", "US Drought: None %", "Area with no drought"),
    ("D0", "drought_d0_pct", "US Drought: D0 (Abnormally Dry) %", "Abnormally dry area"),
    ("D1", "drought_d1_pct", "US Drought: D1 (Moderate) %", "Moderate drought area"),
    ("D2", "drought_d2_pct", "US Drought: D2 (Severe) %", "Severe drought area"),
    ("D3", "drought_d3_pct", "US Drought: D3 (Extreme) %", "Extreme drought area"),
    ("D4", "drought_d4_pct", "US Drought: D4 (Exceptional) %", "Exceptional drought area"),
]

# New REST API (old usdm.unl.edu domain is dead)
_API_BASE = "https://usdmdataservices.unl.edu/api/USStatistics/GetDroughtSeverityStatisticsByArea"


def _make_drought_collector(
    level: str, name: str, display_name: str, description: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="weekly",
            api_docs_url="https://droughtmonitor.unl.edu/DmData/DataDownload/WebServiceInfo.aspx",
            domain="environment",
            category="hydrology",
        )

        def fetch(self) -> pd.DataFrame:
            end = datetime.now()
            start = datetime(end.year - 2, end.month, end.day)
            params = {
                "aoi": "us",
                "startdate": start.strftime("%-m/%-d/%Y"),
                "enddate": end.strftime("%-m/%-d/%Y"),
                "statisticsType": "1",
            }
            resp = requests.get(
                _API_BASE, params=params,
                timeout=self.config.request_timeout,
            )
            resp.raise_for_status()

            df = pd.read_csv(io.StringIO(resp.text), thousands=",")
            if level not in df.columns:
                raise RuntimeError(f"Column {level} not in response")

            df["date"] = pd.to_datetime(df["MapDate"], format="%Y%m%d", utc=True)
            result = df[["date", level]].rename(columns={level: "value"})
            result = result.dropna(subset=["value"])
            return result.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"Drought_{name}"
    _Collector.__qualname__ = f"Drought_{name}"
    return _Collector


def get_drought_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_drought_collector(*t) for t in DROUGHT_LEVELS}
