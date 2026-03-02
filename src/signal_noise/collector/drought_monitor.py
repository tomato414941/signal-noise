from __future__ import annotations

import requests
import pandas as pd

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
            url = (
                "https://usdm.unl.edu/DmData/TimeSeries.aspx/"
                "GetDroughtSeverityStatisticsByAreaPercent?"
                "aession=GetStatisticsByAreaPercent"
                "&areatype=total&area=conus"
                "&statisticstype=1"
            )
            headers = {
                "Content-Type": "application/json",
                "User-Agent": "signal-noise/0.1",
            }
            resp = requests.get(url, headers=headers, timeout=self.config.request_timeout)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict) and "d" in data:
                import json
                data = json.loads(data["d"])

            rows = []
            for entry in data:
                try:
                    date_str = entry.get("MapDate") or entry.get("ReleaseDate")
                    if not date_str:
                        continue
                    date = pd.Timestamp(date_str, tz="UTC")
                    val = float(entry.get(level, 0))
                    rows.append({"date": date, "value": val})
                except (ValueError, TypeError, KeyError):
                    continue

            if not rows:
                raise RuntimeError(f"No drought data for {level}")
            df = pd.DataFrame(rows)
            cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=365 * 2)
            df = df[df["date"] >= cutoff]
            return df.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"Drought_{name}"
    _Collector.__qualname__ = f"Drought_{name}"
    return _Collector


def get_drought_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_drought_collector(*t) for t in DROUGHT_LEVELS}
