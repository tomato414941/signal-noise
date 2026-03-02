from __future__ import annotations

from datetime import datetime, timedelta, UTC

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

# USGS NWIS groundwater monitoring wells
# Parameter 72019 = depth to water level below land surface (feet)
_GW_BASE_URL = (
    "https://waterservices.usgs.gov/nwis/dv/"
    "?format=json&sites={site}&startDT={start}&endDT={end}&parameterCd=72019"
)

# Representative long-record climate response wells
_GW_SITES = [
    ("403057074484401", "usgs_gw_nj", "USGS Groundwater: NJ Coastal Plain"),
    ("362532099125301", "usgs_gw_ok_ogallala", "USGS Groundwater: OK Ogallala Aquifer"),
    ("370812122235501", "usgs_gw_ca_sv", "USGS Groundwater: CA Santa Clara Valley"),
    ("290000095070001", "usgs_gw_tx_gulf", "USGS Groundwater: TX Gulf Coast"),
    ("433027088261601", "usgs_gw_wi", "USGS Groundwater: WI Cambrian-Ordovician"),
]


def _make_gw_collector(
    site_id: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url=f"https://waterdata.usgs.gov/nwis/gw?site_no={site_id}",
            domain="environment",
            category="hydrology",
        )

        def fetch(self) -> pd.DataFrame:
            end = datetime.now(UTC) - timedelta(days=2)
            start = end - timedelta(days=730)
            url = _GW_BASE_URL.format(
                site=site_id,
                start=start.strftime("%Y-%m-%d"),
                end=end.strftime("%Y-%m-%d"),
            )
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()
            data = resp.json()

            ts_list = data.get("value", {}).get("timeSeries", [])
            if not ts_list:
                raise RuntimeError(f"No groundwater time series for site {site_id}")

            values = ts_list[0].get("values", [{}])[0].get("value", [])
            rows = []
            for v in values:
                val = v.get("value")
                if val is None or val == "" or val == "-999999":
                    continue
                try:
                    dt = pd.to_datetime(v["dateTime"], utc=True)
                    rows.append({"date": dt, "value": float(val)})
                except (ValueError, KeyError):
                    continue

            if not rows:
                raise RuntimeError(f"No valid groundwater data for site {site_id}")

            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"USGS_GW_{name}"
    _Collector.__qualname__ = f"USGS_GW_{name}"
    return _Collector


def get_groundwater_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_gw_collector(*t) for t in _GW_SITES}
