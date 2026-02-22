from __future__ import annotations

from datetime import UTC, datetime, timedelta

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta

# (site_id, collector_name, display_name, description)
# USGS NWIS stations for major rivers / reservoirs
# Parameter 00060 = discharge (cubic feet per second)
USGS_WATER_SITES: list[tuple[str, str, str, str]] = [
    # ── Major US rivers (economic / drought indicators) ──
    ("09380000", "usgs_colorado_lees", "USGS: Colorado River at Lees Ferry", "drought"),
    ("07010000", "usgs_mississippi_stl", "USGS: Mississippi River at St. Louis", "flood"),
    ("01389500", "usgs_passaic_nj", "USGS: Passaic River NJ", "flood"),
    ("12340000", "usgs_clark_fork_mt", "USGS: Clark Fork at St. Regis MT", "hydro"),
    ("14211720", "usgs_willamette_or", "USGS: Willamette River Portland OR", "hydro"),
    ("01646500", "usgs_potomac_dc", "USGS: Potomac River at Washington DC", "flood"),
    ("08279500", "usgs_rio_grande_nm", "USGS: Rio Grande at Embudo NM", "drought"),
    ("11303500", "usgs_san_joaquin_ca", "USGS: San Joaquin River CA", "drought"),
]

_BASE_URL = (
    "https://waterservices.usgs.gov/nwis/dv/"
    "?format=json&sites={site}&startDT={start}&endDT={end}&parameterCd=00060"
)


def _make_water_collector(
    site_id: str, name: str, display_name: str, data_type_detail: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = SourceMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url=f"https://waterdata.usgs.gov/nwis/dv?site_no={site_id}",
            domain="earth",
            category="hydrology",
        )

        def fetch(self) -> pd.DataFrame:
            end = datetime.now(UTC) - timedelta(days=2)
            start = end - timedelta(days=730)
            url = _BASE_URL.format(
                site=site_id,
                start=start.strftime("%Y-%m-%d"),
                end=end.strftime("%Y-%m-%d"),
            )
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()
            data = resp.json()

            ts_list = (
                data.get("value", {})
                .get("timeSeries", [])
            )
            if not ts_list:
                raise RuntimeError(f"No time series for USGS site {site_id}")

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
                raise RuntimeError(f"No valid data for USGS site {site_id}")

            df = pd.DataFrame(rows)
            return df.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"USGS_{name}"
    _Collector.__qualname__ = f"USGS_{name}"
    return _Collector


def get_water_collectors() -> dict[str, type[BaseCollector]]:
    return {t[1]: _make_water_collector(*t) for t in USGS_WATER_SITES}
