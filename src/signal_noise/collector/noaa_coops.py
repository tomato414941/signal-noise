from __future__ import annotations

from datetime import UTC, datetime, timedelta

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

# (station_id, product, collector_name, display_name, category)
NOAA_COOPS_SERIES: list[tuple[str, str, str, str, str]] = [
    # ── Water Level (tide gauge, meters MLLW) ──
    ("9414290", "water_level", "coops_wl_san_francisco", "NOAA CO-OPS: San Francisco Water Level", "marine"),
    ("8518750", "water_level", "coops_wl_battery_nyc", "NOAA CO-OPS: The Battery NYC Water Level", "marine"),
    ("8723214", "water_level", "coops_wl_virginia_key", "NOAA CO-OPS: Virginia Key FL Water Level", "marine"),
    ("9410660", "water_level", "coops_wl_los_angeles", "NOAA CO-OPS: Los Angeles Water Level", "marine"),
    ("8658120", "water_level", "coops_wl_wilmington_nc", "NOAA CO-OPS: Wilmington NC Water Level", "marine"),
    ("9447130", "water_level", "coops_wl_seattle", "NOAA CO-OPS: Seattle Water Level", "marine"),
    # ── Water Temperature (°C) ──
    ("8518750", "water_temperature", "coops_wt_battery_nyc", "NOAA CO-OPS: NYC Water Temp", "hydrology"),
    ("9414290", "water_temperature", "coops_wt_san_francisco", "NOAA CO-OPS: SF Water Temp", "hydrology"),
]

_BASE_URL = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"


def _make_coops_collector(
    station_id: str,
    product: str,
    name: str,
    display_name: str,
    category: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="hourly",
            api_docs_url="https://api.tidesandcurrents.noaa.gov/api/prod/",
            domain="earth",
            category=category,
        )

        def fetch(self) -> pd.DataFrame:
            end = datetime.now(UTC)
            start = end - timedelta(days=31)
            params: dict[str, str] = {
                "station": station_id,
                "begin_date": start.strftime("%Y%m%d"),
                "end_date": end.strftime("%Y%m%d"),
                "product": product,
                "units": "metric",
                "time_zone": "gmt",
                "application": "signal-noise",
                "format": "json",
                "interval": "h",
            }
            if product == "water_level":
                params["datum"] = "MLLW"

            resp = requests.get(
                _BASE_URL, params=params, timeout=self.config.request_timeout,
            )
            resp.raise_for_status()
            payload = resp.json()

            if "error" in payload:
                msg = payload["error"].get("message", "unknown")
                raise RuntimeError(f"NOAA CO-OPS error: {msg}")

            rows: list[dict] = []
            for entry in payload.get("data", []):
                val = entry.get("v")
                if val is None or val == "":
                    continue
                try:
                    ts = pd.to_datetime(entry["t"], utc=True)
                    rows.append({"timestamp": ts, "value": float(val)})
                except (ValueError, KeyError):
                    continue

            if not rows:
                raise RuntimeError(
                    f"No data for NOAA CO-OPS station {station_id}/{product}"
                )

            df = pd.DataFrame(rows)
            return df.sort_values("timestamp").reset_index(drop=True)

    _Collector.__name__ = f"COOPS_{name}"
    _Collector.__qualname__ = f"COOPS_{name}"
    return _Collector


def get_coops_collectors() -> dict[str, type[BaseCollector]]:
    return {t[2]: _make_coops_collector(*t) for t in NOAA_COOPS_SERIES}
