from __future__ import annotations

from datetime import UTC, datetime, timedelta

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta

# (lat, lon, location, collector_name, display_name, parameter)
# Parameters: ALLSKY_SFC_SW_DWN = solar radiation (kW-hr/m²/day)
#             T2M = temperature at 2m (°C)
#             PRECTOTCORR = precipitation (mm/day)
NASA_POWER_POINTS: list[tuple[float, float, str, str, str, str]] = [
    # ── Solar radiation at crypto mining / energy regions ──
    (64.15, -21.94, "Reykjavik", "power_solar_reykjavik", "NASA Solar: Reykjavik", "ALLSKY_SFC_SW_DWN"),
    (47.38, 8.54, "Zurich", "power_solar_zurich", "NASA Solar: Zurich", "ALLSKY_SFC_SW_DWN"),
    (35.68, 139.65, "Tokyo", "power_solar_tokyo", "NASA Solar: Tokyo", "ALLSKY_SFC_SW_DWN"),
    (40.71, -74.01, "New York", "power_solar_nyc", "NASA Solar: New York", "ALLSKY_SFC_SW_DWN"),
    (39.90, 116.41, "Beijing", "power_solar_beijing", "NASA Solar: Beijing", "ALLSKY_SFC_SW_DWN"),
    (-23.55, -46.63, "São Paulo", "power_solar_sao_paulo", "NASA Solar: São Paulo", "ALLSKY_SFC_SW_DWN"),
    (28.61, 77.21, "New Delhi", "power_solar_delhi", "NASA Solar: New Delhi", "ALLSKY_SFC_SW_DWN"),
    (30.04, 31.24, "Cairo", "power_solar_cairo", "NASA Solar: Cairo", "ALLSKY_SFC_SW_DWN"),
    (25.20, 55.27, "Dubai", "power_solar_dubai", "NASA Solar: Dubai", "ALLSKY_SFC_SW_DWN"),
    (-33.87, 151.21, "Sydney", "power_solar_sydney", "NASA Solar: Sydney", "ALLSKY_SFC_SW_DWN"),
    # ── Precipitation at major economic hubs ──
    (40.71, -74.01, "New York", "power_precip_nyc", "NASA Precip: New York", "PRECTOTCORR"),
    (51.51, -0.13, "London", "power_precip_london", "NASA Precip: London", "PRECTOTCORR"),
    (35.68, 139.65, "Tokyo", "power_precip_tokyo", "NASA Precip: Tokyo", "PRECTOTCORR"),
    (39.90, 116.41, "Beijing", "power_precip_beijing", "NASA Precip: Beijing", "PRECTOTCORR"),
    (19.43, -99.13, "Mexico City", "power_precip_mexico", "NASA Precip: Mexico City", "PRECTOTCORR"),
]

_BASE_URL = (
    "https://power.larc.nasa.gov/api/temporal/daily/point"
    "?start={start}&end={end}"
    "&latitude={lat}&longitude={lon}"
    "&community=RE&parameters={param}&format=JSON"
)


def _make_power_collector(
    lat: float, lon: float, location: str,
    name: str, display_name: str, parameter: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = SourceMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://power.larc.nasa.gov/docs/services/api/",
            domain="earth",
            category="satellite",
        )

        def fetch(self) -> pd.DataFrame:
            end = datetime.now(UTC) - timedelta(days=7)
            start = end - timedelta(days=730)
            url = _BASE_URL.format(
                lat=lat, lon=lon,
                start=start.strftime("%Y%m%d"),
                end=end.strftime("%Y%m%d"),
                param=parameter,
            )
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            data = resp.json()

            params = data.get("properties", {}).get("parameter", {})
            series = params.get(parameter, {})

            if not series:
                raise RuntimeError(f"No NASA POWER data for {location}/{parameter}")

            rows = []
            for date_str, val in series.items():
                if val is None or val == -999.0:
                    continue
                dt = pd.to_datetime(date_str, format="%Y%m%d", utc=True)
                rows.append({"date": dt, "value": float(val)})

            if not rows:
                raise RuntimeError(f"No valid NASA POWER data for {location}/{parameter}")

            df = pd.DataFrame(rows)
            return df.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"Power_{name}"
    _Collector.__qualname__ = f"Power_{name}"
    return _Collector


def get_power_collectors() -> dict[str, type[BaseCollector]]:
    return {t[3]: _make_power_collector(*t) for t in NASA_POWER_POINTS}
