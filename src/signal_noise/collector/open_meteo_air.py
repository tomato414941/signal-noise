from __future__ import annotations

from datetime import UTC, datetime, timedelta

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector._cache import SharedAPICache

_air_cache = SharedAPICache(ttl=300)

# (lat, lon, city_name, collector_name, display_name)
# Cities with notable air quality issues or economic significance
AIR_QUALITY_CITIES: list[tuple[float, float, str, str, str]] = [
    (39.9042, 116.4074, "Beijing", "air_beijing", "Air Quality: Beijing"),
    (28.6139, 77.2090, "New Delhi", "air_delhi", "Air Quality: New Delhi"),
    (31.2304, 121.4737, "Shanghai", "air_shanghai", "Air Quality: Shanghai"),
    (35.6762, 139.6503, "Tokyo", "air_tokyo", "Air Quality: Tokyo"),
    (37.5665, 126.9780, "Seoul", "air_seoul", "Air Quality: Seoul"),
    (40.7128, -74.006, "New York", "air_nyc", "Air Quality: New York"),
    (51.5074, -0.1278, "London", "air_london", "Air Quality: London"),
    (-23.5505, -46.6333, "São Paulo", "air_sao_paulo", "Air Quality: São Paulo"),
    (30.0444, 31.2357, "Cairo", "air_cairo", "Air Quality: Cairo"),
    (14.5995, 120.9842, "Manila", "air_manila", "Air Quality: Manila"),
]

_AQ_URL = (
    "https://air-quality-api.open-meteo.com/v1/air-quality"
    "?latitude={lat}&longitude={lon}"
    "&hourly=pm2_5,pm10"
    "&timezone=UTC"
    "&start_date={start}&end_date={end}"
)


def _make_air_collector(
    lat: float, lon: float, city: str, name: str, display_name: str
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://open-meteo.com/en/docs/air-quality-api",
            domain="earth",
            category="air_quality",
        )

        def fetch(self) -> pd.DataFrame:
            end = datetime.now(UTC) - timedelta(days=2)
            start = end - timedelta(days=90)
            url = _AQ_URL.format(
                lat=lat, lon=lon,
                start=start.strftime("%Y-%m-%d"),
                end=end.strftime("%Y-%m-%d"),
            )
            timeout = self.config.request_timeout

            def _fetch() -> dict:
                resp = requests.get(url, timeout=timeout)
                resp.raise_for_status()
                return resp.json()

            data = _air_cache.get_or_fetch(f"{lat}:{lon}", _fetch)

            hourly = data.get("hourly", {})
            times = hourly.get("time", [])
            pm25 = hourly.get("pm2_5", [])

            if not times:
                raise RuntimeError(f"No air quality data for {city}")

            # Aggregate hourly -> daily mean
            daily: dict[str, list[float]] = {}
            for i, t in enumerate(times):
                val = pm25[i] if i < len(pm25) else None
                if val is None:
                    continue
                day = t[:10]
                daily.setdefault(day, []).append(float(val))

            if not daily:
                raise RuntimeError(f"No valid air quality data for {city}")

            rows = [
                {
                    "date": pd.to_datetime(day, utc=True),
                    "value": sum(vals) / len(vals),
                }
                for day, vals in daily.items()
            ]

            df = pd.DataFrame(rows)
            return df.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"Air_{name}"
    _Collector.__qualname__ = f"Air_{name}"
    return _Collector


def get_air_collectors() -> dict[str, type[BaseCollector]]:
    return {t[3]: _make_air_collector(*t) for t in AIR_QUALITY_CITIES}
