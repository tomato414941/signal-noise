from __future__ import annotations

from datetime import UTC, datetime, timedelta

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector._cache import SharedAPICache

_weather_cache = SharedAPICache(ttl=300)

# (lat, lon, city_name, collector_name, display_name)
WEATHER_CITIES: list[tuple[float, float, str, str, str]] = [
    # ── Major financial centers ──
    (40.7128, -74.006, "New York", "meteo_nyc", "Weather: New York"),
    (51.5074, -0.1278, "London", "meteo_london", "Weather: London"),
    (35.6762, 139.6503, "Tokyo", "meteo_tokyo", "Weather: Tokyo"),
    (22.3193, 114.1694, "Hong Kong", "meteo_hk", "Weather: Hong Kong"),
    (1.3521, 103.8198, "Singapore", "meteo_sg", "Weather: Singapore"),
    (48.8566, 2.3522, "Paris", "meteo_paris", "Weather: Paris"),
    (50.1109, 8.6821, "Frankfurt", "meteo_frankfurt", "Weather: Frankfurt"),
    (47.3769, 8.5417, "Zurich", "meteo_zurich", "Weather: Zurich"),
    (55.7558, 37.6173, "Moscow", "meteo_moscow", "Weather: Moscow"),
    (25.2048, 55.2708, "Dubai", "meteo_dubai", "Weather: Dubai"),
    # ── Mining / energy hubs ──
    (61.5240, -105.3188, "Canada (avg)", "meteo_canada", "Weather: Canada"),
    (-23.5505, -46.6333, "São Paulo", "meteo_sao_paulo", "Weather: São Paulo"),
    (-33.8688, 151.2093, "Sydney", "meteo_sydney", "Weather: Sydney"),
    (39.9042, 116.4074, "Beijing", "meteo_beijing", "Weather: Beijing"),
    (31.2304, 121.4737, "Shanghai", "meteo_shanghai", "Weather: Shanghai"),
    # ── Crypto mining / energy regions ──
    (64.1466, -21.9426, "Reykjavik", "meteo_reykjavik", "Weather: Reykjavik"),
    (30.0444, 31.2357, "Cairo", "meteo_cairo", "Weather: Cairo"),
    (19.4326, -99.1332, "Mexico City", "meteo_mexico", "Weather: Mexico City"),
    (28.6139, 77.2090, "New Delhi", "meteo_delhi", "Weather: New Delhi"),
    (37.5665, 126.9780, "Seoul", "meteo_seoul", "Weather: Seoul"),
]

_ARCHIVE_URL = (
    "https://archive-api.open-meteo.com/v1/archive"
    "?latitude={lat}&longitude={lon}"
    "&start_date={start}&end_date={end}"
    "&daily=temperature_2m_mean,precipitation_sum,windspeed_10m_max"
    "&timezone=UTC"
)


def _make_weather_collector(
    lat: float, lon: float, city: str, name: str, display_name: str
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://open-meteo.com/en/docs/historical-weather-api",
            domain="earth",
            category="weather",
        )

        def fetch(self) -> pd.DataFrame:
            end = datetime.now(UTC) - timedelta(days=5)
            start = end - timedelta(days=730)
            url = _ARCHIVE_URL.format(
                lat=lat, lon=lon,
                start=start.strftime("%Y-%m-%d"),
                end=end.strftime("%Y-%m-%d"),
            )
            timeout = self.config.request_timeout

            def _fetch() -> dict:
                resp = requests.get(url, timeout=timeout)
                resp.raise_for_status()
                return resp.json()

            data = _weather_cache.get_or_fetch(f"{lat}:{lon}", _fetch)

            daily = data.get("daily", {})
            times = daily.get("time", [])
            temps = daily.get("temperature_2m_mean", [])
            precip = daily.get("precipitation_sum", [])
            wind = daily.get("windspeed_10m_max", [])

            if not times:
                raise RuntimeError(f"No weather data for {city}")

            rows = []
            for i, t in enumerate(times):
                temp_val = temps[i] if i < len(temps) else None
                if temp_val is None:
                    continue
                rows.append({
                    "date": pd.to_datetime(t, utc=True),
                    "value": float(temp_val),
                    "precipitation": float(precip[i]) if i < len(precip) and precip[i] is not None else 0.0,
                    "wind_max": float(wind[i]) if i < len(wind) and wind[i] is not None else 0.0,
                })

            if not rows:
                raise RuntimeError(f"No valid weather data for {city}")

            df = pd.DataFrame(rows)
            return df.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"Meteo_{name}"
    _Collector.__qualname__ = f"Meteo_{name}"
    return _Collector


def get_weather_collectors() -> dict[str, type[BaseCollector]]:
    return {t[3]: _make_weather_collector(*t) for t in WEATHER_CITIES}
