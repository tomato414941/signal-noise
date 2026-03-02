"""Open-Meteo current (realtime) weather — hourly snapshot signals.

Uses the free forecast API with `current` parameter to get the latest
observation for each city.  Cannot be backfilled.
API: https://open-meteo.com/en/docs
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector._cache import SharedAPICache

_realtime_cache = SharedAPICache(ttl=300)

_FORECAST_URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude={lat}&longitude={lon}"
    "&current=temperature_2m,relative_humidity_2m,wind_speed_10m"
    "&timezone=UTC"
)

# Reuse the same cities as open_meteo_weather (financial + energy hubs)
REALTIME_CITIES: list[tuple[float, float, str, str]] = [
    (40.7128, -74.006, "New York", "nyc"),
    (51.5074, -0.1278, "London", "london"),
    (35.6762, 139.6503, "Tokyo", "tokyo"),
    (22.3193, 114.1694, "Hong Kong", "hk"),
    (1.3521, 103.8198, "Singapore", "sg"),
    (48.8566, 2.3522, "Paris", "paris"),
    (50.1109, 8.6821, "Frankfurt", "frankfurt"),
    (47.3769, 8.5417, "Zurich", "zurich"),
    (55.7558, 37.6173, "Moscow", "moscow"),
    (25.2048, 55.2708, "Dubai", "dubai"),
    (61.5240, -105.3188, "Canada (avg)", "canada"),
    (-23.5505, -46.6333, "São Paulo", "sao_paulo"),
    (-33.8688, 151.2093, "Sydney", "sydney"),
    (39.9042, 116.4074, "Beijing", "beijing"),
    (31.2304, 121.4737, "Shanghai", "shanghai"),
    (64.1466, -21.9426, "Reykjavik", "reykjavik"),
    (30.0444, 31.2357, "Cairo", "cairo"),
    (19.4326, -99.1332, "Mexico City", "mexico"),
    (28.6139, 77.2090, "New Delhi", "delhi"),
    (37.5665, 126.9780, "Seoul", "seoul"),
]


def _make_realtime_collector(
    lat: float, lon: float, city: str, key: str,
) -> type[BaseCollector]:
    url = _FORECAST_URL.format(lat=lat, lon=lon)
    cache_key = f"rt:{lat}:{lon}"

    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=f"realtime_temp_{key}",
            display_name=f"Realtime Temp: {city}",
            update_frequency="hourly",
            api_docs_url="https://open-meteo.com/en/docs",
            domain="environment",
            category="weather",
        )

        def fetch(self) -> pd.DataFrame:
            def _fetch() -> dict:
                resp = requests.get(url, timeout=15)
                resp.raise_for_status()
                return resp.json()["current"]

            data = _realtime_cache.get_or_fetch(cache_key, _fetch)
            ts = pd.Timestamp.now(tz="UTC").floor("h")
            return pd.DataFrame({"timestamp": [ts], "value": [float(data["temperature_2m"])]})

    _Collector.__name__ = f"RealtimeTemp_{key}"
    _Collector.__qualname__ = f"RealtimeTemp_{key}"
    return _Collector


def get_realtime_collectors() -> dict[str, type[BaseCollector]]:
    return {
        f"realtime_temp_{key}": _make_realtime_collector(lat, lon, city, key)
        for lat, lon, city, key in REALTIME_CITIES
    }
