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
    (34.0522, -118.2437, "Los Angeles", "la"),
    (41.8781, -87.6298, "Chicago", "chicago"),
    (29.7604, -95.3698, "Houston", "houston"),
    (25.7617, -80.1918, "Miami", "miami"),
    (43.6532, -79.3832, "Toronto", "toronto"),
    (49.2827, -123.1207, "Vancouver", "vancouver"),
    (51.5074, -0.1278, "London", "london"),
    (53.3498, -6.2603, "Dublin", "dublin"),
    (52.3676, 4.9041, "Amsterdam", "amsterdam"),
    (40.4168, -3.7038, "Madrid", "madrid"),
    (41.9028, 12.4964, "Rome", "rome"),
    (48.2082, 16.3738, "Vienna", "vienna"),
    (52.2297, 21.0122, "Warsaw", "warsaw"),
    (60.1699, 24.9384, "Helsinki", "helsinki"),
    (38.7223, -9.1393, "Lisbon", "lisbon"),
    (37.9838, 23.7275, "Athens", "athens"),
    (41.0082, 28.9784, "Istanbul", "istanbul"),
    (35.6762, 139.6503, "Tokyo", "tokyo"),
    (22.3193, 114.1694, "Hong Kong", "hk"),
    (1.3521, 103.8198, "Singapore", "sg"),
    (-6.2088, 106.8456, "Jakarta", "jakarta"),
    (13.7563, 100.5018, "Bangkok", "bangkok"),
    (3.1390, 101.6869, "Kuala Lumpur", "kuala_lumpur"),
    (14.5995, 120.9842, "Manila", "manila"),
    (48.8566, 2.3522, "Paris", "paris"),
    (50.1109, 8.6821, "Frankfurt", "frankfurt"),
    (47.3769, 8.5417, "Zurich", "zurich"),
    (55.7558, 37.6173, "Moscow", "moscow"),
    (32.0853, 34.7818, "Tel Aviv", "tel_aviv"),
    (24.7136, 46.6753, "Riyadh", "riyadh"),
    (25.2854, 51.5310, "Doha", "doha"),
    (25.2048, 55.2708, "Dubai", "dubai"),
    (61.5240, -105.3188, "Canada (avg)", "canada"),
    (-23.5505, -46.6333, "São Paulo", "sao_paulo"),
    (4.7110, -74.0721, "Bogota", "bogota"),
    (-12.0464, -77.0428, "Lima", "lima"),
    (-33.4489, -70.6693, "Santiago", "santiago"),
    (-26.2041, 28.0473, "Johannesburg", "johannesburg"),
    (6.5244, 3.3792, "Lagos", "lagos"),
    (-1.2921, 36.8219, "Nairobi", "nairobi"),
    (-33.9249, 18.4241, "Cape Town", "cape_town"),
    (-33.8688, 151.2093, "Sydney", "sydney"),
    (-36.8485, 174.7633, "Auckland", "auckland"),
    (39.9042, 116.4074, "Beijing", "beijing"),
    (31.2304, 121.4737, "Shanghai", "shanghai"),
    (64.1466, -21.9426, "Reykjavik", "reykjavik"),
    (30.0444, 31.2357, "Cairo", "cairo"),
    (19.4326, -99.1332, "Mexico City", "mexico"),
    (28.6139, 77.2090, "New Delhi", "delhi"),
    (19.0760, 72.8777, "Mumbai", "mumbai"),
    (-34.6037, -58.3816, "Buenos Aires", "buenos_aires"),
    (59.3293, 18.0686, "Stockholm", "stockholm"),
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
