"""Open-Meteo temperature collectors for major cities.

Tracks daily mean temperature as a baseline environmental signal.
Extreme deviations can correlate with energy demand, agricultural
output, and economic disruption.

No API key required.  Docs: https://open-meteo.com/en/docs
"""
from __future__ import annotations

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

_BASE_URL = "https://api.open-meteo.com/v1/forecast"

# (city, lat, lon, collector_name, display_name)
_CITIES: list[tuple[str, float, float, str, str]] = [
    ("Tokyo", 35.6762, 139.6503, "meteo_tokyo", "Temperature: Tokyo"),
    ("New York", 40.7128, -74.0060, "meteo_newyork", "Temperature: New York"),
    ("London", 51.5074, -0.1278, "meteo_london", "Temperature: London"),
    ("Singapore", 1.3521, 103.8198, "meteo_singapore", "Temperature: Singapore"),
    ("São Paulo", -23.5505, -46.6333, "meteo_saopaulo", "Temperature: São Paulo"),
    ("Dubai", 25.2048, 55.2708, "meteo_dubai", "Temperature: Dubai"),
    ("Frankfurt", 50.1109, 8.6821, "meteo_frankfurt", "Temperature: Frankfurt"),
    ("Sydney", -33.8688, 151.2093, "meteo_sydney", "Temperature: Sydney"),
]


def _make_meteo_collector(
    city: str, lat: float, lon: float, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://open-meteo.com/en/docs",
            domain="environment",
            category="weather",
        )

        def fetch(self) -> pd.DataFrame:
            resp = requests.get(
                _BASE_URL,
                params={
                    "latitude": lat,
                    "longitude": lon,
                    "daily": "temperature_2m_mean",
                    "past_days": 90,
                    "forecast_days": 0,
                    "timezone": "auto",
                },
                timeout=self.config.request_timeout,
            )
            resp.raise_for_status()
            daily = resp.json().get("daily", {})
            dates = daily.get("time", [])
            temps = daily.get("temperature_2m_mean", [])

            rows = [
                {
                    "date": pd.to_datetime(d, utc=True),
                    "value": float(t),
                }
                for d, t in zip(dates, temps)
                if t is not None
            ]
            if not rows:
                raise RuntimeError(f"No temperature data for {city}")
            return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"Meteo_{name}"
    _Collector.__qualname__ = f"Meteo_{name}"
    return _Collector


def get_open_meteo_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_meteo_collector(city, lat, lon, name, display)
        for city, lat, lon, name, display in _CITIES
    }
