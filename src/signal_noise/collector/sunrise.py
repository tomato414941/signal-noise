"""Day length collectors via sunrise-sunset.org API.

Tracks photoperiod (day length in hours) at key latitudes.
No API key required.
"""
from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

_API_URL = "https://api.sunrise-sunset.org/json"

# (name, display_name, lat, lng)
_LOCATIONS = [
    ("day_length_equator", "Day Length: Equator (hours)", 0.0, 0.0),
    ("day_length_london", "Day Length: London (hours)", 51.5074, -0.1278),
    ("day_length_tokyo", "Day Length: Tokyo (hours)", 35.6762, 139.6503),
    ("day_length_nyc", "Day Length: New York (hours)", 40.7128, -74.0060),
    ("day_length_tromso", "Day Length: Tromsø (hours)", 69.6496, 18.9560),
    ("day_length_sydney", "Day Length: Sydney (hours)", -33.8688, 151.2093),
    ("day_length_capetown", "Day Length: Cape Town (hours)", -33.9249, 18.4241),
]


def _make_sunrise_collector(
    name: str, display_name: str, lat: float, lng: float,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://sunrise-sunset.org/api",
            domain="environment",
            category="celestial",
        )

        def fetch(self) -> pd.DataFrame:
            resp = requests.get(
                _API_URL,
                params={"lat": lat, "lng": lng, "date": "today", "formatted": "0"},
                timeout=self.config.request_timeout,
            )
            resp.raise_for_status()
            results = resp.json().get("results", {})
            day_length_sec = results.get("day_length", 0)
            hours = day_length_sec / 3600.0
            now = pd.Timestamp.now(tz="UTC").normalize()
            return pd.DataFrame([{"date": now, "value": round(hours, 2)}])

    _Collector.__name__ = f"Sunrise_{name}"
    _Collector.__qualname__ = f"Sunrise_{name}"
    return _Collector


def get_sunrise_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_sunrise_collector(name, display, lat, lng)
        for name, display, lat, lng in _LOCATIONS
    }
