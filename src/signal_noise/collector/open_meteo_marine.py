from __future__ import annotations

from datetime import UTC, datetime, timedelta

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta

# (lat, lon, location_name, collector_name, display_name)
# Key shipping lanes and ocean monitoring points
MARINE_POINTS: list[tuple[float, float, str, str, str]] = [
    # ── Major shipping chokepoints ──
    (30.0, 32.5, "Suez Canal", "marine_suez", "Marine: Suez Canal"),
    (9.0, -79.5, "Panama Canal", "marine_panama", "Marine: Panama Canal"),
    (1.3, 104.0, "Strait of Malacca", "marine_malacca", "Marine: Malacca Strait"),
    (36.0, -5.5, "Strait of Gibraltar", "marine_gibraltar", "Marine: Gibraltar"),
    (12.5, 43.3, "Bab el-Mandeb", "marine_bab", "Marine: Bab el-Mandeb"),
    (26.0, 56.0, "Strait of Hormuz", "marine_hormuz", "Marine: Hormuz"),
    # ── Major ocean routes ──
    (40.0, -30.0, "North Atlantic", "marine_n_atlantic", "Marine: North Atlantic"),
    (35.0, 140.0, "Pacific (Japan)", "marine_pacific_jp", "Marine: Pacific Japan"),
    (-34.0, 18.5, "Cape of Good Hope", "marine_cape", "Marine: Cape of Good Hope"),
    (55.0, 5.0, "North Sea", "marine_north_sea", "Marine: North Sea"),
]

_MARINE_URL = (
    "https://marine-api.open-meteo.com/v1/marine"
    "?latitude={lat}&longitude={lon}"
    "&start_date={start}&end_date={end}"
    "&daily=wave_height_max,wave_period_max"
    "&timezone=UTC"
)


def _make_marine_collector(
    lat: float, lon: float, location: str, name: str, display_name: str
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url="https://open-meteo.com/en/docs/marine-weather-api",
            domain="earth",
            category="marine",
        )

        def fetch(self) -> pd.DataFrame:
            end = datetime.now(UTC) - timedelta(days=2)
            start = end - timedelta(days=730)
            url = _MARINE_URL.format(
                lat=lat, lon=lon,
                start=start.strftime("%Y-%m-%d"),
                end=end.strftime("%Y-%m-%d"),
            )
            resp = requests.get(url, timeout=self.config.request_timeout)
            resp.raise_for_status()
            data = resp.json()

            daily = data.get("daily", {})
            times = daily.get("time", [])
            wave_h = daily.get("wave_height_max", [])
            wave_p = daily.get("wave_period_max", [])

            if not times:
                raise RuntimeError(f"No marine data for {location}")

            rows = []
            for i, t in enumerate(times):
                h = wave_h[i] if i < len(wave_h) else None
                if h is None:
                    continue
                rows.append({
                    "date": pd.to_datetime(t, utc=True),
                    "value": float(h),
                    "wave_period": float(wave_p[i]) if i < len(wave_p) and wave_p[i] is not None else 0.0,
                })

            if not rows:
                raise RuntimeError(f"No valid marine data for {location}")

            df = pd.DataFrame(rows)
            return df.sort_values("date").reset_index(drop=True)

    _Collector.__name__ = f"Marine_{name}"
    _Collector.__qualname__ = f"Marine_{name}"
    return _Collector


def get_marine_collectors() -> dict[str, type[BaseCollector]]:
    return {t[3]: _make_marine_collector(*t) for t in MARINE_POINTS}
