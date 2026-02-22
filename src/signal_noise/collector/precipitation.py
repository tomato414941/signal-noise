from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class GlobalPrecipitationCollector(BaseCollector):
    """Global precipitation index via Open-Meteo archive API.

    Queries the Open-Meteo historical weather archive for daily
    precipitation at a representative equatorial grid point and
    averages over multiple locations for a global indicator.
    """

    meta = CollectorMeta(
        name="global_precip_index",
        display_name="Global Precipitation Index (mm/day)",
        update_frequency="daily",
        api_docs_url="https://open-meteo.com/en/docs/historical-weather-api",
        domain="earth",
        category="weather",
    )

    # Sample multiple representative locations for a rough global index
    _LOCATIONS = [
        (0, 0),        # Gulf of Guinea (equatorial)
        (0, 120),      # Indonesia (equatorial)
        (0, -60),      # Amazon (equatorial)
        (30, -90),     # Gulf Coast (subtropical)
        (-30, 150),    # Eastern Australia (subtropical)
        (50, 10),      # Central Europe (temperate)
    ]

    _BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

    def fetch(self) -> pd.DataFrame:
        end = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=5)
        start = end - pd.Timedelta(days=365)
        all_series: list[pd.Series] = []

        for lat, lon in self._LOCATIONS:
            url = (
                f"{self._BASE_URL}?latitude={lat}&longitude={lon}"
                f"&start_date={start.strftime('%Y-%m-%d')}"
                f"&end_date={end.strftime('%Y-%m-%d')}"
                f"&daily=precipitation_sum&timezone=UTC"
            )
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            daily = data.get("daily", {})
            times = daily.get("time", [])
            values = daily.get("precipitation_sum", [])
            if times and values:
                s = pd.Series(values, index=pd.to_datetime(times), name=f"{lat}_{lon}")
                all_series.append(s)

        if not all_series:
            raise RuntimeError("No precipitation data from Open-Meteo")

        # Average across locations for a global index
        combined = pd.concat(all_series, axis=1)
        avg = combined.mean(axis=1).dropna()
        if avg.empty:
            raise RuntimeError("No precipitation data after averaging")

        df = pd.DataFrame({
            "date": pd.to_datetime(avg.index, utc=True),
            "value": avg.values.astype(float),
        })
        return df.sort_values("date").reset_index(drop=True)
