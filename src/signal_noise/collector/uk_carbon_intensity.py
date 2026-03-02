from __future__ import annotations

from datetime import UTC, datetime, timedelta

import requests
import pandas as pd

from signal_noise.collector._cache import SharedAPICache
from signal_noise.collector.base import BaseCollector, CollectorMeta

_carbon_cache = SharedAPICache(ttl=1800)


def _fetch_carbon_data(timeout: int = 30) -> list[dict]:
    def _fetch() -> list[dict]:
        end = datetime.now(UTC)
        start = end - timedelta(days=14)
        url = (
            f"https://api.carbonintensity.org.uk/intensity/"
            f"{start.strftime('%Y-%m-%dT%H:%MZ')}/{end.strftime('%Y-%m-%dT%H:%MZ')}"
        )
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.json().get("data", [])

    return _carbon_cache.get_or_fetch("intensity", _fetch)


class _UKCarbonBase(BaseCollector):
    _field: str = ""

    def fetch(self) -> pd.DataFrame:
        data = _fetch_carbon_data(timeout=self.config.request_timeout)

        rows: list[dict] = []
        for entry in data:
            intensity = entry.get("intensity", {})
            val = intensity.get(self._field)
            ts = entry.get("from")
            if val is None or ts is None:
                continue
            try:
                rows.append({
                    "timestamp": pd.to_datetime(ts, utc=True),
                    "value": float(val),
                })
            except (ValueError, TypeError):
                continue

        if not rows:
            raise RuntimeError(f"No UK carbon intensity data for field={self._field}")

        df = pd.DataFrame(rows)
        df["date"] = df["timestamp"].dt.normalize()
        daily = df.groupby("date")["value"].mean().reset_index()
        return daily.sort_values("date").reset_index(drop=True)


class UKCarbonActualCollector(_UKCarbonBase):
    _field = "actual"
    meta = CollectorMeta(
        name="uk_carbon_actual",
        display_name="UK Carbon Intensity (Actual, gCO2/kWh)",
        update_frequency="daily",
        api_docs_url="https://carbon-intensity.github.io/api-definitions/",
        domain="environment",
        category="climate",
    )


class UKCarbonForecastCollector(_UKCarbonBase):
    _field = "forecast"
    meta = CollectorMeta(
        name="uk_carbon_forecast",
        display_name="UK Carbon Intensity (Forecast, gCO2/kWh)",
        update_frequency="daily",
        api_docs_url="https://carbon-intensity.github.io/api-definitions/",
        domain="environment",
        category="climate",
    )
