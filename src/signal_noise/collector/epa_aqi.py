from __future__ import annotations

import requests
import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class EPAAQICollector(BaseCollector):
    """US EPA AirNow current AQI observations."""

    meta = CollectorMeta(
        name="epa_aqi_us",
        display_name="US EPA AQI Average",
        update_frequency="daily",
        api_docs_url="https://aqs.epa.gov/aqsweb/documents/data_api.html",
        domain="earth",
        category="air_quality",
    )

    URL = "https://www.airnowapi.org/aq/observation/zipCode/current/?format=application/json&zipCode=10001&distance=100&API_KEY=DEMO_KEY"

    def fetch(self) -> pd.DataFrame:
        resp = requests.get(self.URL, timeout=self.config.request_timeout)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            raise RuntimeError("No EPA AQI data")
        now = pd.Timestamp.now(tz="UTC").normalize()
        aqi_values = [float(d["AQI"]) for d in data if "AQI" in d]
        avg_aqi = sum(aqi_values) / len(aqi_values) if aqi_values else 0
        return pd.DataFrame([{"date": now, "value": avg_aqi}])
