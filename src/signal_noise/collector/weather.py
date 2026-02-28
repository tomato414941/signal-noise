from __future__ import annotations

import os

import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class NYWeatherCollector(BaseCollector):
    _skip_registration = True

    meta = CollectorMeta(
        name="ny_weather",
        display_name="NYC Weather",
        update_frequency="hourly",
        api_docs_url="https://openweathermap.org/api",
        requires_key=True,
        domain="earth",
        category="weather",
    )

    def fetch(self) -> pd.DataFrame:
        api_key = os.environ.get("OPENWEATHERMAP_API_KEY")
        if not api_key:
            raise RuntimeError(
                "OPENWEATHERMAP_API_KEY not set. Get a free key at https://openweathermap.org/api"
            )
        raise NotImplementedError("NYWeatherCollector requires paid API for historical data")
