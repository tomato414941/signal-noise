from __future__ import annotations

import os

import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta


class NYWeatherCollector(BaseCollector):
    meta = SourceMeta(
        name="ny_weather",
        display_name="NYC Weather",
        update_frequency="hourly",
        data_type="weather",
        api_docs_url="https://openweathermap.org/api",
        requires_key=True,
    )

    def fetch(self) -> pd.DataFrame:
        api_key = os.environ.get("OPENWEATHERMAP_API_KEY")
        if not api_key:
            raise RuntimeError(
                "OPENWEATHERMAP_API_KEY not set. Get a free key at https://openweathermap.org/api"
            )
        raise NotImplementedError("NYWeatherCollector requires paid API for historical data")
