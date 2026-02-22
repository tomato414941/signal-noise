from __future__ import annotations

import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta


class DayOfWeekCollector(BaseCollector):
    meta = SourceMeta(
        name="day_of_week",
        display_name="Day of Week (0=Mon)",
        update_frequency="daily",
        api_docs_url="N/A",
        domain="computed",
        category="temporal",
    )

    def fetch(self) -> pd.DataFrame:
        dates = pd.date_range(end=pd.Timestamp.now(tz="UTC").normalize(), periods=365 * 3, freq="D")
        return pd.DataFrame({"date": dates, "value": dates.dayofweek.astype(float)})


class HourOfDayCollector(BaseCollector):
    meta = SourceMeta(
        name="hour_of_day",
        display_name="Hour of Day (UTC)",
        update_frequency="daily",
        api_docs_url="N/A",
        domain="computed",
        category="temporal",
    )

    def fetch(self) -> pd.DataFrame:
        times = pd.date_range(end=pd.Timestamp.now(tz="UTC"), periods=365 * 2 * 24, freq="h")
        return pd.DataFrame({"timestamp": times, "value": times.hour.astype(float)})
