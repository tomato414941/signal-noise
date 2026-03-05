from __future__ import annotations

import pandas as pd

from signal_noise.collector.base import BaseCollector, CollectorMeta


class DayOfWeekCollector(BaseCollector):
    meta = CollectorMeta(
        name="day_of_week",
        display_name="Day of Week (0=Mon)",
        update_frequency="daily",
        api_docs_url="N/A",
        domain="sentiment",
        category="temporal",
        collection_level="L4",
    )

    def fetch(self) -> pd.DataFrame:
        dates = pd.date_range(end=pd.Timestamp.now(tz="UTC").normalize(), periods=365 * 3, freq="D")
        return pd.DataFrame({"date": dates, "value": dates.dayofweek.astype(float)})


class HourOfDayCollector(BaseCollector):
    meta = CollectorMeta(
        name="hour_of_day",
        display_name="Hour of Day (UTC)",
        update_frequency="daily",
        api_docs_url="N/A",
        domain="sentiment",
        category="temporal",
        collection_level="L4",
    )

    def fetch(self) -> pd.DataFrame:
        end = pd.Timestamp.now(tz="UTC").floor("h")
        times = pd.date_range(end=end, periods=365 * 2 * 24, freq="h")
        return pd.DataFrame({"timestamp": times, "value": times.hour.astype(float)})


class Friday13Collector(BaseCollector):
    meta = CollectorMeta(
        name="is_friday_13",
        display_name="Is Friday the 13th",
        update_frequency="daily",
        api_docs_url="N/A",
        domain="sentiment",
        category="temporal",
        collection_level="L4",
    )

    def fetch(self) -> pd.DataFrame:
        dates = pd.date_range(end=pd.Timestamp.now(tz="UTC").normalize(), periods=365 * 3, freq="D")
        values = ((dates.day == 13) & (dates.dayofweek == 4)).astype(float)
        return pd.DataFrame({"date": dates, "value": values})


class DaysToHalloweenCollector(BaseCollector):
    meta = CollectorMeta(
        name="days_to_halloween",
        display_name="Days Until Halloween",
        update_frequency="daily",
        api_docs_url="N/A",
        domain="sentiment",
        category="temporal",
        collection_level="L4",
    )

    def fetch(self) -> pd.DataFrame:
        dates = pd.date_range(end=pd.Timestamp.now(tz="UTC").normalize(), periods=365 * 3, freq="D")
        values: list[float] = []
        for d in dates:
            target = pd.Timestamp(year=d.year, month=10, day=31, tz="UTC")
            if d > target:
                target = pd.Timestamp(year=d.year + 1, month=10, day=31, tz="UTC")
            values.append(float((target - d).days))
        return pd.DataFrame({"date": dates, "value": values})


class DaysToNewYearCollector(BaseCollector):
    meta = CollectorMeta(
        name="days_to_new_year",
        display_name="Days Until New Year",
        update_frequency="daily",
        api_docs_url="N/A",
        domain="sentiment",
        category="temporal",
        collection_level="L4",
    )

    def fetch(self) -> pd.DataFrame:
        dates = pd.date_range(end=pd.Timestamp.now(tz="UTC").normalize(), periods=365 * 3, freq="D")
        values: list[float] = []
        for d in dates:
            target = pd.Timestamp(year=d.year + 1, month=1, day=1, tz="UTC")
            values.append(float((target - d).days))
        return pd.DataFrame({"date": dates, "value": values})
