from signal_noise.collector.temporal import (
    DayOfWeekCollector,
    DaysToHalloweenCollector,
    DaysToNewYearCollector,
    Friday13Collector,
    HourOfDayCollector,
)


class TestDayOfWeek:
    def test_fetch_returns_correct_columns(self):
        c = DayOfWeekCollector()
        df = c.fetch()
        assert "date" in df.columns
        assert "value" in df.columns

    def test_values_in_range(self):
        c = DayOfWeekCollector()
        df = c.fetch()
        assert df["value"].min() >= 0
        assert df["value"].max() <= 6

    def test_row_count(self):
        c = DayOfWeekCollector()
        df = c.fetch()
        assert len(df) == 365 * 3


class TestHourOfDay:
    def test_fetch_returns_correct_columns(self):
        c = HourOfDayCollector()
        df = c.fetch()
        assert "timestamp" in df.columns
        assert "value" in df.columns

    def test_values_in_range(self):
        c = HourOfDayCollector()
        df = c.fetch()
        assert df["value"].min() >= 0
        assert df["value"].max() <= 23


class TestFriday13:
    def test_binary_values(self):
        c = Friday13Collector()
        df = c.fetch()
        assert set(df["value"].unique()).issubset({0.0, 1.0})

    def test_has_some_zero(self):
        c = Friday13Collector()
        df = c.fetch()
        assert (df["value"] == 0.0).any()


class TestDaysToHalloween:
    def test_value_range(self):
        c = DaysToHalloweenCollector()
        df = c.fetch()
        assert df["value"].min() >= 0
        assert df["value"].max() <= 366


class TestDaysToNewYear:
    def test_value_range(self):
        c = DaysToNewYearCollector()
        df = c.fetch()
        assert df["value"].min() >= 0
        assert df["value"].max() <= 366


class TestTemporalRegistration:
    def test_new_collectors_registered(self):
        from signal_noise.collector import COLLECTORS
        for name in ["is_friday_13", "days_to_halloween", "days_to_new_year"]:
            assert name in COLLECTORS, f"{name} not registered"
