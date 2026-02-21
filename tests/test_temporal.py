from signal_noise.collector.temporal import DayOfWeekCollector, HourOfDayCollector


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
