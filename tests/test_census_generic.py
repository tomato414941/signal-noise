"""Tests for Census Bureau collectors."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from signal_noise.collector.census_generic import (
    CENSUS_SERIES,
    _make_census_collector,
    get_census_collectors,
)


CENSUS_RESPONSE = [
    ["cell_value", "time"],
    ["1500", "2024-01"],
    ["1520", "2024-02"],
    ["1540", "2024-03"],
]


class TestCensusFactory:
    @patch("signal_noise.collector.census_generic.requests.get")
    def test_fetch(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = CENSUS_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_census_collector(
            "/eits/resconst", "category_code=TOTAL&data_type_code=PERMITS",
            "time", "cell_value",
            "test_permits", "Test Permits",
            "monthly", "macro", "economic",
        )
        df = cls().fetch()
        assert len(df) == 3
        assert df["value"].iloc[0] == 1500.0
        assert df["date"].is_monotonic_increasing

    @patch("signal_noise.collector.census_generic.requests.get")
    def test_empty_raises(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = [["cell_value", "time"]]
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_census_collector(
            "/eits/resconst", "category_code=TOTAL&data_type_code=PERMITS",
            "time", "cell_value",
            "test_permits", "Test Permits",
            "monthly", "macro", "economic",
        )
        with pytest.raises(RuntimeError, match="No Census data"):
            cls().fetch()


class TestCensusMeta:
    def test_domain_category(self):
        cls = _make_census_collector(
            "/eits/ressales", "category_code=AVGPRICE&data_type_code=TOTAL",
            "time", "cell_value",
            "test_price", "Test Price",
            "monthly", "real_estate", "real_estate",
        )
        assert cls.meta.domain == "real_estate"
        assert cls.meta.category == "real_estate"


class TestCensusRegistry:
    def test_series_count(self):
        assert len(CENSUS_SERIES) >= 20

    def test_no_duplicates(self):
        names = [t[4] for t in CENSUS_SERIES]
        assert len(names) == len(set(names))

    def test_total_count(self):
        collectors = get_census_collectors()
        assert len(collectors) == len(CENSUS_SERIES)

    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS

        expected = [
            "census_building_permits", "census_housing_starts",
            "census_retail_total", "census_durable_orders",
            "census_construction_spend",
        ]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"
