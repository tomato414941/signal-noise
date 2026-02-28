"""Tests for WHO GHO collectors."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from signal_noise.collector.who_gho import (
    WHO_GHO_SERIES,
    _make_who_gho_collector,
    get_who_gho_collectors,
)

WHO_GHO_RESPONSE = {
    "value": [
        {"TimeDim": 2019, "NumericValue": 73.3, "SpatialDim": "GLOBAL", "Dim1": "SEX_BTSX"},
        {"TimeDim": 2020, "NumericValue": 72.8, "SpatialDim": "GLOBAL", "Dim1": "SEX_BTSX"},
        {"TimeDim": 2021, "NumericValue": 71.4, "SpatialDim": "GLOBAL", "Dim1": "SEX_BTSX"},
    ],
}


class TestWHOGHOFactory:
    @patch("signal_noise.collector.who_gho.requests.get")
    def test_fetch(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = WHO_GHO_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_who_gho_collector(
            "WHOSIS_000001", "GLOBAL", "SEX_BTSX",
            "test_life_exp", "Test Life Exp", "health", "public_health",
        )
        df = cls().fetch()
        assert len(df) == 3
        assert df["value"].iloc[0] == 73.3
        assert df["date"].is_monotonic_increasing

    @patch("signal_noise.collector.who_gho.requests.get")
    def test_fetch_no_sex_filter(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = WHO_GHO_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_who_gho_collector(
            "GHED_CHEGDP_SHA2011", "USA", None,
            "test_health_exp", "Test Health Exp", "health", "public_health",
        )
        df = cls().fetch()
        assert len(df) == 3

        url = mock_get.call_args[0][0]
        assert "Dim1" not in url
        assert "SpatialDim eq 'USA'" in url

    @patch("signal_noise.collector.who_gho.requests.get")
    def test_empty_raises(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"value": []}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_who_gho_collector(
            "WHOSIS_000001", "GLOBAL", "SEX_BTSX",
            "test_empty", "Test Empty", "health", "public_health",
        )
        with pytest.raises(RuntimeError, match="No WHO data"):
            cls().fetch()

    def test_meta(self):
        cls = _make_who_gho_collector(
            "WHOSIS_000001", "GLOBAL", "SEX_BTSX",
            "test_meta", "Test Meta", "health", "public_health",
        )
        assert cls.meta.domain == "health"
        assert cls.meta.category == "public_health"
        assert cls.meta.update_frequency == "yearly"


class TestWHOGHORegistry:
    def test_series_count(self):
        assert len(WHO_GHO_SERIES) >= 20

    def test_no_duplicates(self):
        names = [t[3] for t in WHO_GHO_SERIES]
        assert len(names) == len(set(names))

    def test_total_count(self):
        collectors = get_who_gho_collectors()
        assert len(collectors) == len(WHO_GHO_SERIES)

    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS

        expected = ["who_life_expectancy", "who_hale", "who_health_exp_us"]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"
