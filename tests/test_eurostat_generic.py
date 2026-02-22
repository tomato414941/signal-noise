"""Tests for Eurostat collectors."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from signal_noise.collector.eurostat_generic import (
    EUROSTAT_SERIES,
    _parse_period,
    _make_eurostat_collector,
    get_eurostat_collectors,
)


EUROSTAT_RESPONSE = {
    "dimension": {
        "time": {
            "category": {
                "index": {
                    "2024-01": 0,
                    "2024-02": 1,
                    "2024-03": 2,
                }
            }
        }
    },
    "value": {
        "0": 108.5,
        "1": 109.1,
        "2": 109.8,
    },
}

EUROSTAT_QUARTERLY = {
    "dimension": {
        "time": {
            "category": {
                "index": {
                    "2024Q1": 0,
                    "2024Q2": 1,
                }
            }
        }
    },
    "value": {
        "0": 3250000.0,
        "1": 3280000.0,
    },
}

EUROSTAT_EMPTY = {
    "dimension": {"time": {"category": {"index": {}}}},
    "value": {},
}


class TestParsePeriod:
    def test_monthly(self):
        assert _parse_period("2024-01") == "2024-01-01"

    def test_quarterly(self):
        assert _parse_period("2024Q1") == "2024-01-01"
        assert _parse_period("2024Q3") == "2024-07-01"

    def test_annual(self):
        assert _parse_period("2024") == "2024-01-01"

    def test_invalid(self):
        assert _parse_period("abc") is None


class TestEurostatFactory:
    @patch("signal_noise.collector.eurostat_generic.requests.get")
    def test_fetch_monthly(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = EUROSTAT_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_eurostat_collector(
            "prc_hicp_midx", "coicop=CP00&unit=I15&geo=EU27_2020",
            "test_hicp", "Test HICP", "monthly", "macro", "inflation",
        )
        df = cls().fetch()
        assert len(df) == 3
        assert df["value"].iloc[0] == 108.5
        assert df["date"].is_monotonic_increasing

    @patch("signal_noise.collector.eurostat_generic.requests.get")
    def test_fetch_quarterly(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = EUROSTAT_QUARTERLY
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_eurostat_collector(
            "nama_10_gdp", "na_item=B1GQ&unit=CLV10_MEUR&geo=EU27_2020&s_adj=SCA",
            "test_gdp", "Test GDP", "quarterly", "macro", "economic",
        )
        df = cls().fetch()
        assert len(df) == 2

    @patch("signal_noise.collector.eurostat_generic.requests.get")
    def test_empty_raises(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = EUROSTAT_EMPTY
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_eurostat_collector(
            "prc_hicp_midx", "coicop=CP00&unit=I15&geo=EU27_2020",
            "test_hicp", "Test HICP", "monthly", "macro", "inflation",
        )
        with pytest.raises(RuntimeError, match="No Eurostat data"):
            cls().fetch()


class TestEurostatMeta:
    def test_domain_category(self):
        cls = _make_eurostat_collector(
            "une_rt_m", "age=TOTAL&sex=T&unit=PC_ACT&s_adj=SA&geo=EU27_2020",
            "test_unemp", "Test Unemp", "monthly", "macro", "labor",
        )
        assert cls.meta.domain == "macro"
        assert cls.meta.category == "labor"
        assert cls.meta.requires_key is False


class TestEurostatRegistry:
    def test_series_count(self):
        assert len(EUROSTAT_SERIES) >= 35

    def test_no_duplicates(self):
        names = [t[2] for t in EUROSTAT_SERIES]
        assert len(names) == len(set(names))

    def test_total_count(self):
        collectors = get_eurostat_collectors()
        assert len(collectors) == len(EUROSTAT_SERIES)

    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS

        expected = [
            "eu_gdp_real", "eu_hicp_all", "eu_unemp_total",
            "eu_indprod", "eu_retail", "eu_business_conf",
        ]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"
