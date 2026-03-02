"""Tests for BEA (Bureau of Economic Analysis) L2 collectors."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from signal_noise.collector.base import CATEGORIES, DOMAINS, CollectorMeta
from signal_noise.collector.bea_generic import (
    BEA_SERIES,
    _bea_cache,
    _make_bea_collector,
    _parse_period,
    get_bea_collectors,
)


class TestParsePeriod:
    def test_quarterly(self):
        ts = _parse_period("2024Q1")
        assert ts.year == 2024
        assert ts.month == 1

    def test_quarterly_q4(self):
        ts = _parse_period("2023Q4")
        assert ts.year == 2023
        assert ts.month == 10

    def test_annual(self):
        ts = _parse_period("2023")
        assert ts.year == 2023
        assert ts.month == 1

    def test_invalid(self):
        assert _parse_period("abc") is None
        assert _parse_period("2024Q5") is None
        assert _parse_period("") is None


class TestBEAGeneric:
    def setup_method(self):
        _bea_cache.clear()

    def test_series_count(self):
        assert len(BEA_SERIES) >= 12

    def test_no_duplicate_names(self):
        names = [t[3] for t in BEA_SERIES]
        assert len(names) == len(set(names))

    def test_factory_creates_collector(self):
        cls = _make_bea_collector(
            "NIPA", "T10106", "1", "test_bea", "Test", "quarterly", "economy", "economic",
        )
        assert cls.meta.name == "test_bea"
        assert cls.meta.domain == "economy"
        assert cls.meta.requires_key is True
        assert isinstance(cls.meta, CollectorMeta)

    def test_get_collectors_returns_dict(self):
        collectors = get_bea_collectors()
        assert isinstance(collectors, dict)
        assert len(collectors) == len(BEA_SERIES)
        assert "bea_gdp_real" in collectors
        assert "bea_gdi" in collectors

    @patch("signal_noise.collector.bea_generic._get_bea_key", return_value="fake-key")
    @patch("signal_noise.collector.bea_generic.requests.get")
    def test_fetch_quarterly(self, mock_get, _mock_key):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "BEAAPI": {
                "Results": {
                    "Data": [
                        {"LineNumber": "1", "TimePeriod": "2024Q1", "DataValue": "22,038.2"},
                        {"LineNumber": "1", "TimePeriod": "2024Q2", "DataValue": "22,156.3"},
                        {"LineNumber": "2", "TimePeriod": "2024Q1", "DataValue": "15,123.4"},
                    ]
                }
            }
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_bea_collector(
            "NIPA", "T10106", "1", "test_gdp", "Test", "quarterly", "economy", "economic",
        )
        df = cls().fetch()
        assert len(df) == 2  # only line 1
        assert df["value"].iloc[0] == 22038.2
        assert df["date"].iloc[1].month == 4  # Q2 -> April

    @patch("signal_noise.collector.bea_generic._get_bea_key", return_value="fake-key")
    @patch("signal_noise.collector.bea_generic.requests.get")
    def test_fetch_skips_na(self, mock_get, _mock_key):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "BEAAPI": {
                "Results": {
                    "Data": [
                        {"LineNumber": "1", "TimePeriod": "2024Q1", "DataValue": "100.0"},
                        {"LineNumber": "1", "TimePeriod": "2024Q2", "DataValue": "---"},
                        {"LineNumber": "1", "TimePeriod": "2024Q3", "DataValue": "105.0"},
                    ]
                }
            }
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_bea_collector(
            "NIPA", "T10101", "1", "test_growth", "Test", "quarterly", "economy", "economic",
        )
        df = cls().fetch()
        assert len(df) == 2  # "---" skipped

    @patch("signal_noise.collector.bea_generic._get_bea_key", return_value="fake-key")
    @patch("signal_noise.collector.bea_generic.requests.get")
    def test_fetch_api_error(self, mock_get, _mock_key):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "BEAAPI": {
                "Results": {
                    "Error": {
                        "ErrorDetail": {"Description": "Invalid parameter"}
                    }
                }
            }
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_bea_collector(
            "NIPA", "T10106", "1", "test_err", "Test", "quarterly", "economy", "economic",
        )
        with pytest.raises(RuntimeError, match="BEA API error"):
            cls().fetch()

    @patch("signal_noise.collector.bea_generic._get_bea_key", return_value="fake-key")
    @patch("signal_noise.collector.bea_generic.requests.get")
    def test_fetch_no_data_raises(self, mock_get, _mock_key):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "BEAAPI": {"Results": {"Data": []}}
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_bea_collector(
            "NIPA", "T10106", "1", "test_empty", "Test", "quarterly", "economy", "economic",
        )
        with pytest.raises(RuntimeError, match="No data for BEA"):
            cls().fetch()

    def test_all_series_valid_domain_category(self):
        for _, _, _, name, _, _, domain, category in BEA_SERIES:
            assert domain in DOMAINS, f"{name}: invalid domain {domain}"
            assert category in CATEGORIES, f"{name}: invalid category {category}"


class TestBEARegistration:
    def test_bea_registered(self):
        from signal_noise.collector import COLLECTORS

        for name in ["bea_gdp_real", "bea_gdi", "bea_corporate_profits"]:
            assert name in COLLECTORS, f"{name} not registered"
