"""Tests for FRED generic collectors."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from signal_noise.collector.fred_generic import (
    FRED_SERIES,
    get_fred_collectors,
    _make_fred_collector,
)


FRED_API_RESPONSE = {
    "observations": [
        {"date": "2024-01-01", "value": "3.5"},
        {"date": "2024-01-08", "value": "3.4"},
        {"date": "2024-01-15", "value": "."},
        {"date": "2024-01-22", "value": "3.6"},
    ]
}


class TestFredGenericFactory:
    def test_series_count(self):
        assert len(FRED_SERIES) >= 20

    def test_no_duplicate_names(self):
        names = [t[1] for t in FRED_SERIES]
        assert len(names) == len(set(names))

    def test_no_duplicate_series_ids(self):
        ids = [t[0] for t in FRED_SERIES]
        assert len(ids) == len(set(ids))

    def test_factory_creates_collector(self):
        cls = _make_fred_collector("ICSA", "test_icsa", "Test ICSA", "weekly", "economy", "labor")
        assert cls.meta.name == "test_icsa"
        assert cls.meta.domain == "economy"
        assert cls.meta.category == "labor"
        assert cls.meta.requires_key is True

    def test_get_fred_collectors_returns_dict(self):
        collectors = get_fred_collectors()
        assert isinstance(collectors, dict)
        assert len(collectors) == len(FRED_SERIES)

    def test_all_series_have_valid_domain_category(self):
        from signal_noise.collector.base import DOMAINS, CATEGORIES
        for series_id, name, display, freq, dom, cat in FRED_SERIES:
            assert dom in DOMAINS, f"{name} has invalid domain: {dom}"
            assert cat in CATEGORIES, f"{name} has invalid category: {cat}"

    def test_all_series_have_valid_frequency(self):
        valid_freqs = {"daily", "weekly", "monthly", "quarterly"}
        for series_id, name, display, freq, dom, cat in FRED_SERIES:
            assert freq in valid_freqs, f"{name} has invalid frequency: {freq}"

    def test_all_categories_present(self):
        categories = {t[5] for t in FRED_SERIES}
        assert "labor" in categories
        assert "inflation" in categories
        assert "rates" in categories
        assert "economic" in categories


class TestFredGenericFetch:
    @patch("signal_noise.collector.fred_generic._get_fred_key", return_value="test_key")
    @patch("signal_noise.collector.fred_generic.requests.get")
    def test_fetch_parses_observations(self, mock_get, mock_key):
        mock_resp = MagicMock()
        mock_resp.json.return_value = FRED_API_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_fred_collector("ICSA", "test_icsa", "Test", "weekly", "economy", "labor")
        df = cls().fetch()
        assert "date" in df.columns
        assert "value" in df.columns
        # "." value should be skipped
        assert len(df) == 3
        assert df["value"].iloc[0] == 3.5

    @patch("signal_noise.collector.fred_generic._get_fred_key", return_value="test_key")
    @patch("signal_noise.collector.fred_generic.requests.get")
    def test_sorted_by_date(self, mock_get, mock_key):
        mock_resp = MagicMock()
        mock_resp.json.return_value = FRED_API_RESPONSE
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_fred_collector("UNRATE", "test_unrate", "Test", "monthly", "economy", "labor")
        df = cls().fetch()
        assert df["date"].is_monotonic_increasing

    @patch("signal_noise.collector.fred_generic._get_fred_key", return_value="test_key")
    @patch("signal_noise.collector.fred_generic.requests.get")
    def test_empty_response_raises(self, mock_get, mock_key):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"observations": []}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_fred_collector("GDP", "test_gdp", "Test", "quarterly", "economy", "economic")
        with pytest.raises(RuntimeError, match="No data"):
            cls().fetch()

    @patch("signal_noise.collector.fred_generic._get_fred_key", return_value="test_key")
    @patch("signal_noise.collector.fred_generic.requests.get")
    def test_all_missing_values_raises(self, mock_get, mock_key):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "observations": [
                {"date": "2024-01-01", "value": "."},
                {"date": "2024-01-02", "value": "."},
            ]
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_fred_collector("TEST", "test_miss", "Test", "daily", "economy", "labor")
        with pytest.raises(RuntimeError, match="No data"):
            cls().fetch()


class TestFredRegistration:
    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS
        expected = [
            "fred_jobless_claims", "fred_unemployment", "fred_cpi",
            "fred_fed_funds", "fred_m2", "fred_gdp",
            "fred_consumer_sentiment", "fred_financial_stress",
        ]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"


class TestWikiLaborRegistration:
    def test_labor_pages_registered(self):
        from signal_noise.collector import COLLECTORS
        expected = [
            "wiki_strike", "wiki_trade_union", "wiki_minimum_wage",
            "wiki_lawsuit", "wiki_fin_regulation",
            "wiki_fed", "wiki_interest_rate", "wiki_central_bank",
        ]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"
