"""Tests for Finnhub L2 collectors."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from signal_noise.collector.base import CATEGORIES, DOMAINS, CollectorMeta
from signal_noise.collector.finnhub_generic import (
    FINNHUB_SERIES,
    _finnhub_cache,
    _make_finnhub_collector,
    get_finnhub_collectors,
)


class TestFinnhubGeneric:
    def setup_method(self):
        _finnhub_cache.clear()

    def test_series_count(self):
        assert len(FINNHUB_SERIES) >= 20

    def test_no_duplicate_names(self):
        names = [t[1] for t in FINNHUB_SERIES]
        assert len(names) == len(set(names))

    def test_factory_creates_collector(self):
        cls = _make_finnhub_collector(
            "AAPL", "test_finnhub", "Test", "daily", "financial", "equity",
        )
        assert cls.meta.name == "test_finnhub"
        assert cls.meta.domain == "financial"
        assert cls.meta.requires_key is True
        assert isinstance(cls.meta, CollectorMeta)

    def test_get_collectors_returns_dict(self):
        collectors = get_finnhub_collectors()
        assert isinstance(collectors, dict)
        assert len(collectors) == len(FINNHUB_SERIES)
        assert "finnhub_aapl" in collectors
        assert "finnhub_nvda" in collectors
        assert "finnhub_gld" in collectors

    @patch("signal_noise.collector.finnhub_generic._get_finnhub_key", return_value="fake-key")
    @patch("signal_noise.collector.finnhub_generic.requests.get")
    def test_fetch_candle(self, mock_get, _mock_key):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "s": "ok",
            "t": [1706745600, 1706832000, 1706918400],
            "c": [185.85, 187.68, 189.97],
            "h": [186.95, 188.44, 190.32],
            "l": [184.35, 186.76, 188.48],
            "o": [185.03, 187.04, 189.33],
            "v": [55467800, 49702900, 42589600],
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_finnhub_collector(
            "AAPL", "test_aapl", "Test", "daily", "financial", "equity",
        )
        df = cls().fetch()
        assert len(df) == 3
        assert df["value"].iloc[0] == 185.85
        assert df["value"].iloc[2] == 189.97
        assert "date" in df.columns

    @patch("signal_noise.collector.finnhub_generic._get_finnhub_key", return_value="fake-key")
    @patch("signal_noise.collector.finnhub_generic.requests.get")
    def test_fetch_no_data(self, mock_get, _mock_key):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"s": "no_data"}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_finnhub_collector(
            "INVALID", "test_nodata", "Test", "daily", "financial", "equity",
        )
        with pytest.raises(RuntimeError, match="no data"):
            cls().fetch()

    @patch("signal_noise.collector.finnhub_generic._get_finnhub_key", return_value="fake-key")
    @patch("signal_noise.collector.finnhub_generic.requests.get")
    def test_fetch_empty_arrays(self, mock_get, _mock_key):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"s": "ok", "t": [], "c": []}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_finnhub_collector(
            "AAPL", "test_empty", "Test", "daily", "financial", "equity",
        )
        with pytest.raises(RuntimeError, match="empty candle"):
            cls().fetch()

    @patch("signal_noise.collector.finnhub_generic._get_finnhub_key", return_value="fake-key")
    @patch("signal_noise.collector.finnhub_generic.requests.get")
    def test_fetch_api_error(self, mock_get, _mock_key):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"s": "error"}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_finnhub_collector(
            "AAPL", "test_err", "Test", "daily", "financial", "equity",
        )
        with pytest.raises(RuntimeError, match="Finnhub API error"):
            cls().fetch()

    def test_all_series_valid_domain_category(self):
        for _, name, _, _, domain, category in FINNHUB_SERIES:
            assert domain in DOMAINS, f"{name}: invalid domain {domain}"
            assert category in CATEGORIES, f"{name}: invalid category {category}"


class TestFinnhubRegistration:
    def test_finnhub_registered(self):
        from signal_noise.collector import COLLECTORS

        for name in ["finnhub_aapl", "finnhub_nvda", "finnhub_gld", "finnhub_tlt"]:
            assert name in COLLECTORS, f"{name} not registered"
