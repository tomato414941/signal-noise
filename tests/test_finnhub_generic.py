"""Tests for Finnhub L2 collectors (metric series, recommendations, insider, earnings)."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from signal_noise.collector.base import CATEGORIES, DOMAINS, CollectorMeta
from signal_noise.collector.finnhub_generic import (
    FINNHUB_EARNINGS_SERIES,
    FINNHUB_INSIDER_SERIES,
    FINNHUB_METRIC_SERIES,
    FINNHUB_REC_SERIES,
    _finnhub_cache,
    _make_earnings_collector,
    _make_insider_collector,
    _make_metric_collector,
    _make_rec_collector,
    get_finnhub_collectors,
)


class TestFinnhubMetric:
    def setup_method(self):
        _finnhub_cache.clear()

    def test_metric_series_count(self):
        from signal_noise.collector.finnhub_generic import _STOCKS, _METRICS
        assert len(FINNHUB_METRIC_SERIES) == len(_STOCKS) * len(_METRICS)

    def test_no_duplicate_metric_names(self):
        names = [t[2] for t in FINNHUB_METRIC_SERIES]
        assert len(names) == len(set(names))

    def test_factory_creates_collector(self):
        cls = _make_metric_collector(
            "AAPL", "eps", "test_eps", "Test EPS",
            "quarterly", "markets", "equity",
        )
        assert cls.meta.name == "test_eps"
        assert cls.meta.requires_key is True
        assert isinstance(cls.meta, CollectorMeta)

    @patch("signal_noise.collector.finnhub_generic._get_finnhub_key", return_value="fake")
    @patch("signal_noise.collector.finnhub_generic.requests.get")
    def test_fetch_metric(self, mock_get, _mock_key):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "metric": {},
            "series": {
                "quarterly": {
                    "eps": [
                        {"period": "2025-12-27", "v": 2.84},
                        {"period": "2025-09-27", "v": 1.85},
                        {"period": "2025-06-28", "v": 1.57},
                    ]
                }
            },
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_metric_collector(
            "AAPL", "eps", "test_eps", "Test", "quarterly", "markets", "equity",
        )
        df = cls().fetch()
        assert len(df) == 3
        assert df["value"].iloc[-1] == 2.84
        assert df["date"].iloc[0].month == 6

    @patch("signal_noise.collector.finnhub_generic._get_finnhub_key", return_value="fake")
    @patch("signal_noise.collector.finnhub_generic.requests.get")
    def test_fetch_metric_no_data(self, mock_get, _mock_key):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"metric": {}, "series": {"quarterly": {}}}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_metric_collector(
            "AAPL", "eps", "test_empty", "Test", "quarterly", "markets", "equity",
        )
        with pytest.raises(RuntimeError, match="no quarterly eps data"):
            cls().fetch()

    @patch("signal_noise.collector.finnhub_generic._get_finnhub_key", return_value="fake")
    @patch("signal_noise.collector.finnhub_generic.requests.get")
    def test_fetch_metric_api_error(self, mock_get, _mock_key):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"error": "Access denied"}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_metric_collector(
            "AAPL", "eps", "test_err", "Test", "quarterly", "markets", "equity",
        )
        with pytest.raises(RuntimeError, match="metric error"):
            cls().fetch()

    @patch("signal_noise.collector.finnhub_generic._get_finnhub_key", return_value="fake")
    @patch("signal_noise.collector.finnhub_generic.requests.get")
    def test_cache_shared_between_metrics(self, mock_get, _mock_key):
        """Multiple metric collectors for the same stock share one API call."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "metric": {},
            "series": {
                "quarterly": {
                    "eps": [{"period": "2025-12-27", "v": 2.84}],
                    "peTTM": [{"period": "2025-12-27", "v": 32.9}],
                }
            },
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls_eps = _make_metric_collector(
            "AAPL", "eps", "t_eps", "T", "quarterly", "markets", "equity",
        )
        cls_pe = _make_metric_collector(
            "AAPL", "peTTM", "t_pe", "T", "quarterly", "markets", "equity",
        )
        cls_eps().fetch()
        cls_pe().fetch()
        assert mock_get.call_count == 1


class TestFinnhubRecommendation:
    def setup_method(self):
        _finnhub_cache.clear()

    def test_rec_series_count(self):
        from signal_noise.collector.finnhub_generic import _STOCKS
        assert len(FINNHUB_REC_SERIES) == len(_STOCKS)

    @patch("signal_noise.collector.finnhub_generic._get_finnhub_key", return_value="fake")
    @patch("signal_noise.collector.finnhub_generic.requests.get")
    def test_fetch_recommendation(self, mock_get, _mock_key):
        mock_resp = MagicMock()
        mock_resp.json.return_value = [
            {
                "buy": 20, "hold": 10, "sell": 2,
                "strongBuy": 15, "strongSell": 0,
                "period": "2026-02-01", "symbol": "AAPL",
            },
            {
                "buy": 18, "hold": 12, "sell": 3,
                "strongBuy": 14, "strongSell": 1,
                "period": "2026-01-01", "symbol": "AAPL",
            },
        ]
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_rec_collector(
            "AAPL", "test_rec", "Test Rec", "monthly", "sentiment", "equity",
        )
        df = cls().fetch()
        assert len(df) == 2
        expected = (15 * 5 + 20 * 4 + 10 * 3 + 2 * 2 + 0 * 1) / 47
        assert abs(df["value"].iloc[-1] - round(expected, 4)) < 0.01

    @patch("signal_noise.collector.finnhub_generic._get_finnhub_key", return_value="fake")
    @patch("signal_noise.collector.finnhub_generic.requests.get")
    def test_fetch_recommendation_empty(self, mock_get, _mock_key):
        mock_resp = MagicMock()
        mock_resp.json.return_value = []
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_rec_collector(
            "SPY", "test_norec", "Test", "monthly", "sentiment", "equity",
        )
        with pytest.raises(RuntimeError, match="no recommendations"):
            cls().fetch()

    @patch("signal_noise.collector.finnhub_generic._get_finnhub_key", return_value="fake")
    @patch("signal_noise.collector.finnhub_generic.requests.get")
    def test_recommendation_score_range(self, mock_get, _mock_key):
        """Score should be between 1.0 and 5.0."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = [
            {
                "buy": 0, "hold": 0, "sell": 0,
                "strongBuy": 0, "strongSell": 10,
                "period": "2026-01-01", "symbol": "TEST",
            },
        ]
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_rec_collector(
            "TEST", "test_low", "Test", "monthly", "sentiment", "equity",
        )
        df = cls().fetch()
        assert df["value"].iloc[0] == 1.0

        _finnhub_cache.clear()
        mock_resp.json.return_value = [
            {
                "buy": 0, "hold": 0, "sell": 0,
                "strongBuy": 10, "strongSell": 0,
                "period": "2026-01-01", "symbol": "TEST",
            },
        ]
        df = cls().fetch()
        assert df["value"].iloc[0] == 5.0


class TestFinnhubInsider:
    def setup_method(self):
        _finnhub_cache.clear()

    def test_insider_series_count(self):
        from signal_noise.collector.finnhub_generic import _STOCKS
        assert len(FINNHUB_INSIDER_SERIES) == len(_STOCKS)

    @patch("signal_noise.collector.finnhub_generic._get_finnhub_key", return_value="fake")
    @patch("signal_noise.collector.finnhub_generic.requests.get")
    def test_fetch_insider(self, mock_get, _mock_key):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "data": [
                {"symbol": "AAPL", "year": 2024, "month": 2, "change": -89388, "mspr": -63.74},
                {"symbol": "AAPL", "year": 2024, "month": 5, "change": -81849, "mspr": -100.0},
                {"symbol": "AAPL", "year": 2024, "month": 9, "change": 549640, "mspr": 100.0},
            ],
            "symbol": "AAPL",
        }
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_insider_collector(
            "AAPL", "test_insider", "Test Insider", "monthly", "sentiment", "sentiment",
        )
        df = cls().fetch()
        assert len(df) == 3
        assert df["value"].iloc[0] == -63.74  # Feb sorted first
        assert df["date"].iloc[0].month == 2

    @patch("signal_noise.collector.finnhub_generic._get_finnhub_key", return_value="fake")
    @patch("signal_noise.collector.finnhub_generic.requests.get")
    def test_fetch_insider_empty(self, mock_get, _mock_key):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"data": [], "symbol": "AAPL"}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_insider_collector(
            "AAPL", "test_noinsider", "Test", "monthly", "sentiment", "sentiment",
        )
        with pytest.raises(RuntimeError, match="no insider sentiment"):
            cls().fetch()


class TestFinnhubEarnings:
    def setup_method(self):
        _finnhub_cache.clear()

    def test_earnings_series_count(self):
        from signal_noise.collector.finnhub_generic import _STOCKS
        assert len(FINNHUB_EARNINGS_SERIES) == len(_STOCKS)

    @patch("signal_noise.collector.finnhub_generic._get_finnhub_key", return_value="fake")
    @patch("signal_noise.collector.finnhub_generic.requests.get")
    def test_fetch_earnings(self, mock_get, _mock_key):
        mock_resp = MagicMock()
        mock_resp.json.return_value = [
            {
                "actual": 2.84, "estimate": 2.7257,
                "period": "2025-12-31", "quarter": 1,
                "surprise": 0.1143, "surprisePercent": 4.1934,
                "symbol": "AAPL", "year": 2026,
            },
            {
                "actual": 1.85, "estimate": 1.8075,
                "period": "2025-09-30", "quarter": 4,
                "surprise": 0.0425, "surprisePercent": 2.3513,
                "symbol": "AAPL", "year": 2025,
            },
        ]
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_earnings_collector(
            "AAPL", "test_earn", "Test Earn", "quarterly", "markets", "equity",
        )
        df = cls().fetch()
        assert len(df) == 2
        assert df["value"].iloc[-1] == 4.1934

    @patch("signal_noise.collector.finnhub_generic._get_finnhub_key", return_value="fake")
    @patch("signal_noise.collector.finnhub_generic.requests.get")
    def test_fetch_earnings_empty(self, mock_get, _mock_key):
        mock_resp = MagicMock()
        mock_resp.json.return_value = []
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        cls = _make_earnings_collector(
            "AAPL", "test_noearn", "Test", "quarterly", "markets", "equity",
        )
        with pytest.raises(RuntimeError, match="no earnings"):
            cls().fetch()


class TestFinnhubValidation:
    def test_all_metric_valid_domain_category(self):
        for _, _, name, _, _, domain, category in FINNHUB_METRIC_SERIES:
            assert domain in DOMAINS, f"{name}: invalid domain {domain}"
            assert category in CATEGORIES, f"{name}: invalid category {category}"

    def test_all_rec_valid_domain_category(self):
        for _, name, _, _, domain, category in FINNHUB_REC_SERIES:
            assert domain in DOMAINS, f"{name}: invalid domain {domain}"
            assert category in CATEGORIES, f"{name}: invalid category {category}"

    def test_all_insider_valid_domain_category(self):
        for _, name, _, _, domain, category in FINNHUB_INSIDER_SERIES:
            assert domain in DOMAINS, f"{name}: invalid domain {domain}"
            assert category in CATEGORIES, f"{name}: invalid category {category}"

    def test_all_earnings_valid_domain_category(self):
        for _, name, _, _, domain, category in FINNHUB_EARNINGS_SERIES:
            assert domain in DOMAINS, f"{name}: invalid domain {domain}"
            assert category in CATEGORIES, f"{name}: invalid category {category}"

    def test_no_duplicate_names_across_all(self):
        all_names = (
            [t[2] for t in FINNHUB_METRIC_SERIES]
            + [t[1] for t in FINNHUB_REC_SERIES]
            + [t[1] for t in FINNHUB_INSIDER_SERIES]
            + [t[1] for t in FINNHUB_EARNINGS_SERIES]
        )
        assert len(all_names) == len(set(all_names))

    def test_total_collector_count(self):
        from signal_noise.collector.finnhub_generic import _STOCKS, _METRICS
        collectors = get_finnhub_collectors()
        n = len(_STOCKS)
        expected = n * len(_METRICS) + n + n + n  # metric + rec + insider + earnings
        assert len(collectors) == expected


class TestFinnhubRegistration:
    def test_finnhub_registered(self):
        from signal_noise.collector import COLLECTORS

        for name in [
            "finnhub_aapl_eps", "finnhub_aapl_rec",
            "finnhub_aapl_insider", "finnhub_aapl_earnings",
            "finnhub_amd_eps", "finnhub_gs_rec",
            "finnhub_fdx_insider", "finnhub_len_earnings",
        ]:
            assert name in COLLECTORS, f"{name} not registered"
