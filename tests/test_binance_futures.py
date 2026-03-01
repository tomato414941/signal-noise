"""Tests for Binance Futures hourly collectors."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from signal_noise.collector.binance_futures import (
    FUTURES_SYMBOLS,
    _METRICS,
    _make_futures_collector,
    get_binance_futures_collectors,
)


FUNDING_RESPONSE = [
    {
        "symbol": "BTCUSDT",
        "fundingRate": "0.00010000",
        "fundingTime": 1704067200000,
        "markPrice": "42000.00",
    },
    {
        "symbol": "BTCUSDT",
        "fundingRate": "0.00015000",
        "fundingTime": 1704096000000,
        "markPrice": "42100.00",
    },
]

OI_RESPONSE = [
    {
        "symbol": "BTCUSDT",
        "sumOpenInterest": "75000.00000000",
        "sumOpenInterestValue": "3150000000.00",
        "timestamp": 1704067200000,
    },
    {
        "symbol": "BTCUSDT",
        "sumOpenInterest": "76000.00000000",
        "sumOpenInterestValue": "3192000000.00",
        "timestamp": 1704070800000,
    },
]

LS_RESPONSE = [
    {
        "symbol": "BTCUSDT",
        "longShortRatio": "1.2500",
        "longAccount": "0.5556",
        "shortAccount": "0.4444",
        "timestamp": 1704067200000,
    },
    {
        "symbol": "BTCUSDT",
        "longShortRatio": "1.3000",
        "longAccount": "0.5652",
        "shortAccount": "0.4348",
        "timestamp": 1704070800000,
    },
]

TAKER_RESPONSE = [
    {
        "buySellRatio": "0.9800",
        "buyVol": "5000.00",
        "sellVol": "5102.04",
        "timestamp": 1704067200000,
    },
    {
        "buySellRatio": "1.0200",
        "buyVol": "5100.00",
        "sellVol": "5000.00",
        "timestamp": 1704070800000,
    },
]


def _mock_response(data):
    resp = MagicMock()
    resp.json.return_value = data
    resp.raise_for_status = MagicMock()
    return resp


class TestFuturesFactory:
    @patch("signal_noise.collector.binance_futures.requests.get")
    def test_fetch_funding_rate(self, mock_get):
        mock_get.return_value = _mock_response(FUNDING_RESPONSE)

        cls = _make_futures_collector(
            "BTCUSDT", "btc", "funding",
            "/fapi/v1/fundingRate", "fundingRate", "fundingTime", "", "Funding Rate",
        )
        df = cls().fetch()
        assert len(df) == 2
        assert df["value"].iloc[0] == pytest.approx(0.0001)
        assert df["value"].iloc[1] == pytest.approx(0.00015)
        assert df["date"].is_monotonic_increasing

    @patch("signal_noise.collector.binance_futures.requests.get")
    def test_fetch_open_interest(self, mock_get):
        mock_get.return_value = _mock_response(OI_RESPONSE)

        cls = _make_futures_collector(
            "BTCUSDT", "btc", "oi",
            "/futures/data/openInterestHist", "sumOpenInterest", "timestamp", "1h", "Open Interest",
        )
        df = cls().fetch()
        assert len(df) == 2
        assert df["value"].iloc[0] == 75000.0
        assert df["value"].iloc[1] == 76000.0
        assert df["date"].is_monotonic_increasing

    @patch("signal_noise.collector.binance_futures.requests.get")
    def test_fetch_ls_ratio(self, mock_get):
        mock_get.return_value = _mock_response(LS_RESPONSE)

        cls = _make_futures_collector(
            "BTCUSDT", "btc", "ls_global",
            "/futures/data/globalLongShortAccountRatio", "longShortRatio", "timestamp", "1h", "Global L/S Ratio",
        )
        df = cls().fetch()
        assert len(df) == 2
        assert df["value"].iloc[0] == pytest.approx(1.25)
        assert df["value"].iloc[1] == pytest.approx(1.30)

    @patch("signal_noise.collector.binance_futures.requests.get")
    def test_fetch_taker_ratio(self, mock_get):
        mock_get.return_value = _mock_response(TAKER_RESPONSE)

        cls = _make_futures_collector(
            "BTCUSDT", "btc", "taker_ratio",
            "/futures/data/takerlongshortRatio", "buySellRatio", "timestamp", "1h", "Taker Buy/Sell Ratio",
        )
        df = cls().fetch()
        assert len(df) == 2
        assert df["value"].iloc[0] == pytest.approx(0.98)
        assert df["value"].iloc[1] == pytest.approx(1.02)

    @patch("signal_noise.collector.binance_futures.requests.get")
    def test_empty_response_raises(self, mock_get):
        mock_get.return_value = _mock_response([])

        cls = _make_futures_collector(
            "BTCUSDT", "btc", "funding",
            "/fapi/v1/fundingRate", "fundingRate", "fundingTime", "", "Funding Rate",
        )
        with pytest.raises(RuntimeError, match="No Binance Futures data"):
            cls().fetch()


class TestFuturesMeta:
    def test_domain_category(self):
        cls = _make_futures_collector(
            "ETHUSDT", "eth", "oi",
            "/futures/data/openInterestHist", "sumOpenInterest", "timestamp", "1h", "Open Interest",
        )
        assert cls.meta.domain == "financial"
        assert cls.meta.category == "crypto"

    def test_update_frequency(self):
        cls = _make_futures_collector(
            "SOLUSDT", "sol", "funding",
            "/fapi/v1/fundingRate", "fundingRate", "fundingTime", "", "Funding Rate",
        )
        assert cls.meta.update_frequency == "hourly"

    def test_name_format(self):
        cls = _make_futures_collector(
            "BTCUSDT", "btc", "ls_top",
            "/futures/data/topLongShortAccountRatio", "longShortRatio", "timestamp", "1h", "Top Trader L/S Ratio",
        )
        assert cls.meta.name == "futures_ls_top_btc"
        assert "BTCUSDT" in cls.meta.display_name


class TestFuturesRegistry:
    def test_total_count(self):
        collectors = get_binance_futures_collectors()
        assert len(collectors) == 15

    def test_no_duplicate_names(self):
        collectors = get_binance_futures_collectors()
        assert len(collectors) == len(FUTURES_SYMBOLS) * len(_METRICS)

    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS

        expected = [
            "futures_funding_btc", "futures_oi_btc", "futures_ls_global_btc",
            "futures_ls_top_eth", "futures_taker_ratio_sol",
        ]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"
