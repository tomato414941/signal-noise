"""Tests for Binance Futures hourly collectors (legacy + Layer 2)."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from signal_noise.collector.binance_futures import (
    FUTURES_SYMBOLS,
    _METRICS,
    _make_futures_collector,
    get_binance_futures_collectors,
    get_funding_rate_collectors,
    get_liquidation_collectors,
    get_open_interest_collectors,
    get_long_short_ratio_collectors,
)
from signal_noise.collector.base import CATEGORIES, DOMAINS


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

LIQUIDATION_RESPONSE = [
    {
        "symbol": "BTCUSDT",
        "price": "42000.00",
        "origQty": "1.000",
        "executedQty": "1.000",
        "side": "SELL",
        "time": 1704067200000,
    },
    {
        "symbol": "BTCUSDT",
        "price": "42100.00",
        "origQty": "0.500",
        "executedQty": "0.500",
        "side": "BUY",
        "time": 1704067500000,
    },
    {
        "symbol": "BTCUSDT",
        "price": "42200.00",
        "origQty": "2.000",
        "executedQty": "2.000",
        "side": "SELL",
        "time": 1704067800000,
    },
]

OI_SNAPSHOT_RESPONSE = {
    "symbol": "BTCUSDT",
    "openInterest": "75000.00000000",
    "time": 1704067200000,
}


def _mock_response(data):
    resp = MagicMock()
    resp.json.return_value = data
    resp.raise_for_status = MagicMock()
    return resp


# ── Legacy collectors tests ──

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
        assert cls.meta.domain == "markets"
        assert cls.meta.category == "crypto_derivatives"

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


# ── Layer 2 collector tests ──

class TestFundingRateCollectors:
    def test_factory_count(self):
        collectors = get_funding_rate_collectors()
        assert len(collectors) == 3

    @patch("signal_noise.collector.binance_futures.requests.get")
    def test_fetch(self, mock_get):
        mock_get.return_value = _mock_response(FUNDING_RESPONSE)
        collectors = get_funding_rate_collectors()
        cls = collectors["funding_rate_btc"]
        df = cls().fetch()
        assert len(df) == 2
        assert "timestamp" in df.columns
        assert "value" in df.columns
        assert df["value"].iloc[0] == pytest.approx(0.0001)
        assert df["timestamp"].is_monotonic_increasing

    @patch("signal_noise.collector.binance_futures.requests.get")
    def test_empty_response(self, mock_get):
        mock_get.return_value = _mock_response([])
        collectors = get_funding_rate_collectors()
        df = collectors["funding_rate_btc"]().fetch()
        assert df.empty

    def test_meta(self):
        collectors = get_funding_rate_collectors()
        for name, cls in collectors.items():
            assert cls.meta.domain == "markets"
            assert cls.meta.category == "crypto_derivatives"
            assert cls.meta.signal_type == "scalar"
            assert cls.meta.update_frequency == "hourly"
            assert cls.meta.interval == 3600


class TestLiquidationCollectors:
    def test_factory_count(self):
        collectors = get_liquidation_collectors()
        assert len(collectors) == 2

    @patch("signal_noise.collector.binance_futures.requests.get")
    def test_fetch_with_buckets(self, mock_get):
        mock_get.return_value = _mock_response(LIQUIDATION_RESPONSE)
        collectors = get_liquidation_collectors()
        df = collectors["liq_ratio_btc_1h"]().fetch()
        assert len(df) == 1  # all in same hour bucket
        # liq_long = 42000*1 + 42200*2 = 126400, liq_short = 42100*0.5 = 21050
        # ratio = 126400 / (126400 + 21050) ≈ 0.8572
        assert 0.8 < df["value"].iloc[0] < 0.9

    @patch("signal_noise.collector.binance_futures.requests.get")
    def test_empty_response(self, mock_get):
        mock_get.return_value = _mock_response([])
        collectors = get_liquidation_collectors()
        df = collectors["liq_ratio_btc_1h"]().fetch()
        assert df.empty

    def test_meta(self):
        collectors = get_liquidation_collectors()
        for name, cls in collectors.items():
            assert cls.meta.domain == "markets"
            assert cls.meta.category == "crypto_derivatives"
            assert cls.meta.signal_type == "scalar"
            assert cls.meta.interval == 3600


class TestOpenInterestCollectors:
    def test_factory_count(self):
        collectors = get_open_interest_collectors()
        assert len(collectors) == 3

    @patch("signal_noise.collector.binance_futures.requests.get")
    def test_fetch(self, mock_get):
        mock_get.return_value = _mock_response(OI_SNAPSHOT_RESPONSE)
        collectors = get_open_interest_collectors()
        df = collectors["oi_btc_1h"]().fetch()
        assert len(df) == 1
        assert df["value"].iloc[0] == 75000.0

    @patch("signal_noise.collector.binance_futures.requests.get")
    def test_empty_response(self, mock_get):
        mock_get.return_value = _mock_response({})
        collectors = get_open_interest_collectors()
        df = collectors["oi_btc_1h"]().fetch()
        assert df.empty

    def test_meta(self):
        collectors = get_open_interest_collectors()
        for name, cls in collectors.items():
            assert cls.meta.domain == "markets"
            assert cls.meta.category == "crypto_derivatives"
            assert cls.meta.signal_type == "scalar"
            assert cls.meta.interval == 3600


class TestLongShortRatioCollectors:
    def test_factory_count(self):
        collectors = get_long_short_ratio_collectors()
        assert len(collectors) == 3

    @patch("signal_noise.collector.binance_futures.requests.get")
    def test_fetch(self, mock_get):
        mock_get.return_value = _mock_response(LS_RESPONSE)
        collectors = get_long_short_ratio_collectors()
        df = collectors["ls_ratio_global_btc"]().fetch()
        assert len(df) == 2
        assert df["value"].iloc[0] == pytest.approx(1.25)

    @patch("signal_noise.collector.binance_futures.requests.get")
    def test_empty_response(self, mock_get):
        mock_get.return_value = _mock_response([])
        collectors = get_long_short_ratio_collectors()
        df = collectors["ls_ratio_global_btc"]().fetch()
        assert df.empty

    def test_meta(self):
        collectors = get_long_short_ratio_collectors()
        for name, cls in collectors.items():
            assert cls.meta.domain == "markets"
            assert cls.meta.category == "crypto_derivatives"
            assert cls.meta.signal_type == "scalar"
            assert cls.meta.interval == 3600


class TestLayer2Registration:
    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS

        expected_names = [
            "funding_rate_btc", "funding_rate_eth", "funding_rate_sol",
            "liq_ratio_btc_1h", "liq_ratio_eth_1h",
            "oi_btc_1h", "oi_eth_1h", "oi_sol_1h",
            "ls_ratio_global_btc", "ls_ratio_top_btc", "ls_position_ratio_btc",
        ]
        for name in expected_names:
            assert name in COLLECTORS, f"{name} not registered"

    def test_total_new_count(self):
        all_new = {}
        all_new.update(get_funding_rate_collectors())
        all_new.update(get_liquidation_collectors())
        all_new.update(get_open_interest_collectors())
        all_new.update(get_long_short_ratio_collectors())
        assert len(all_new) == 11

    def test_taxonomy_validation(self):
        all_new = {}
        all_new.update(get_funding_rate_collectors())
        all_new.update(get_liquidation_collectors())
        all_new.update(get_open_interest_collectors())
        all_new.update(get_long_short_ratio_collectors())
        for name, cls in all_new.items():
            assert cls.meta.domain in DOMAINS, f"{name}: invalid domain"
            assert cls.meta.category in CATEGORIES, f"{name}: invalid category"
