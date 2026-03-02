"""Tests for cross-exchange BTC intelligence collectors."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from signal_noise.collector._cache import SharedAPICache
from signal_noise.collector.base import CATEGORIES, DOMAINS
from signal_noise.collector.cross_exchange import (
    CrossExchangeSpreadBinanceBybitCollector,
    CrossExchangeSpreadBinanceOkxCollector,
    LeadLagCollector,
    VolumeDominanceCollector,
    _compute_lead_lag,
    _xex_cache,
)


@pytest.fixture(autouse=True)
def _clear_cache():
    _xex_cache.clear()
    yield
    _xex_cache.clear()


def _make_ticker(last: float, volume: float) -> dict:
    return {
        "last": last,
        "quoteVolume": volume,
        "symbol": "BTC/USDT",
        "bid": last - 1,
        "ask": last + 1,
    }


def _make_ohlcv(closes: list[float]) -> list[list]:
    base_ts = 1_700_000_000_000
    return [
        [base_ts + i * 3_600_000, c - 10, c + 10, c - 20, c, 1000.0]
        for i, c in enumerate(closes)
    ]


# ── Helper tests ──


class TestComputeLeadLag:
    def test_identical_returns_zero(self):
        r = np.array([0.01, -0.02, 0.03, 0.01, -0.01])
        assert _compute_lead_lag(r, r) == pytest.approx(0.0)

    def test_positive_when_a_leads(self):
        a = np.array([0.01, -0.02, 0.03, 0.01, -0.01, 0.02, -0.03, 0.01, 0.02, -0.01])
        b = np.array([0.0, 0.01, -0.02, 0.03, 0.01, -0.01, 0.02, -0.03, 0.01, 0.02])
        ll = _compute_lead_lag(a, b)
        assert ll > 0

    def test_short_returns_zero(self):
        assert _compute_lead_lag(np.array([0.01, 0.02]), np.array([0.01, 0.02])) == 0.0

    def test_empty_returns_zero(self):
        assert _compute_lead_lag(np.array([]), np.array([])) == 0.0

    def test_constant_returns_zero(self):
        r = np.array([0.01, 0.01, 0.01, 0.01, 0.01])
        assert _compute_lead_lag(r, r) == 0.0


# ── Spread collectors ──


class TestSpreadBinanceBybit:
    @patch("signal_noise.collector.cross_exchange._get_exchange")
    def test_fetch(self, mock_get_ex):
        mock_ex = MagicMock()
        mock_ex.fetch_ticker.side_effect = lambda s: (
            _make_ticker(50000.0, 1e9) if mock_ex._name == "binance"
            else _make_ticker(50005.0, 8e8)
        )

        def get_ex(name):
            mock_ex._name = name
            return mock_ex

        mock_get_ex.side_effect = get_ex
        df = CrossExchangeSpreadBinanceBybitCollector().fetch()
        assert len(df) == 1
        # (50005 - 50000) / 50000 * 10000 = 1.0 bps
        assert df["value"].iloc[0] == pytest.approx(1.0)

    @patch("signal_noise.collector.cross_exchange._get_exchange")
    def test_equal_prices_zero_spread(self, mock_get_ex):
        mock_ex = MagicMock()
        mock_ex.fetch_ticker.return_value = _make_ticker(50000.0, 1e9)
        mock_get_ex.return_value = mock_ex
        df = CrossExchangeSpreadBinanceBybitCollector().fetch()
        assert df["value"].iloc[0] == pytest.approx(0.0)

    def test_meta(self):
        m = CrossExchangeSpreadBinanceBybitCollector.meta
        assert m.name == "spread_binance_bybit_btc"
        assert m.domain == "markets"
        assert m.category == "crypto"
        assert m.signal_type == "scalar"
        assert m.interval == 3600


class TestSpreadBinanceOkx:
    @patch("signal_noise.collector.cross_exchange._get_exchange")
    def test_fetch(self, mock_get_ex):
        mock_ex = MagicMock()
        mock_ex.fetch_ticker.side_effect = lambda s: (
            _make_ticker(50000.0, 1e9) if mock_ex._name == "binance"
            else _make_ticker(49990.0, 7e8)
        )

        def get_ex(name):
            mock_ex._name = name
            return mock_ex

        mock_get_ex.side_effect = get_ex
        df = CrossExchangeSpreadBinanceOkxCollector().fetch()
        assert len(df) == 1
        # (49990 - 50000) / 50000 * 10000 = -2.0 bps
        assert df["value"].iloc[0] == pytest.approx(-2.0)

    def test_meta(self):
        m = CrossExchangeSpreadBinanceOkxCollector.meta
        assert m.name == "spread_binance_okx_btc"
        assert m.interval == 3600


# ── Volume dominance ──


class TestVolumeDominance:
    @patch("signal_noise.collector.cross_exchange._get_exchange")
    def test_fetch(self, mock_get_ex):
        volumes = {"binance": 5e9, "bybit": 3e9, "okx": 2e9}
        mock_ex = MagicMock()
        mock_ex.fetch_ticker.side_effect = lambda s: _make_ticker(50000.0, volumes[mock_ex._name])

        def get_ex(name):
            mock_ex._name = name
            return mock_ex

        mock_get_ex.side_effect = get_ex
        df = VolumeDominanceCollector().fetch()
        assert len(df) == 1
        # 5e9 / (5e9 + 3e9 + 2e9) = 0.5
        assert df["value"].iloc[0] == pytest.approx(0.5)

    @patch("signal_noise.collector.cross_exchange._get_exchange")
    def test_zero_volume(self, mock_get_ex):
        mock_ex = MagicMock()
        mock_ex.fetch_ticker.return_value = _make_ticker(50000.0, 0.0)
        mock_get_ex.return_value = mock_ex
        df = VolumeDominanceCollector().fetch()
        assert df["value"].iloc[0] == pytest.approx(0.0)

    def test_meta(self):
        m = VolumeDominanceCollector.meta
        assert m.name == "volume_dominance_btc"
        assert m.interval == 3600


# ── Lead-lag ──


class TestLeadLag:
    @patch("signal_noise.collector.cross_exchange._get_exchange")
    def test_fetch(self, mock_get_ex):
        closes_b = [100 + i * 0.5 for i in range(25)]
        closes_o = [100] + [100 + i * 0.5 for i in range(24)]

        def get_ex(name):
            mock_ex = MagicMock()
            if name == "binance":
                mock_ex.fetch_ohlcv.return_value = _make_ohlcv(closes_b)
            else:
                mock_ex.fetch_ohlcv.return_value = _make_ohlcv(closes_o)
            return mock_ex

        mock_get_ex.side_effect = get_ex
        df = LeadLagCollector().fetch()
        assert len(df) == 1
        assert isinstance(df["value"].iloc[0], float)

    @patch("signal_noise.collector.cross_exchange._get_exchange")
    def test_short_data_returns_zero(self, mock_get_ex):
        mock_ex = MagicMock()
        mock_ex.fetch_ohlcv.return_value = _make_ohlcv([100.0, 101.0])
        mock_get_ex.return_value = mock_ex
        df = LeadLagCollector().fetch()
        assert df["value"].iloc[0] == pytest.approx(0.0)

    def test_meta(self):
        m = LeadLagCollector.meta
        assert m.name == "lead_lag_btc"
        assert m.interval == 3600


# ── Registration & taxonomy ──


class TestRegistration:
    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS

        expected = [
            "spread_binance_bybit_btc",
            "spread_binance_okx_btc",
            "volume_dominance_btc",
            "lead_lag_btc",
        ]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"

    def test_taxonomy_valid(self):
        collectors = [
            CrossExchangeSpreadBinanceBybitCollector,
            CrossExchangeSpreadBinanceOkxCollector,
            VolumeDominanceCollector,
            LeadLagCollector,
        ]
        for cls in collectors:
            assert cls.meta.domain in DOMAINS, f"{cls.meta.name}: invalid domain"
            assert cls.meta.category in CATEGORIES, f"{cls.meta.name}: invalid category"
            assert cls.meta.interval == 3600, f"{cls.meta.name}: wrong interval"
