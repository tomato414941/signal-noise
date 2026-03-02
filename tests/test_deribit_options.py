"""Tests for Deribit BTC/ETH options collectors."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from signal_noise.collector._cache import SharedAPICache
from signal_noise.collector.base import CATEGORIES, DOMAINS
from signal_noise.collector.deribit_options import (
    _bs_delta,
    _bs_gamma,
    _compute_gex,
    _compute_max_pain,
    _deribit_cache,
    _expiry_to_timestamp,
    _parse_instrument_name,
    get_deribit_options_collectors,
)


# ── Mock API responses ──

DVOL_RESPONSE = {
    "result": {
        "data": [
            [1704060000000, 55.0, 56.0, 54.0, 55.5],
            [1704063600000, 55.5, 57.0, 55.0, 56.2],
        ],
    },
}

INSTRUMENTS_RESPONSE = {
    "result": [
        {"instrument_name": "BTC-7MAR26-90000-C", "kind": "option"},
        {"instrument_name": "BTC-7MAR26-90000-P", "kind": "option"},
        {"instrument_name": "BTC-7MAR26-85000-P", "kind": "option"},
        {"instrument_name": "BTC-7MAR26-85000-C", "kind": "option"},
        {"instrument_name": "BTC-7MAR26-95000-C", "kind": "option"},
        {"instrument_name": "BTC-7MAR26-95000-P", "kind": "option"},
        {"instrument_name": "BTC-7MAR26-80000-P", "kind": "option"},
        {"instrument_name": "BTC-7MAR26-80000-C", "kind": "option"},
        {"instrument_name": "BTC-7MAR26-100000-C", "kind": "option"},
        {"instrument_name": "BTC-7MAR26-100000-P", "kind": "option"},
        {"instrument_name": "BTC-14MAR26-90000-C", "kind": "option"},
        {"instrument_name": "BTC-14MAR26-90000-P", "kind": "option"},
    ],
}

BOOK_SUMMARY_RESPONSE = {
    "result": [
        {"instrument_name": "BTC-7MAR26-90000-C", "mark_iv": 55.0, "volume": 100.0, "open_interest": 500.0},
        {"instrument_name": "BTC-7MAR26-90000-P", "mark_iv": 56.0, "volume": 80.0, "open_interest": 400.0},
        {"instrument_name": "BTC-7MAR26-85000-P", "mark_iv": 60.0, "volume": 50.0, "open_interest": 300.0},
        {"instrument_name": "BTC-7MAR26-85000-C", "mark_iv": 52.0, "volume": 30.0, "open_interest": 200.0},
        {"instrument_name": "BTC-7MAR26-95000-C", "mark_iv": 58.0, "volume": 40.0, "open_interest": 350.0},
        {"instrument_name": "BTC-7MAR26-95000-P", "mark_iv": 62.0, "volume": 70.0, "open_interest": 250.0},
        {"instrument_name": "BTC-7MAR26-80000-P", "mark_iv": 65.0, "volume": 60.0, "open_interest": 150.0},
        {"instrument_name": "BTC-7MAR26-80000-C", "mark_iv": 50.0, "volume": 20.0, "open_interest": 100.0},
        {"instrument_name": "BTC-7MAR26-100000-C", "mark_iv": 61.0, "volume": 35.0, "open_interest": 180.0},
        {"instrument_name": "BTC-7MAR26-100000-P", "mark_iv": 68.0, "volume": 45.0, "open_interest": 120.0},
        {"instrument_name": "BTC-14MAR26-90000-C", "mark_iv": 54.0, "volume": 25.0, "open_interest": 200.0},
        {"instrument_name": "BTC-14MAR26-90000-P", "mark_iv": 55.0, "volume": 30.0, "open_interest": 180.0},
    ],
}

INDEX_PRICE_RESPONSE = {"result": {"index_price": 90000.0}}

TICKER_PUT_RESPONSE = {"result": {"mark_iv": 62.0, "instrument_name": "BTC-7MAR26-85000-P"}}
TICKER_CALL_RESPONSE = {"result": {"mark_iv": 58.0, "instrument_name": "BTC-7MAR26-95000-C"}}


def _mock_response(data):
    resp = MagicMock()
    resp.json.return_value = data
    resp.raise_for_status = MagicMock()
    return resp


@pytest.fixture(autouse=True)
def _clear_cache():
    _deribit_cache.clear()
    yield
    _deribit_cache.clear()


# Get collector classes from factory
_all_collectors = get_deribit_options_collectors()
DeribitBTCDVOLCollector = _all_collectors["iv_atm_btc_30d"]
DeribitBTCATM7dCollector = _all_collectors["iv_atm_btc_7d"]
DeribitBTCSkew7dCollector = _all_collectors["iv_skew_btc_7d"]
DeribitBTCPCRCollector = _all_collectors["put_call_ratio_btc"]
DeribitBTCMaxPainCollector = _all_collectors["max_pain_btc"]
DeribitBTCGEXCollector = _all_collectors["gamma_exposure_btc"]


# ── Helper function tests ──

class TestParseInstrumentName:
    def test_call(self):
        r = _parse_instrument_name("BTC-28MAR26-90000-C")
        assert r["expiry_str"] == "28MAR26"
        assert r["strike"] == 90000.0
        assert r["option_type"] == "C"
        assert r["currency"] == "BTC"

    def test_put(self):
        r = _parse_instrument_name("BTC-7MAR26-85000-P")
        assert r["expiry_str"] == "7MAR26"
        assert r["strike"] == 85000.0
        assert r["option_type"] == "P"

    def test_eth(self):
        r = _parse_instrument_name("ETH-28MAR26-3000-C")
        assert r is not None
        assert r["currency"] == "ETH"
        assert r["strike"] == 3000.0

    def test_invalid(self):
        assert _parse_instrument_name("not-an-option") is None


class TestExpiryToTimestamp:
    def test_basic(self):
        ts = _expiry_to_timestamp("28MAR26")
        assert ts.year == 2026
        assert ts.month == 3
        assert ts.day == 28
        assert ts.hour == 8
        assert str(ts.tzinfo) == "UTC"

    def test_single_digit_day(self):
        ts = _expiry_to_timestamp("7MAR26")
        assert ts.day == 7


class TestBsDelta:
    def test_atm_call_near_half(self):
        delta = _bs_delta(spot=90000, strike=90000, t=7 / 365.25, iv=0.55, is_call=True)
        assert 0.45 < delta < 0.60

    def test_atm_put_near_neg_half(self):
        delta = _bs_delta(spot=90000, strike=90000, t=7 / 365.25, iv=0.55, is_call=False)
        assert -0.60 < delta < -0.40

    def test_deep_itm_call(self):
        delta = _bs_delta(spot=100000, strike=50000, t=30 / 365.25, iv=0.5, is_call=True)
        assert delta > 0.95

    def test_deep_otm_put(self):
        delta = _bs_delta(spot=100000, strike=50000, t=30 / 365.25, iv=0.5, is_call=False)
        assert delta > -0.05

    def test_zero_time(self):
        assert _bs_delta(90000, 90000, 0, 0.5, True) == 0.0

    def test_zero_iv(self):
        assert _bs_delta(90000, 90000, 0.1, 0, True) == 0.0


class TestBsGamma:
    def test_atm_positive(self):
        g = _bs_gamma(spot=90000, strike=90000, t=7 / 365.25, iv=0.55)
        assert g > 0

    def test_far_otm_small(self):
        g_atm = _bs_gamma(spot=90000, strike=90000, t=7 / 365.25, iv=0.55)
        g_otm = _bs_gamma(spot=90000, strike=150000, t=7 / 365.25, iv=0.55)
        assert g_otm < g_atm

    def test_zero_time(self):
        assert _bs_gamma(90000, 90000, 0, 0.5) == 0.0


class TestComputeMaxPain:
    def test_basic(self):
        book = BOOK_SUMMARY_RESPONSE["result"]
        instruments = INSTRUMENTS_RESPONSE["result"]
        strike = _compute_max_pain(book, instruments, "7MAR26")
        assert strike is not None
        assert 80000 <= strike <= 100000

    def test_empty_book(self):
        assert _compute_max_pain([], [], "7MAR26") is None


class TestComputeGex:
    def test_returns_number(self):
        book = BOOK_SUMMARY_RESPONSE["result"]
        instruments = INSTRUMENTS_RESPONSE["result"]
        gex = _compute_gex(book, instruments, 90000.0, top_n=5)
        assert isinstance(gex, float)

    def test_empty_book(self):
        gex = _compute_gex([], [], 90000.0)
        assert gex == 0.0


# ── Collector tests (BTC via factory) ──

class TestDeribitDVOL:
    @patch("signal_noise.collector.deribit_options.requests.get")
    def test_fetch(self, mock_get):
        mock_get.return_value = _mock_response(DVOL_RESPONSE)
        df = DeribitBTCDVOLCollector().fetch()
        assert len(df) == 1
        assert "timestamp" in df.columns
        assert "value" in df.columns
        assert df["value"].iloc[0] == pytest.approx(56.2)

    @patch("signal_noise.collector.deribit_options.requests.get")
    def test_empty_response(self, mock_get):
        mock_get.return_value = _mock_response({"result": {"data": []}})
        with pytest.raises(RuntimeError, match="No DVOL data"):
            DeribitBTCDVOLCollector().fetch()

    def test_meta(self):
        m = DeribitBTCDVOLCollector.meta
        assert m.name == "iv_atm_btc_30d"
        assert m.domain == "financial"
        assert m.category == "crypto_derivatives"
        assert m.interval == 3600


class TestDeribitATM7d:
    @patch("signal_noise.collector.deribit_options.requests.get")
    def test_fetch(self, mock_get):
        def side_effect(url, **kwargs):
            if "get_instruments" in url:
                return _mock_response(INSTRUMENTS_RESPONSE)
            if "get_book_summary" in url:
                return _mock_response(BOOK_SUMMARY_RESPONSE)
            if "get_index_price" in url:
                return _mock_response(INDEX_PRICE_RESPONSE)
            return _mock_response({"result": []})

        mock_get.side_effect = side_effect
        df = DeribitBTCATM7dCollector().fetch()
        assert len(df) == 1
        assert df["value"].iloc[0] > 0

    @patch("signal_noise.collector.deribit_options.requests.get")
    def test_empty_instruments(self, mock_get):
        def side_effect(url, **kwargs):
            if "get_index_price" in url:
                return _mock_response(INDEX_PRICE_RESPONSE)
            return _mock_response({"result": []})

        mock_get.side_effect = side_effect
        with pytest.raises(RuntimeError):
            DeribitBTCATM7dCollector().fetch()

    def test_meta(self):
        m = DeribitBTCATM7dCollector.meta
        assert m.name == "iv_atm_btc_7d"
        assert m.interval == 3600


class TestDeribitSkew7d:
    @patch("signal_noise.collector.deribit_options.requests.get")
    def test_fetch(self, mock_get):
        def side_effect(url, **kwargs):
            params = kwargs.get("params", {})
            if "get_instruments" in url:
                return _mock_response(INSTRUMENTS_RESPONSE)
            if "get_book_summary" in url:
                return _mock_response(BOOK_SUMMARY_RESPONSE)
            if "get_index_price" in url:
                return _mock_response(INDEX_PRICE_RESPONSE)
            if "ticker" in url:
                inst = params.get("instrument_name", "")
                if "-P" in inst:
                    return _mock_response(TICKER_PUT_RESPONSE)
                return _mock_response(TICKER_CALL_RESPONSE)
            return _mock_response({"result": {}})

        mock_get.side_effect = side_effect
        df = DeribitBTCSkew7dCollector().fetch()
        assert len(df) == 1
        assert df["value"].iloc[0] == pytest.approx(4.0)

    def test_meta(self):
        m = DeribitBTCSkew7dCollector.meta
        assert m.name == "iv_skew_btc_7d"
        assert m.interval == 3600


class TestDeribitPutCallRatio:
    @patch("signal_noise.collector.deribit_options.requests.get")
    def test_fetch(self, mock_get):
        mock_get.return_value = _mock_response(BOOK_SUMMARY_RESPONSE)
        df = DeribitBTCPCRCollector().fetch()
        assert len(df) == 1
        expected = 335.0 / 250.0
        assert df["value"].iloc[0] == pytest.approx(expected)

    @patch("signal_noise.collector.deribit_options.requests.get")
    def test_empty_response(self, mock_get):
        mock_get.return_value = _mock_response({"result": []})
        with pytest.raises(RuntimeError, match="No Deribit book summary"):
            DeribitBTCPCRCollector().fetch()

    @patch("signal_noise.collector.deribit_options.requests.get")
    def test_zero_call_volume(self, mock_get):
        mock_get.return_value = _mock_response({"result": [
            {"instrument_name": "BTC-7MAR26-90000-P", "mark_iv": 55.0, "volume": 100.0, "open_interest": 500.0},
        ]})
        with pytest.raises(RuntimeError, match="No call volume"):
            DeribitBTCPCRCollector().fetch()

    def test_meta(self):
        m = DeribitBTCPCRCollector.meta
        assert m.name == "put_call_ratio_btc"
        assert m.interval == 3600


class TestDeribitMaxPain:
    @patch("signal_noise.collector.deribit_options.requests.get")
    def test_fetch(self, mock_get):
        def side_effect(url, **kwargs):
            if "get_instruments" in url:
                return _mock_response(INSTRUMENTS_RESPONSE)
            if "get_book_summary" in url:
                return _mock_response(BOOK_SUMMARY_RESPONSE)
            return _mock_response({"result": []})

        mock_get.side_effect = side_effect
        df = DeribitBTCMaxPainCollector().fetch()
        assert len(df) == 1
        assert 80000 <= df["value"].iloc[0] <= 100000

    @patch("signal_noise.collector.deribit_options.requests.get")
    def test_empty_response(self, mock_get):
        mock_get.return_value = _mock_response({"result": []})
        with pytest.raises(RuntimeError):
            DeribitBTCMaxPainCollector().fetch()

    def test_meta(self):
        m = DeribitBTCMaxPainCollector.meta
        assert m.name == "max_pain_btc"
        assert m.interval == 3600


class TestDeribitGEX:
    @patch("signal_noise.collector.deribit_options.requests.get")
    def test_fetch(self, mock_get):
        def side_effect(url, **kwargs):
            if "get_book_summary" in url:
                return _mock_response(BOOK_SUMMARY_RESPONSE)
            if "get_instruments" in url:
                return _mock_response(INSTRUMENTS_RESPONSE)
            if "get_index_price" in url:
                return _mock_response(INDEX_PRICE_RESPONSE)
            return _mock_response({"result": []})

        mock_get.side_effect = side_effect
        df = DeribitBTCGEXCollector().fetch()
        assert len(df) == 1
        assert isinstance(df["value"].iloc[0], float)

    @patch("signal_noise.collector.deribit_options.requests.get")
    def test_empty_response(self, mock_get):
        def side_effect(url, **kwargs):
            if "get_index_price" in url:
                return _mock_response(INDEX_PRICE_RESPONSE)
            return _mock_response({"result": []})

        mock_get.side_effect = side_effect
        with pytest.raises(RuntimeError):
            DeribitBTCGEXCollector().fetch()

    def test_meta(self):
        m = DeribitBTCGEXCollector.meta
        assert m.name == "gamma_exposure_btc"
        assert m.interval == 3600


# ── Factory / Registration tests ──

class TestFactory:
    def test_produces_12_collectors(self):
        collectors = get_deribit_options_collectors()
        assert len(collectors) == 12

    def test_btc_names(self):
        collectors = get_deribit_options_collectors()
        btc = [n for n in collectors if "btc" in n]
        assert len(btc) == 6

    def test_eth_names(self):
        collectors = get_deribit_options_collectors()
        eth = [n for n in collectors if "eth" in n]
        assert len(eth) == 6

    def test_all_registered(self):
        from signal_noise.collector import COLLECTORS

        expected = [
            "iv_atm_btc_30d", "iv_atm_btc_7d", "iv_skew_btc_7d",
            "put_call_ratio_btc", "max_pain_btc", "gamma_exposure_btc",
            "iv_atm_eth_30d", "iv_atm_eth_7d", "iv_skew_eth_7d",
            "put_call_ratio_eth", "max_pain_eth", "gamma_exposure_eth",
        ]
        for name in expected:
            assert name in COLLECTORS, f"{name} not registered"

    def test_taxonomy_valid(self):
        collectors = get_deribit_options_collectors()
        for name, cls in collectors.items():
            assert cls.meta.domain in DOMAINS, f"{name}: invalid domain"
            assert cls.meta.category in CATEGORIES, f"{name}: invalid category"
            assert cls.meta.interval == 3600, f"{name}: wrong interval"
