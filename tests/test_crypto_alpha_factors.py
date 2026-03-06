from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

from signal_noise.collector.base import CATEGORIES, DOMAINS
from signal_noise.collector.crypto_alpha_factors import (
    _FACTOR_SPECS,
    _compute_deribit_state,
    _crypto_alpha_cache,
    _make_crypto_alpha_factor_collector,
    get_crypto_alpha_factor_collectors,
)


@pytest.fixture(autouse=True)
def _clear_cache():
    _crypto_alpha_cache.clear()
    yield
    _crypto_alpha_cache.clear()


def _state(
    *,
    iv30: float,
    iv7: float,
    skew7: float,
    pcr: float,
    spot: float,
    max_pain: float,
) -> dict[str, float | pd.Timestamp]:
    return {
        "timestamp": pd.Timestamp("2026-03-06T16:00:00Z"),
        "spot": spot,
        "iv30": iv30,
        "iv7": iv7,
        "skew7": skew7,
        "pcr": pcr,
        "max_pain": max_pain,
    }


class TestCryptoAlphaSpecs:
    def test_spec_count(self):
        assert len(_FACTOR_SPECS) == 10

    def test_no_duplicate_names(self):
        names = [spec.name for spec in _FACTOR_SPECS]
        assert len(names) == len(set(names))

    def test_domain_category_valid(self):
        collectors = get_crypto_alpha_factor_collectors()
        for cls in collectors.values():
            assert cls.meta.domain in DOMAINS
            assert cls.meta.category in CATEGORIES


class TestHelpers:
    @patch("signal_noise.collector.crypto_alpha_factors._fetch_instruments")
    @patch("signal_noise.collector.crypto_alpha_factors._fetch_book_summary")
    @patch("signal_noise.collector.crypto_alpha_factors._fetch_index_price")
    @patch("signal_noise.collector.crypto_alpha_factors._fetch_dvol")
    @patch("signal_noise.collector.crypto_alpha_factors._find_target_expiry")
    @patch("signal_noise.collector.crypto_alpha_factors._find_atm_instrument")
    @patch("signal_noise.collector.crypto_alpha_factors._find_25delta_instruments")
    @patch("signal_noise.collector.crypto_alpha_factors._fetch_ticker")
    @patch("signal_noise.collector.crypto_alpha_factors._compute_max_pain")
    def test_compute_deribit_state_uses_cache(
        self,
        mock_max_pain,
        mock_fetch_ticker,
        mock_find_25,
        mock_find_atm,
        mock_target_exp,
        mock_dvol,
        mock_index,
        mock_book,
        mock_instruments,
    ):
        mock_instruments.return_value = [{"instrument_name": "BTC-7MAR26-90000-C"}]
        mock_book.return_value = [
            {"instrument_name": "BTC-7MAR26-90000-C", "mark_iv": 55.0, "volume": 20.0},
            {"instrument_name": "BTC-7MAR26-90000-P", "mark_iv": 60.0, "volume": 25.0},
        ]
        mock_index.return_value = 90000.0
        mock_dvol.return_value = [[1, 0, 0, 0, 54.7]]
        mock_target_exp.return_value = "7MAR26"
        mock_find_atm.return_value = "BTC-7MAR26-90000-C"
        mock_find_25.return_value = ("BTC-7MAR26-90000-P", "BTC-7MAR26-90000-C")
        mock_fetch_ticker.side_effect = [{"mark_iv": 60.0}, {"mark_iv": 55.0}]
        mock_max_pain.return_value = 88000.0

        first = _compute_deribit_state("BTC")
        second = _compute_deribit_state("BTC")

        assert first == second
        assert mock_instruments.call_count == 1
        assert first["iv30"] == 54.7
        assert first["iv7"] == 55.0
        assert first["skew7"] == 5.0
        assert first["pcr"] == pytest.approx(1.25)


class TestCryptoAlphaCollectors:
    @patch("signal_noise.collector.crypto_alpha_factors._compute_deribit_state")
    def test_single_currency_factor(self, mock_state):
        mock_state.return_value = _state(
            iv30=55.0,
            iv7=60.0,
            skew7=4.0,
            pcr=1.5,
            spot=90000.0,
            max_pain=87000.0,
        )

        spec = next(spec for spec in _FACTOR_SPECS if spec.name == "btc_iv_term_7d_30d")
        df = _make_crypto_alpha_factor_collector(spec)().fetch()

        assert len(df) == 1
        assert df["value"].iloc[0] == pytest.approx(5.0)

    @patch("signal_noise.collector.crypto_alpha_factors._compute_deribit_state")
    def test_cross_currency_factor(self, mock_state):
        def _side_effect(currency: str):
            if currency == "BTC":
                return _state(iv30=55.0, iv7=60.0, skew7=4.0, pcr=1.5, spot=90000.0, max_pain=87000.0)
            return _state(iv30=77.0, iv7=80.0, skew7=7.0, pcr=2.0, spot=3500.0, max_pain=3400.0)

        mock_state.side_effect = _side_effect

        spec = next(spec for spec in _FACTOR_SPECS if spec.name == "eth_btc_iv30_ratio")
        df = _make_crypto_alpha_factor_collector(spec)().fetch()

        assert len(df) == 1
        assert df["value"].iloc[0] == pytest.approx(77.0 / 55.0)

    @patch("signal_noise.collector.crypto_alpha_factors._compute_deribit_state")
    def test_max_pain_gap_bps(self, mock_state):
        mock_state.return_value = _state(
            iv30=55.0,
            iv7=60.0,
            skew7=4.0,
            pcr=1.5,
            spot=90000.0,
            max_pain=87300.0,
        )

        spec = next(spec for spec in _FACTOR_SPECS if spec.name == "btc_max_pain_gap_bps")
        df = _make_crypto_alpha_factor_collector(spec)().fetch()

        assert df["value"].iloc[0] == pytest.approx((90000.0 - 87300.0) / 90000.0 * 10000)

    def test_get_collectors_returns_all(self):
        collectors = get_crypto_alpha_factor_collectors()
        assert len(collectors) == len(_FACTOR_SPECS)
        assert "btc_iv_term_7d_30d" in collectors
        assert "eth_btc_pcr_ratio" in collectors

    def test_registration(self):
        from signal_noise.collector import COLLECTORS

        expected = [
            "btc_iv_term_7d_30d",
            "eth_iv_term_7d_30d",
            "btc_skew_pcr",
            "eth_btc_iv30_ratio",
            "eth_btc_pcr_ratio",
        ]
        for name in expected:
            assert name in COLLECTORS
