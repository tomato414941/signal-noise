from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import pandas as pd

from signal_noise.collector._cache import SharedAPICache
from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector.deribit_options import (
    _compute_max_pain,
    _fetch_book_summary,
    _fetch_dvol,
    _fetch_index_price,
    _fetch_instruments,
    _fetch_ticker,
    _find_25delta_instruments,
    _find_atm_instrument,
    _find_target_expiry,
)

_crypto_alpha_cache = SharedAPICache(ttl=120)


@dataclass(frozen=True)
class _CryptoFactorSpec:
    name: str
    display_name: str
    compute: Callable[[], float]


def _compute_deribit_state(currency: str) -> dict[str, float | pd.Timestamp]:
    cache_key = f"state:{currency}"

    def _fetch() -> dict[str, float | pd.Timestamp]:
        instruments = _fetch_instruments(currency)
        book = _fetch_book_summary(currency)
        spot = _fetch_index_price(currency)
        dvol = _fetch_dvol(currency)
        if not instruments or not book or not dvol:
            raise RuntimeError(f"No Deribit data for {currency}")

        iv30 = float(dvol[-1][4])
        target_exp = _find_target_expiry(instruments, target_days=7)
        if not target_exp:
            raise RuntimeError(f"No suitable 7d expiry for {currency}")

        atm_name = _find_atm_instrument(book, instruments, target_exp, spot)
        if not atm_name:
            raise RuntimeError(f"No ATM instrument for {currency} 7d expiry")

        iv7: float | None = None
        for entry in book:
            if entry["instrument_name"] == atm_name:
                iv7 = float(entry["mark_iv"])
                break
        if iv7 is None:
            raise RuntimeError(f"No ATM IV for {currency}")

        put_name, call_name = _find_25delta_instruments(
            instruments,
            book,
            target_exp,
            spot,
            iv7 / 100.0,
        )
        if not put_name or not call_name:
            raise RuntimeError(f"Could not find 25-delta instruments for {currency}")

        put_ticker = _fetch_ticker(put_name)
        call_ticker = _fetch_ticker(call_name)
        put_iv = float(put_ticker.get("mark_iv", 0) or 0)
        call_iv = float(call_ticker.get("mark_iv", 0) or 0)
        if put_iv <= 0 or call_iv <= 0:
            raise RuntimeError(f"No skew ticker IV for {currency}")
        skew7 = put_iv - call_iv

        put_vol = 0.0
        call_vol = 0.0
        for entry in book:
            name = entry.get("instrument_name", "")
            volume = float(entry.get("volume", 0) or 0)
            if name.endswith("-P"):
                put_vol += volume
            elif name.endswith("-C"):
                call_vol += volume
        if call_vol <= 0:
            raise RuntimeError(f"No call volume for {currency}")
        pcr = put_vol / call_vol

        max_pain = _compute_max_pain(book, instruments, target_exp)
        if max_pain is None:
            raise RuntimeError(f"Could not compute max pain for {currency}")

        return {
            "timestamp": pd.Timestamp.now(tz="UTC").floor("h"),
            "spot": float(spot),
            "iv30": iv30,
            "iv7": iv7,
            "skew7": skew7,
            "pcr": pcr,
            "max_pain": float(max_pain),
        }

    return _crypto_alpha_cache.get_or_fetch(cache_key, _fetch)


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator == 0:
        raise RuntimeError("Division by zero in crypto alpha factor")
    return numerator / denominator


def _btc_iv_term() -> float:
    state = _compute_deribit_state("BTC")
    return float(state["iv7"]) - float(state["iv30"])


def _eth_iv_term() -> float:
    state = _compute_deribit_state("ETH")
    return float(state["iv7"]) - float(state["iv30"])


def _btc_skew_pcr() -> float:
    state = _compute_deribit_state("BTC")
    return float(state["skew7"]) * float(state["pcr"])


def _eth_skew_pcr() -> float:
    state = _compute_deribit_state("ETH")
    return float(state["skew7"]) * float(state["pcr"])


def _btc_max_pain_gap_bps() -> float:
    state = _compute_deribit_state("BTC")
    spot = float(state["spot"])
    return (spot - float(state["max_pain"])) / spot * 10_000


def _eth_max_pain_gap_bps() -> float:
    state = _compute_deribit_state("ETH")
    spot = float(state["spot"])
    return (spot - float(state["max_pain"])) / spot * 10_000


def _eth_btc_iv30_ratio() -> float:
    eth = _compute_deribit_state("ETH")
    btc = _compute_deribit_state("BTC")
    return _safe_ratio(float(eth["iv30"]), float(btc["iv30"]))


def _eth_btc_iv7_ratio() -> float:
    eth = _compute_deribit_state("ETH")
    btc = _compute_deribit_state("BTC")
    return _safe_ratio(float(eth["iv7"]), float(btc["iv7"]))


def _eth_btc_skew_spread() -> float:
    eth = _compute_deribit_state("ETH")
    btc = _compute_deribit_state("BTC")
    return float(eth["skew7"]) - float(btc["skew7"])


def _eth_btc_pcr_ratio() -> float:
    eth = _compute_deribit_state("ETH")
    btc = _compute_deribit_state("BTC")
    return _safe_ratio(float(eth["pcr"]), float(btc["pcr"]))


_FACTOR_SPECS: list[_CryptoFactorSpec] = [
    _CryptoFactorSpec("btc_iv_term_7d_30d", "BTC IV Term Structure (7d - 30d)", _btc_iv_term),
    _CryptoFactorSpec("eth_iv_term_7d_30d", "ETH IV Term Structure (7d - 30d)", _eth_iv_term),
    _CryptoFactorSpec("btc_skew_pcr", "BTC Skew x Put/Call Ratio", _btc_skew_pcr),
    _CryptoFactorSpec("eth_skew_pcr", "ETH Skew x Put/Call Ratio", _eth_skew_pcr),
    _CryptoFactorSpec("btc_max_pain_gap_bps", "BTC Max Pain Gap (bps)", _btc_max_pain_gap_bps),
    _CryptoFactorSpec("eth_max_pain_gap_bps", "ETH Max Pain Gap (bps)", _eth_max_pain_gap_bps),
    _CryptoFactorSpec("eth_btc_iv30_ratio", "ETH / BTC DVOL Ratio", _eth_btc_iv30_ratio),
    _CryptoFactorSpec("eth_btc_iv7_ratio", "ETH / BTC 7d ATM IV Ratio", _eth_btc_iv7_ratio),
    _CryptoFactorSpec("eth_btc_skew_spread", "ETH - BTC 7d Skew Spread", _eth_btc_skew_spread),
    _CryptoFactorSpec("eth_btc_pcr_ratio", "ETH / BTC Put-Call Ratio", _eth_btc_pcr_ratio),
]


def _make_crypto_alpha_factor_collector(spec: _CryptoFactorSpec) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=spec.name,
            display_name=spec.display_name,
            update_frequency="hourly",
            api_docs_url="https://docs.deribit.com/",
            domain="markets",
            category="crypto_derivatives",
            collect_interval=3600,
        )

        def fetch(self) -> pd.DataFrame:
            value = spec.compute()
            ts = pd.Timestamp.now(tz="UTC").floor("h")
            return pd.DataFrame([{"timestamp": ts, "value": float(value)}])

    _Collector.__name__ = f"CryptoAlphaFactor_{spec.name}"
    _Collector.__qualname__ = _Collector.__name__
    return _Collector


def get_crypto_alpha_factor_collectors() -> dict[str, type[BaseCollector]]:
    return {spec.name: _make_crypto_alpha_factor_collector(spec) for spec in _FACTOR_SPECS}
