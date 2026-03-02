"""Deribit options collectors — derivative signals for L2 tactical trading.

BTC signals (6):
  iv_atm_btc_30d, iv_atm_btc_7d, iv_skew_btc_7d,
  put_call_ratio_btc, max_pain_btc, gamma_exposure_btc

ETH signals (6):
  iv_atm_eth_30d, iv_atm_eth_7d, iv_skew_eth_7d,
  put_call_ratio_eth, max_pain_eth, gamma_exposure_eth

All endpoints are public (no authentication required).
"""
from __future__ import annotations

import logging
import math
import re
from datetime import datetime, timezone

import pandas as pd
import requests

from signal_noise.collector._cache import SharedAPICache
from signal_noise.collector.base import BaseCollector, CollectorMeta

log = logging.getLogger(__name__)

_BASE = "https://deribit.com/api/v2/public"
_deribit_cache = SharedAPICache(ttl=120)

_MONTHS = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}


# ── Shared data fetchers (currency-parameterized) ──

def _fetch_book_summary(currency: str) -> list[dict]:
    def _fetch() -> list[dict]:
        r = requests.get(
            f"{_BASE}/get_book_summary_by_currency",
            params={"currency": currency, "kind": "option"},
            timeout=15,
        )
        r.raise_for_status()
        return r.json().get("result", [])
    return _deribit_cache.get_or_fetch(f"book_summary_{currency}", _fetch)


def _fetch_instruments(currency: str) -> list[dict]:
    def _fetch() -> list[dict]:
        r = requests.get(
            f"{_BASE}/get_instruments",
            params={"currency": currency, "kind": "option", "expired": "false"},
            timeout=15,
        )
        r.raise_for_status()
        return r.json().get("result", [])
    return _deribit_cache.get_or_fetch(f"instruments_{currency}", _fetch)


def _fetch_index_price(currency: str) -> float:
    index_name = f"{currency.lower()}_usd"
    def _fetch() -> float:
        r = requests.get(
            f"{_BASE}/get_index_price",
            params={"index_name": index_name},
            timeout=10,
        )
        r.raise_for_status()
        return float(r.json()["result"]["index_price"])
    return _deribit_cache.get_or_fetch(f"index_price_{currency}", _fetch)


def _fetch_dvol(currency: str) -> list[dict]:
    def _fetch() -> list[dict]:
        r = requests.get(
            f"{_BASE}/get_volatility_index_data",
            params={"currency": currency, "resolution": "3600"},
            timeout=15,
        )
        r.raise_for_status()
        return r.json().get("result", {}).get("data", [])
    return _deribit_cache.get_or_fetch(f"dvol_{currency}", _fetch)


def _fetch_ticker(instrument_name: str) -> dict:
    def _fetch() -> dict:
        r = requests.get(
            f"{_BASE}/ticker",
            params={"instrument_name": instrument_name},
            timeout=10,
        )
        r.raise_for_status()
        return r.json().get("result", {})
    return _deribit_cache.get_or_fetch(f"ticker_{instrument_name}", _fetch)


# ── Helper functions ──

_INSTRUMENT_RE = re.compile(
    r"^(BTC|ETH)-(\d{1,2})([A-Z]{3})(\d{2,4})-(\d+)-(C|P)$"
)


def _parse_instrument_name(name: str) -> dict | None:
    m = _INSTRUMENT_RE.match(name)
    if not m:
        return None
    currency, day, mon_str, year_str, strike_str, opt_type = m.groups()
    return {
        "currency": currency,
        "expiry_str": f"{day}{mon_str}{year_str}",
        "strike": float(strike_str),
        "option_type": opt_type,
    }


def _expiry_to_timestamp(expiry_str: str) -> pd.Timestamp:
    day = int(re.match(r"(\d+)", expiry_str).group(1))
    mon_str = re.search(r"([A-Z]{3})", expiry_str).group(1)
    year_str = re.search(r"[A-Z]{3}(\d+)", expiry_str).group(1)
    year = int(year_str)
    if year < 100:
        year += 2000
    month = _MONTHS[mon_str]
    return pd.Timestamp(datetime(year, month, day, 8, 0, tzinfo=timezone.utc))


def _find_target_expiry(instruments: list[dict], target_days: int = 7) -> str | None:
    now = pd.Timestamp.now(tz="UTC")
    best_name = None
    best_diff = float("inf")
    seen_expiries: set[str] = set()
    for inst in instruments:
        parsed = _parse_instrument_name(inst["instrument_name"])
        if not parsed:
            continue
        exp_str = parsed["expiry_str"]
        if exp_str in seen_expiries:
            continue
        seen_expiries.add(exp_str)
        exp_ts = _expiry_to_timestamp(exp_str)
        days_to_expiry = (exp_ts - now).total_seconds() / 86400
        if days_to_expiry < 1:
            continue
        diff = abs(days_to_expiry - target_days)
        if diff < best_diff:
            best_diff = diff
            best_name = exp_str
    return best_name


def _find_atm_instrument(
    book: list[dict], instruments: list[dict], target_expiry: str, spot: float,
) -> str | None:
    best_name = None
    best_diff = float("inf")
    book_names = {b["instrument_name"] for b in book if b.get("mark_iv") and b["mark_iv"] > 0}
    for inst in instruments:
        name = inst["instrument_name"]
        if name not in book_names:
            continue
        parsed = _parse_instrument_name(name)
        if not parsed or parsed["expiry_str"] != target_expiry or parsed["option_type"] != "C":
            continue
        diff = abs(parsed["strike"] - spot)
        if diff < best_diff:
            best_diff = diff
            best_name = name
    return best_name


def _bs_delta(spot: float, strike: float, t: float, iv: float, is_call: bool) -> float:
    if t <= 0 or iv <= 0 or spot <= 0 or strike <= 0:
        return 0.0
    sigma_sqrt_t = iv * math.sqrt(t)
    d1 = (math.log(spot / strike) + 0.5 * iv * iv * t) / sigma_sqrt_t
    nd1 = 0.5 * (1 + math.erf(d1 / math.sqrt(2)))
    return nd1 if is_call else nd1 - 1.0


def _find_25delta_instruments(
    instruments: list[dict],
    book: list[dict],
    target_expiry: str,
    spot: float,
    atm_iv: float,
) -> tuple[str | None, str | None]:
    book_map = {}
    for b in book:
        if b.get("mark_iv") and b["mark_iv"] > 0:
            book_map[b["instrument_name"]] = b["mark_iv"] / 100.0

    exp_ts = _expiry_to_timestamp(target_expiry)
    now = pd.Timestamp.now(tz="UTC")
    t = max((exp_ts - now).total_seconds() / (365.25 * 86400), 1e-6)

    best_put: tuple[str | None, float] = (None, float("inf"))
    best_call: tuple[str | None, float] = (None, float("inf"))

    for inst in instruments:
        name = inst["instrument_name"]
        parsed = _parse_instrument_name(name)
        if not parsed or parsed["expiry_str"] != target_expiry:
            continue
        if name not in book_map:
            continue
        iv = book_map[name]
        strike = parsed["strike"]
        is_call = parsed["option_type"] == "C"
        delta = _bs_delta(spot, strike, t, iv, is_call)

        if is_call:
            diff = abs(delta - 0.25)
            if diff < best_call[1]:
                best_call = (name, diff)
        else:
            diff = abs(delta - (-0.25))
            if diff < best_put[1]:
                best_put = (name, diff)

    return best_put[0], best_call[0]


def _compute_max_pain(book: list[dict], instruments: list[dict], target_expiry: str) -> float | None:
    oi_data: list[tuple[float, str, float]] = []
    for b in book:
        parsed = _parse_instrument_name(b["instrument_name"])
        if not parsed or parsed["expiry_str"] != target_expiry:
            continue
        oi = b.get("open_interest", 0)
        if not oi or oi <= 0:
            continue
        oi_data.append((parsed["strike"], parsed["option_type"], float(oi)))

    if not oi_data:
        return None

    strikes = sorted({d[0] for d in oi_data})
    min_pain = float("inf")
    max_pain_strike = strikes[0]

    for test_strike in strikes:
        total_pain = 0.0
        for strike, opt_type, oi in oi_data:
            if opt_type == "C":
                intrinsic = max(test_strike - strike, 0)
            else:
                intrinsic = max(strike - test_strike, 0)
            total_pain += intrinsic * oi
        if total_pain < min_pain:
            min_pain = total_pain
            max_pain_strike = test_strike

    return max_pain_strike


def _bs_gamma(spot: float, strike: float, t: float, iv: float) -> float:
    if t <= 0 or iv <= 0 or spot <= 0 or strike <= 0:
        return 0.0
    sigma_sqrt_t = iv * math.sqrt(t)
    d1 = (math.log(spot / strike) + 0.5 * iv * iv * t) / sigma_sqrt_t
    pdf_d1 = math.exp(-0.5 * d1 * d1) / math.sqrt(2 * math.pi)
    return pdf_d1 / (spot * sigma_sqrt_t)


def _compute_gex(book: list[dict], instruments: list[dict], spot: float, top_n: int = 30) -> float:
    now = pd.Timestamp.now(tz="UTC")
    items: list[tuple[float, float, str, float]] = []

    book_map = {}
    for b in book:
        if b.get("mark_iv") and b["mark_iv"] > 0 and b.get("open_interest", 0) > 0:
            book_map[b["instrument_name"]] = b

    for name, b in book_map.items():
        parsed = _parse_instrument_name(name)
        if not parsed:
            continue
        exp_ts = _expiry_to_timestamp(parsed["expiry_str"])
        t = (exp_ts - now).total_seconds() / (365.25 * 86400)
        if t <= 0:
            continue
        items.append((float(b["open_interest"]), parsed["strike"], parsed["option_type"], b["mark_iv"] / 100.0))

    items.sort(key=lambda x: x[0], reverse=True)
    items = items[:top_n]

    gex = 0.0
    for oi, strike, opt_type, iv in items:
        gamma = _bs_gamma(spot, strike, 7 / 365.25, iv)
        contract_gex = gamma * oi * spot * spot * 0.01
        if opt_type == "C":
            gex += contract_gex
        else:
            gex -= contract_gex
    return gex


# ── Factory: generate BTC + ETH collector classes ──

_CURRENCIES = ["BTC", "ETH"]


def _make_dvol_collector(currency: str) -> type[BaseCollector]:
    lower = currency.lower()

    class _DVOLCollector(BaseCollector):
        meta = CollectorMeta(
            name=f"iv_atm_{lower}_30d",
            display_name=f"Deribit {currency} DVOL (30d ATM IV)",
            update_frequency="hourly",
            api_docs_url="https://docs.deribit.com/",
            domain="financial",
            category="crypto_derivatives",
            collect_interval=3600,
        )

        def fetch(self) -> pd.DataFrame:
            data = _fetch_dvol(currency)
            if not data:
                raise RuntimeError(f"No DVOL data from Deribit for {currency}")
            latest = data[-1]
            ts = pd.Timestamp(latest[0], unit="ms", tz="UTC")
            value = float(latest[4])
            return pd.DataFrame([{"timestamp": ts, "value": value}])

    _DVOLCollector.__name__ = f"Deribit{currency}DVOLCollector"
    _DVOLCollector.__qualname__ = _DVOLCollector.__name__
    return _DVOLCollector


def _make_atm7d_collector(currency: str) -> type[BaseCollector]:
    lower = currency.lower()

    class _ATM7dCollector(BaseCollector):
        meta = CollectorMeta(
            name=f"iv_atm_{lower}_7d",
            display_name=f"Deribit {currency} 7d ATM IV",
            update_frequency="hourly",
            api_docs_url="https://docs.deribit.com/",
            domain="financial",
            category="crypto_derivatives",
            collect_interval=3600,
        )

        def fetch(self) -> pd.DataFrame:
            instruments = _fetch_instruments(currency)
            book = _fetch_book_summary(currency)
            spot = _fetch_index_price(currency)
            if not instruments or not book:
                raise RuntimeError(f"No Deribit data for {currency}")
            target_exp = _find_target_expiry(instruments, target_days=7)
            if not target_exp:
                raise RuntimeError(f"No suitable 7d expiry for {currency}")
            atm_name = _find_atm_instrument(book, instruments, target_exp, spot)
            if not atm_name:
                raise RuntimeError(f"No ATM instrument for {currency} 7d expiry")
            for b in book:
                if b["instrument_name"] == atm_name:
                    iv = float(b["mark_iv"])
                    now = pd.Timestamp.now(tz="UTC").floor("h")
                    return pd.DataFrame([{"timestamp": now, "value": iv}])
            raise RuntimeError("ATM instrument not found in book summary")

    _ATM7dCollector.__name__ = f"Deribit{currency}ATM7dCollector"
    _ATM7dCollector.__qualname__ = _ATM7dCollector.__name__
    return _ATM7dCollector


def _make_skew7d_collector(currency: str) -> type[BaseCollector]:
    lower = currency.lower()

    class _Skew7dCollector(BaseCollector):
        meta = CollectorMeta(
            name=f"iv_skew_{lower}_7d",
            display_name=f"Deribit {currency} 7d 25-delta Skew",
            update_frequency="hourly",
            api_docs_url="https://docs.deribit.com/",
            domain="financial",
            category="crypto_derivatives",
            collect_interval=3600,
        )

        def fetch(self) -> pd.DataFrame:
            instruments = _fetch_instruments(currency)
            book = _fetch_book_summary(currency)
            spot = _fetch_index_price(currency)
            if not instruments or not book:
                raise RuntimeError(f"No Deribit data for {currency}")
            target_exp = _find_target_expiry(instruments, target_days=7)
            if not target_exp:
                raise RuntimeError(f"No suitable 7d expiry for {currency}")
            atm_name = _find_atm_instrument(book, instruments, target_exp, spot)
            atm_iv = 0.5
            if atm_name:
                for b in book:
                    if b["instrument_name"] == atm_name:
                        atm_iv = b["mark_iv"] / 100.0
                        break
            put_name, call_name = _find_25delta_instruments(
                instruments, book, target_exp, spot, atm_iv,
            )
            if not put_name or not call_name:
                raise RuntimeError(f"Could not find 25-delta instruments for {currency}")
            put_ticker = _fetch_ticker(put_name)
            call_ticker = _fetch_ticker(call_name)
            put_iv = put_ticker.get("mark_iv", 0)
            call_iv = call_ticker.get("mark_iv", 0)
            if not put_iv or not call_iv:
                raise RuntimeError("No IV in ticker data")
            skew = float(put_iv) - float(call_iv)
            now = pd.Timestamp.now(tz="UTC").floor("h")
            return pd.DataFrame([{"timestamp": now, "value": skew}])

    _Skew7dCollector.__name__ = f"Deribit{currency}Skew7dCollector"
    _Skew7dCollector.__qualname__ = _Skew7dCollector.__name__
    return _Skew7dCollector


def _make_pcr_collector(currency: str) -> type[BaseCollector]:
    lower = currency.lower()

    class _PCRCollector(BaseCollector):
        meta = CollectorMeta(
            name=f"put_call_ratio_{lower}",
            display_name=f"Deribit {currency} Put/Call Volume Ratio",
            update_frequency="hourly",
            api_docs_url="https://docs.deribit.com/",
            domain="financial",
            category="crypto_derivatives",
            collect_interval=3600,
        )

        def fetch(self) -> pd.DataFrame:
            book = _fetch_book_summary(currency)
            if not book:
                raise RuntimeError(f"No Deribit book summary for {currency}")
            put_vol = 0.0
            call_vol = 0.0
            for b in book:
                name = b.get("instrument_name", "")
                vol = b.get("volume", 0) or 0
                if name.endswith("-P"):
                    put_vol += float(vol)
                elif name.endswith("-C"):
                    call_vol += float(vol)
            if call_vol == 0:
                raise RuntimeError(f"No call volume for {currency}")
            ratio = put_vol / call_vol
            now = pd.Timestamp.now(tz="UTC").floor("h")
            return pd.DataFrame([{"timestamp": now, "value": ratio}])

    _PCRCollector.__name__ = f"Deribit{currency}PCRCollector"
    _PCRCollector.__qualname__ = _PCRCollector.__name__
    return _PCRCollector


def _make_maxpain_collector(currency: str) -> type[BaseCollector]:
    lower = currency.lower()

    class _MaxPainCollector(BaseCollector):
        meta = CollectorMeta(
            name=f"max_pain_{lower}",
            display_name=f"Deribit {currency} Max Pain",
            update_frequency="hourly",
            api_docs_url="https://docs.deribit.com/",
            domain="financial",
            category="crypto_derivatives",
            collect_interval=3600,
        )

        def fetch(self) -> pd.DataFrame:
            instruments = _fetch_instruments(currency)
            book = _fetch_book_summary(currency)
            if not instruments or not book:
                raise RuntimeError(f"No Deribit data for {currency}")
            target_exp = _find_target_expiry(instruments, target_days=7)
            if not target_exp:
                raise RuntimeError(f"No suitable expiry for {currency}")
            max_pain = _compute_max_pain(book, instruments, target_exp)
            if max_pain is None:
                raise RuntimeError(f"Could not compute max pain for {currency}")
            now = pd.Timestamp.now(tz="UTC").floor("h")
            return pd.DataFrame([{"timestamp": now, "value": max_pain}])

    _MaxPainCollector.__name__ = f"Deribit{currency}MaxPainCollector"
    _MaxPainCollector.__qualname__ = _MaxPainCollector.__name__
    return _MaxPainCollector


def _make_gex_collector(currency: str) -> type[BaseCollector]:
    lower = currency.lower()

    class _GEXCollector(BaseCollector):
        meta = CollectorMeta(
            name=f"gamma_exposure_{lower}",
            display_name=f"Deribit {currency} Gamma Exposure",
            update_frequency="hourly",
            api_docs_url="https://docs.deribit.com/",
            domain="financial",
            category="crypto_derivatives",
            collect_interval=3600,
        )

        def fetch(self) -> pd.DataFrame:
            book = _fetch_book_summary(currency)
            instruments = _fetch_instruments(currency)
            spot = _fetch_index_price(currency)
            if not book or not instruments:
                raise RuntimeError(f"No Deribit data for {currency}")
            gex = _compute_gex(book, instruments, spot)
            now = pd.Timestamp.now(tz="UTC").floor("h")
            return pd.DataFrame([{"timestamp": now, "value": gex}])

    _GEXCollector.__name__ = f"Deribit{currency}GEXCollector"
    _GEXCollector.__qualname__ = _GEXCollector.__name__
    return _GEXCollector


# ── Register all collectors via factory function ──

_FACTORIES = [
    _make_dvol_collector,
    _make_atm7d_collector,
    _make_skew7d_collector,
    _make_pcr_collector,
    _make_maxpain_collector,
    _make_gex_collector,
]


def get_deribit_options_collectors() -> dict[str, type[BaseCollector]]:
    """Auto-discovered factory: generate BTC + ETH options collectors."""
    result = {}
    for currency in _CURRENCIES:
        for factory in _FACTORIES:
            cls = factory(currency)
            result[cls.meta.name] = cls
    return result
