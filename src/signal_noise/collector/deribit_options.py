"""Deribit BTC options collectors — 6 derivative signals for L2 tactical trading.

Signals:
  iv_atm_btc_30d      — DVOL (30-day ATM IV index)
  iv_atm_btc_7d       — 7-day ATM implied volatility
  iv_skew_btc_7d      — 7-day 25-delta risk reversal (put IV - call IV)
  put_call_ratio_btc  — put/call volume ratio
  max_pain_btc        — nearest expiry max pain strike
  gamma_exposure_btc  — net dealer gamma exposure (BS approximation)

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

# Deribit expiry month codes
_MONTHS = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}
_INSTRUMENT_RE = re.compile(
    r"^BTC-(\d{1,2})([A-Z]{3})(\d{2,4})-(\d+)-(C|P)$"
)


# ── Shared data fetchers ──

def _fetch_book_summary() -> list[dict]:
    def _fetch() -> list[dict]:
        r = requests.get(
            f"{_BASE}/get_book_summary_by_currency",
            params={"currency": "BTC", "kind": "option"},
            timeout=15,
        )
        r.raise_for_status()
        return r.json().get("result", [])
    return _deribit_cache.get_or_fetch("book_summary", _fetch)


def _fetch_instruments() -> list[dict]:
    def _fetch() -> list[dict]:
        r = requests.get(
            f"{_BASE}/get_instruments",
            params={"currency": "BTC", "kind": "option", "expired": "false"},
            timeout=15,
        )
        r.raise_for_status()
        return r.json().get("result", [])
    return _deribit_cache.get_or_fetch("instruments", _fetch)


def _fetch_index_price() -> float:
    def _fetch() -> float:
        r = requests.get(
            f"{_BASE}/get_index_price",
            params={"index_name": "btc_usd"},
            timeout=10,
        )
        r.raise_for_status()
        return float(r.json()["result"]["index_price"])
    return _deribit_cache.get_or_fetch("index_price", _fetch)


def _fetch_dvol() -> list[dict]:
    def _fetch() -> list[dict]:
        r = requests.get(
            f"{_BASE}/get_volatility_index_data",
            params={"currency": "BTC", "resolution": "3600"},
            timeout=15,
        )
        r.raise_for_status()
        return r.json().get("result", {}).get("data", [])
    return _deribit_cache.get_or_fetch("dvol", _fetch)


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

def _parse_instrument_name(name: str) -> dict | None:
    """Parse e.g. 'BTC-28MAR26-90000-C' -> {expiry_str, strike, option_type}."""
    m = _INSTRUMENT_RE.match(name)
    if not m:
        return None
    day, mon_str, year_str, strike_str, opt_type = m.groups()
    return {
        "expiry_str": f"{day}{mon_str}{year_str}",
        "strike": float(strike_str),
        "option_type": opt_type,
    }


def _expiry_to_timestamp(expiry_str: str) -> pd.Timestamp:
    """Convert Deribit expiry string (e.g. '28MAR26') to UTC timestamp at 08:00."""
    # Extract day, month, year
    day = int(re.match(r"(\d+)", expiry_str).group(1))
    mon_str = re.search(r"([A-Z]{3})", expiry_str).group(1)
    year_str = re.search(r"[A-Z]{3}(\d+)", expiry_str).group(1)
    year = int(year_str)
    if year < 100:
        year += 2000
    month = _MONTHS[mon_str]
    return pd.Timestamp(datetime(year, month, day, 8, 0, tzinfo=timezone.utc))


def _find_target_expiry(instruments: list[dict], target_days: int = 7) -> str | None:
    """Find nearest expiry at least 1 day away, closest to target_days."""
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
    """Find ATM call instrument for given expiry (closest strike to spot)."""
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
    """Black-Scholes delta using erf approximation (r=0)."""
    if t <= 0 or iv <= 0 or spot <= 0 or strike <= 0:
        return 0.0
    sigma_sqrt_t = iv * math.sqrt(t)
    d1 = (math.log(spot / strike) + 0.5 * iv * iv * t) / sigma_sqrt_t
    # N(d1) via erf: N(x) = 0.5 * (1 + erf(x / sqrt(2)))
    nd1 = 0.5 * (1 + math.erf(d1 / math.sqrt(2)))
    return nd1 if is_call else nd1 - 1.0


def _find_25delta_instruments(
    instruments: list[dict],
    book: list[dict],
    target_expiry: str,
    spot: float,
    atm_iv: float,
) -> tuple[str | None, str | None]:
    """Find 25-delta put and call instruments for given expiry."""
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
    """Compute max pain strike: the strike minimizing total intrinsic loss for option writers."""
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
    """Black-Scholes gamma (r=0)."""
    if t <= 0 or iv <= 0 or spot <= 0 or strike <= 0:
        return 0.0
    sigma_sqrt_t = iv * math.sqrt(t)
    d1 = (math.log(spot / strike) + 0.5 * iv * iv * t) / sigma_sqrt_t
    pdf_d1 = math.exp(-0.5 * d1 * d1) / math.sqrt(2 * math.pi)
    return pdf_d1 / (spot * sigma_sqrt_t)


def _compute_gex(book: list[dict], instruments: list[dict], spot: float, top_n: int = 30) -> float:
    """Compute net dealer gamma exposure (call +, put -) from top OI options."""
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

    # Sort by OI descending, take top_n
    items.sort(key=lambda x: x[0], reverse=True)
    items = items[:top_n]

    gex = 0.0
    for oi, strike, opt_type, iv in items:
        # Recompute t for each (simplified: use same now)
        gamma = _bs_gamma(spot, strike, 7 / 365.25, iv)  # approximate t
        contract_gex = gamma * oi * spot * spot * 0.01
        if opt_type == "C":
            gex += contract_gex
        else:
            gex -= contract_gex
    return gex


# ── Collector classes ──

class DeribitDVOLCollector(BaseCollector):
    """DVOL — Deribit 30-day ATM implied volatility index."""

    meta = CollectorMeta(
        name="iv_atm_btc_30d",
        display_name="Deribit BTC DVOL (30d ATM IV)",
        update_frequency="hourly",
        api_docs_url="https://docs.deribit.com/",
        domain="financial",
        category="crypto_derivatives",
        collect_interval=3600,
    )

    def fetch(self) -> pd.DataFrame:
        data = _fetch_dvol()
        if not data:
            raise RuntimeError("No DVOL data from Deribit")
        # data is list of [timestamp_ms, open, high, low, close]
        latest = data[-1]
        ts = pd.Timestamp(latest[0], unit="ms", tz="UTC")
        value = float(latest[4])  # close
        return pd.DataFrame([{"timestamp": ts, "value": value}])


class DeribitATM7dCollector(BaseCollector):
    """7-day ATM implied volatility from nearest expiry."""

    meta = CollectorMeta(
        name="iv_atm_btc_7d",
        display_name="Deribit BTC 7d ATM IV",
        update_frequency="hourly",
        api_docs_url="https://docs.deribit.com/",
        domain="financial",
        category="crypto_derivatives",
        collect_interval=3600,
    )

    def fetch(self) -> pd.DataFrame:
        instruments = _fetch_instruments()
        book = _fetch_book_summary()
        spot = _fetch_index_price()
        if not instruments or not book:
            raise RuntimeError("No Deribit instruments/book data")

        target_exp = _find_target_expiry(instruments, target_days=7)
        if not target_exp:
            raise RuntimeError("No suitable 7d expiry found")

        atm_name = _find_atm_instrument(book, instruments, target_exp, spot)
        if not atm_name:
            raise RuntimeError("No ATM instrument found for 7d expiry")

        for b in book:
            if b["instrument_name"] == atm_name:
                iv = float(b["mark_iv"])
                now = pd.Timestamp.now(tz="UTC").floor("h")
                return pd.DataFrame([{"timestamp": now, "value": iv}])

        raise RuntimeError("ATM instrument not found in book summary")


class DeribitSkew7dCollector(BaseCollector):
    """7-day 25-delta risk reversal: put IV - call IV."""

    meta = CollectorMeta(
        name="iv_skew_btc_7d",
        display_name="Deribit BTC 7d 25-delta Skew",
        update_frequency="hourly",
        api_docs_url="https://docs.deribit.com/",
        domain="financial",
        category="crypto_derivatives",
        collect_interval=3600,
    )

    def fetch(self) -> pd.DataFrame:
        instruments = _fetch_instruments()
        book = _fetch_book_summary()
        spot = _fetch_index_price()
        if not instruments or not book:
            raise RuntimeError("No Deribit instruments/book data")

        target_exp = _find_target_expiry(instruments, target_days=7)
        if not target_exp:
            raise RuntimeError("No suitable 7d expiry found")

        # Get ATM IV for delta estimation
        atm_name = _find_atm_instrument(book, instruments, target_exp, spot)
        atm_iv = 0.5  # fallback
        if atm_name:
            for b in book:
                if b["instrument_name"] == atm_name:
                    atm_iv = b["mark_iv"] / 100.0
                    break

        put_name, call_name = _find_25delta_instruments(
            instruments, book, target_exp, spot, atm_iv,
        )
        if not put_name or not call_name:
            raise RuntimeError("Could not find 25-delta instruments")

        put_ticker = _fetch_ticker(put_name)
        call_ticker = _fetch_ticker(call_name)

        put_iv = put_ticker.get("mark_iv", 0)
        call_iv = call_ticker.get("mark_iv", 0)
        if not put_iv or not call_iv:
            raise RuntimeError("No IV in ticker data")

        skew = float(put_iv) - float(call_iv)
        now = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame([{"timestamp": now, "value": skew}])


class DeribitPutCallRatioCollector(BaseCollector):
    """Put/call volume ratio across all BTC options."""

    meta = CollectorMeta(
        name="put_call_ratio_btc",
        display_name="Deribit BTC Put/Call Volume Ratio",
        update_frequency="hourly",
        api_docs_url="https://docs.deribit.com/",
        domain="financial",
        category="crypto_derivatives",
        collect_interval=3600,
    )

    def fetch(self) -> pd.DataFrame:
        book = _fetch_book_summary()
        if not book:
            raise RuntimeError("No Deribit book summary data")

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
            raise RuntimeError("No call volume — market may be closed")

        ratio = put_vol / call_vol
        now = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame([{"timestamp": now, "value": ratio}])


class DeribitMaxPainCollector(BaseCollector):
    """Max pain strike for nearest BTC options expiry."""

    meta = CollectorMeta(
        name="max_pain_btc",
        display_name="Deribit BTC Max Pain",
        update_frequency="hourly",
        api_docs_url="https://docs.deribit.com/",
        domain="financial",
        category="crypto_derivatives",
        collect_interval=3600,
    )

    def fetch(self) -> pd.DataFrame:
        instruments = _fetch_instruments()
        book = _fetch_book_summary()
        if not instruments or not book:
            raise RuntimeError("No Deribit instruments/book data")

        target_exp = _find_target_expiry(instruments, target_days=7)
        if not target_exp:
            raise RuntimeError("No suitable expiry found")

        max_pain = _compute_max_pain(book, instruments, target_exp)
        if max_pain is None:
            raise RuntimeError("Could not compute max pain")

        now = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame([{"timestamp": now, "value": max_pain}])


class DeribitGEXCollector(BaseCollector):
    """Net dealer gamma exposure (BS approximation, top 30 OI options)."""

    meta = CollectorMeta(
        name="gamma_exposure_btc",
        display_name="Deribit BTC Gamma Exposure",
        update_frequency="hourly",
        api_docs_url="https://docs.deribit.com/",
        domain="financial",
        category="crypto_derivatives",
        collect_interval=3600,
    )

    def fetch(self) -> pd.DataFrame:
        book = _fetch_book_summary()
        instruments = _fetch_instruments()
        spot = _fetch_index_price()
        if not book or not instruments:
            raise RuntimeError("No Deribit book/instruments data")

        gex = _compute_gex(book, instruments, spot)
        now = pd.Timestamp.now(tz="UTC").floor("h")
        return pd.DataFrame([{"timestamp": now, "value": gex}])
