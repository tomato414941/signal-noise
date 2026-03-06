from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
import yfinance as yf

from signal_noise.collector._cache import SharedAPICache
from signal_noise.collector.base import BaseCollector, CollectorMeta

_alpha_cache = SharedAPICache(ttl=1800)


@dataclass(frozen=True)
class _FactorSpec:
    left: str
    right: str
    name: str
    display_name: str
    mode: str
    domain: str
    category: str


_FACTOR_SPECS: list[_FactorSpec] = [
    _FactorSpec("^VVIX", "^VIX", "vvix_vix_ratio", "VVIX / VIX Ratio", "ratio", "markets", "equity"),
    _FactorSpec("^SKEW", "^VIX", "skew_vix_spread", "CBOE SKEW - VIX Spread", "spread", "markets", "equity"),
    _FactorSpec("HYG", "LQD", "hyg_lqd_ratio", "HYG / LQD Ratio", "ratio", "markets", "rates"),
    _FactorSpec("TLT", "HYG", "tlt_hyg_ratio", "TLT / HYG Ratio", "ratio", "markets", "rates"),
    _FactorSpec("XLY", "XLP", "xly_xlp_ratio", "XLY / XLP Ratio", "ratio", "markets", "equity"),
    _FactorSpec("XLI", "XLU", "xli_xlu_ratio", "XLI / XLU Ratio", "ratio", "markets", "equity"),
    _FactorSpec("XLF", "XLK", "xlf_xlk_ratio", "XLF / XLK Ratio", "ratio", "markets", "equity"),
    _FactorSpec("EEM", "SPY", "eem_spy_ratio", "EEM / SPY Ratio", "ratio", "markets", "equity"),
    _FactorSpec("HG=F", "GC=F", "copper_gold_ratio", "Copper / Gold Ratio", "ratio", "markets", "commodity"),
    _FactorSpec("BZ=F", "CL=F", "brent_wti_spread", "Brent - WTI Spread", "spread", "markets", "commodity"),
]


def _normalize_close_frame(raw: pd.DataFrame, tickers: tuple[str, ...]) -> pd.DataFrame:
    if raw.empty:
        raise RuntimeError(f"No Yahoo data returned for {', '.join(tickers)}")

    if isinstance(raw.columns, pd.MultiIndex):
        if "Close" not in raw.columns.get_level_values(0):
            raise RuntimeError("Yahoo response missing Close column")
        close = raw["Close"].copy()
    else:
        if "Close" not in raw.columns:
            raise RuntimeError("Yahoo response missing Close column")
        close = raw[["Close"]].copy()
        close.columns = [tickers[0]]

    if isinstance(close, pd.Series):
        close = close.to_frame(name=tickers[0])

    missing = [ticker for ticker in tickers if ticker not in close.columns]
    if missing:
        raise RuntimeError(f"Yahoo response missing tickers: {', '.join(missing)}")

    close = close[list(tickers)].dropna(how="all").sort_index()
    if close.empty:
        raise RuntimeError(f"No Yahoo close history for {', '.join(tickers)}")

    idx = close.index
    close.index = idx.tz_localize("UTC") if idx.tz is None else idx.tz_convert("UTC")
    return close


def _fetch_close_history(tickers: tuple[str, ...], *, period: str = "2y") -> pd.DataFrame:
    cache_key = f"close:{period}:{','.join(tickers)}"

    def _fetch() -> pd.DataFrame:
        raw = yf.download(
            tickers=list(tickers),
            period=period,
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=False,
        )
        return _normalize_close_frame(raw, tickers)

    return _alpha_cache.get_or_fetch(cache_key, _fetch)


def _compute_factor(spec: _FactorSpec) -> pd.DataFrame:
    close = _fetch_close_history((spec.left, spec.right))
    left = close[spec.left].astype(float)
    right = close[spec.right].astype(float)

    if spec.mode == "ratio":
        values = left / right.replace(0.0, pd.NA)
    elif spec.mode == "spread":
        values = left - right
    else:
        raise RuntimeError(f"Unsupported factor mode: {spec.mode}")

    result = (
        pd.DataFrame({"date": values.index, "value": values.values})
        .dropna()
        .sort_values("date")
        .reset_index(drop=True)
    )
    if result.empty:
        raise RuntimeError(f"No derived values for {spec.name}")
    return result


def _make_alpha_factor_collector(spec: _FactorSpec) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=spec.name,
            display_name=spec.display_name,
            update_frequency="daily",
            api_docs_url="https://finance.yahoo.com/",
            domain=spec.domain,
            category=spec.category,
        )

        def fetch(self) -> pd.DataFrame:
            return _compute_factor(spec)

    _Collector.__name__ = f"AlphaFactor_{spec.name}"
    _Collector.__qualname__ = f"AlphaFactor_{spec.name}"
    return _Collector


def get_alpha_factor_collectors() -> dict[str, type[BaseCollector]]:
    return {spec.name: _make_alpha_factor_collector(spec) for spec in _FACTOR_SPECS}
