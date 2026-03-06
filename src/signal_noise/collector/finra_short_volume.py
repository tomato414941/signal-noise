from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from io import StringIO

import pandas as pd
import requests

from signal_noise.collector._cache import SharedAPICache
from signal_noise.collector._utils import build_timeseries_df
from signal_noise.collector.base import BaseCollector, CollectorMeta

_finra_cache = SharedAPICache(ttl=21_600)
_BASE_URL = "https://cdn.finra.org/equity/regsho/daily/CNMSshvol{:%Y%m%d}.txt"
_LOOKBACK_DAYS = 180
_HEADERS = {
    "User-Agent": "signal-noise/0.1 (research)",
    "Accept": "text/plain",
}


@dataclass(frozen=True)
class _RawSpec:
    symbol: str
    name: str
    display_name: str


@dataclass(frozen=True)
class _SpreadSpec:
    left: str
    right: str
    name: str
    display_name: str


_RAW_SPECS: list[_RawSpec] = [
    _RawSpec("SPY", "finra_spy_short_ratio", "FINRA SPY Short Volume Ratio"),
    _RawSpec("QQQ", "finra_qqq_short_ratio", "FINRA QQQ Short Volume Ratio"),
    _RawSpec("IWM", "finra_iwm_short_ratio", "FINRA IWM Short Volume Ratio"),
    _RawSpec("HYG", "finra_hyg_short_ratio", "FINRA HYG Short Volume Ratio"),
    _RawSpec("XLF", "finra_xlf_short_ratio", "FINRA XLF Short Volume Ratio"),
    _RawSpec("SMH", "finra_smh_short_ratio", "FINRA SMH Short Volume Ratio"),
    _RawSpec("TSLA", "finra_tsla_short_ratio", "FINRA TSLA Short Volume Ratio"),
    _RawSpec("NVDA", "finra_nvda_short_ratio", "FINRA NVDA Short Volume Ratio"),
]

_SPREAD_SPECS: list[_SpreadSpec] = [
    _SpreadSpec("IWM", "SPY", "finra_small_large_short_spread", "FINRA IWM - SPY Short Ratio Spread"),
    _SpreadSpec("QQQ", "SPY", "finra_tech_broad_short_spread", "FINRA QQQ - SPY Short Ratio Spread"),
]


def _iter_candidate_days(lookback_days: int) -> list[date]:
    end = datetime.now(UTC).date()
    out: list[date] = []
    for offset in range(lookback_days + 1):
        day = end - timedelta(days=offset)
        if day.weekday() < 5:
            out.append(day)
    return out


def _parse_finra_text(text: str, universe: set[str]) -> dict[str, float]:
    frame = pd.read_csv(StringIO(text), sep="|", dtype={"Symbol": str})
    if "Date" not in frame.columns or "Symbol" not in frame.columns:
        raise RuntimeError("Unexpected FINRA short volume columns")

    if not frame.empty and isinstance(frame.iloc[-1]["Date"], str) and "|" not in str(frame.iloc[-1]["Date"]):
        pass

    frame = frame[frame["Symbol"].isin(universe)].copy()
    if frame.empty:
        return {}

    frame["ShortVolume"] = pd.to_numeric(frame["ShortVolume"], errors="coerce")
    frame["TotalVolume"] = pd.to_numeric(frame["TotalVolume"], errors="coerce")
    frame = frame.dropna(subset=["ShortVolume", "TotalVolume"])
    frame = frame[frame["TotalVolume"] > 0]
    if frame.empty:
        return {}

    frame["ratio"] = frame["ShortVolume"] / frame["TotalVolume"]
    return dict(zip(frame["Symbol"], frame["ratio"], strict=False))


def _fetch_finra_history(*, lookback_days: int = _LOOKBACK_DAYS, timeout: int = 30) -> dict[str, list[dict]]:
    universe = tuple(sorted(spec.symbol for spec in _RAW_SPECS))
    cache_key = f"finra_short_volume:{lookback_days}:{','.join(universe)}"

    def _fetch() -> dict[str, list[dict]]:
        rows_by_symbol: dict[str, list[dict]] = {symbol: [] for symbol in universe}
        for trade_day in _iter_candidate_days(lookback_days):
            url = _BASE_URL.format(trade_day)
            resp = requests.get(url, headers=_HEADERS, timeout=timeout)
            if resp.status_code in (403, 404):
                continue
            resp.raise_for_status()
            ratios = _parse_finra_text(resp.text, set(universe))
            if not ratios:
                continue
            ts = pd.Timestamp(trade_day, tz="UTC")
            for symbol, ratio in ratios.items():
                rows_by_symbol[symbol].append({"date": ts, "value": float(ratio)})

        populated = {
            symbol: rows
            for symbol, rows in rows_by_symbol.items()
            if rows
        }
        if not populated:
            raise RuntimeError("No FINRA short volume history found")
        return populated

    return _finra_cache.get_or_fetch(cache_key, _fetch)


def _symbol_series(symbol: str) -> pd.DataFrame:
    history = _fetch_finra_history()
    rows = history.get(symbol, [])
    return build_timeseries_df(rows, f"FINRA short volume {symbol}")


def _spread_series(left: str, right: str) -> pd.DataFrame:
    history = _fetch_finra_history()
    left_rows = history.get(left, [])
    right_rows = history.get(right, [])
    if not left_rows or not right_rows:
        raise RuntimeError(f"No FINRA spread history for {left}/{right}")

    left_df = pd.DataFrame(left_rows).rename(columns={"value": "left_value"})
    right_df = pd.DataFrame(right_rows).rename(columns={"value": "right_value"})
    merged = left_df.merge(right_df, on="date", how="inner").sort_values("date")
    if merged.empty:
        raise RuntimeError(f"No overlapping FINRA spread history for {left}/{right}")

    merged["value"] = merged["left_value"] - merged["right_value"]
    return build_timeseries_df(
        merged[["date", "value"]].to_dict(orient="records"),
        f"FINRA short spread {left}/{right}",
    )


def _make_raw_collector(spec: _RawSpec) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=spec.name,
            display_name=spec.display_name,
            update_frequency="daily",
            api_docs_url="https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data/daily-short-sale-volume-files",
            domain="markets",
            category="microstructure",
        )

        def fetch(self) -> pd.DataFrame:
            return _symbol_series(spec.symbol)

    _Collector.__name__ = f"FINRAShortVolume_{spec.symbol}"
    _Collector.__qualname__ = _Collector.__name__
    return _Collector


def _make_spread_collector(spec: _SpreadSpec) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=spec.name,
            display_name=spec.display_name,
            update_frequency="daily",
            api_docs_url="https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data/daily-short-sale-volume-files",
            domain="markets",
            category="microstructure",
        )

        def fetch(self) -> pd.DataFrame:
            return _spread_series(spec.left, spec.right)

    _Collector.__name__ = f"FINRAShortSpread_{spec.name}"
    _Collector.__qualname__ = _Collector.__name__
    return _Collector


def get_finra_short_volume_collectors() -> dict[str, type[BaseCollector]]:
    collectors = {spec.name: _make_raw_collector(spec) for spec in _RAW_SPECS}
    collectors.update({spec.name: _make_spread_collector(spec) for spec in _SPREAD_SPECS})
    return collectors
