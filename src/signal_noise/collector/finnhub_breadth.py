from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import pandas as pd

from signal_noise.collector._cache import SharedAPICache
from signal_noise.collector._utils import build_timeseries_df
from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector.finnhub_generic import (
    _fetch_earnings,
    _fetch_insider_sentiment,
    _fetch_recommendation,
)

_breadth_cache = SharedAPICache(ttl=3600)

_GROUPS: dict[str, tuple[str, ...]] = {
    "semis": ("NVDA", "AMD", "AVGO"),
    "megacap": ("AAPL", "MSFT", "GOOGL", "AMZN", "META"),
    "energy": ("XOM", "CVX"),
    "industrials": ("CAT", "DE", "UNP", "FDX"),
    "housing": ("DHI", "LEN"),
}

_GROUP_LABELS = {
    "semis": "Semis",
    "megacap": "Megacap Tech",
    "energy": "Energy",
    "industrials": "Industrials/Logistics",
    "housing": "Housing",
}


@dataclass(frozen=True)
class _BreadthSpec:
    source: str
    group: str
    name: str
    display_name: str
    update_frequency: str
    domain: str
    category: str


@dataclass(frozen=True)
class _SpreadSpec:
    source: str
    left_group: str
    right_group: str
    name: str
    display_name: str
    update_frequency: str
    domain: str
    category: str


_BREADTH_SPECS: list[_BreadthSpec] = [
    _BreadthSpec("recommendation", "semis", "finnhub_semis_rec_breadth", "Finnhub Semis Positive Recommendation Breadth", "monthly", "sentiment", "equity"),
    _BreadthSpec("recommendation", "megacap", "finnhub_megacap_rec_breadth", "Finnhub Megacap Positive Recommendation Breadth", "monthly", "sentiment", "equity"),
    _BreadthSpec("recommendation", "energy", "finnhub_energy_rec_breadth", "Finnhub Energy Positive Recommendation Breadth", "monthly", "sentiment", "equity"),
    _BreadthSpec("recommendation", "industrials", "finnhub_industrials_rec_breadth", "Finnhub Industrials Positive Recommendation Breadth", "monthly", "sentiment", "equity"),
    _BreadthSpec("earnings", "semis", "finnhub_semis_earnings_breadth", "Finnhub Semis Positive Earnings Breadth", "quarterly", "markets", "equity"),
    _BreadthSpec("earnings", "housing", "finnhub_housing_earnings_breadth", "Finnhub Housing Positive Earnings Breadth", "quarterly", "markets", "equity"),
    _BreadthSpec("earnings", "industrials", "finnhub_industrials_earnings_breadth", "Finnhub Industrials Positive Earnings Breadth", "quarterly", "markets", "equity"),
    _BreadthSpec("insider", "energy", "finnhub_energy_insider_breadth", "Finnhub Energy Positive Insider Breadth", "monthly", "sentiment", "sentiment"),
    _BreadthSpec("insider", "housing", "finnhub_housing_insider_breadth", "Finnhub Housing Positive Insider Breadth", "monthly", "sentiment", "sentiment"),
    _BreadthSpec("insider", "industrials", "finnhub_industrials_insider_breadth", "Finnhub Industrials Positive Insider Breadth", "monthly", "sentiment", "sentiment"),
]

_SPREAD_SPECS: list[_SpreadSpec] = [
    _SpreadSpec("recommendation", "semis", "energy", "finnhub_semis_energy_rec_spread", "Finnhub Semis - Energy Recommendation Breadth Spread", "monthly", "sentiment", "equity"),
    _SpreadSpec("earnings", "housing", "industrials", "finnhub_housing_industrials_earnings_spread", "Finnhub Housing - Industrials Earnings Breadth Spread", "quarterly", "markets", "equity"),
    _SpreadSpec("insider", "energy", "housing", "finnhub_energy_housing_insider_spread", "Finnhub Energy - Housing Insider Breadth Spread", "monthly", "sentiment", "sentiment"),
]


def _recommendation_rows(symbol: str) -> list[dict]:
    data = _fetch_recommendation(symbol)
    rows: list[dict] = []
    for rec in data:
        total = (
            rec.get("strongBuy", 0) + rec.get("buy", 0) + rec.get("hold", 0)
            + rec.get("sell", 0) + rec.get("strongSell", 0)
        )
        if total == 0:
            continue
        try:
            dt = pd.to_datetime(rec["period"], utc=True)
            score = (
                rec.get("strongBuy", 0) * 5
                + rec.get("buy", 0) * 4
                + rec.get("hold", 0) * 3
                + rec.get("sell", 0) * 2
                + rec.get("strongSell", 0)
            ) / total
        except (KeyError, TypeError, ValueError):
            continue
        rows.append({"date": dt, "value": float(score)})
    return rows


def _insider_rows(symbol: str) -> list[dict]:
    data = _fetch_insider_sentiment(symbol)
    rows: list[dict] = []
    for entry in data:
        year = entry.get("year")
        month = entry.get("month")
        mspr = entry.get("mspr")
        if year is None or month is None or mspr is None:
            continue
        try:
            dt = pd.Timestamp(f"{year}-{month:02d}-01", tz="UTC")
            rows.append({"date": dt, "value": float(mspr)})
        except (TypeError, ValueError):
            continue
    return rows


def _earnings_rows(symbol: str) -> list[dict]:
    data = _fetch_earnings(symbol)
    rows: list[dict] = []
    for entry in data:
        try:
            dt = pd.to_datetime(entry["period"], utc=True)
            rows.append({"date": dt, "value": float(entry["surprisePercent"])})
        except (KeyError, TypeError, ValueError):
            continue
    return rows


_SOURCE_ROW_BUILDERS: dict[str, Callable[[str], list[dict]]] = {
    "recommendation": _recommendation_rows,
    "insider": _insider_rows,
    "earnings": _earnings_rows,
}


def _positive_mask(source: str, frame: pd.DataFrame) -> pd.DataFrame:
    if source == "recommendation":
        return frame.gt(3.0)
    if source in {"insider", "earnings"}:
        return frame.gt(0.0)
    raise RuntimeError(f"Unsupported Finnhub breadth source: {source}")


def _group_value_frame(source: str, group: str) -> pd.DataFrame:
    symbols = _GROUPS[group]
    cache_key = f"finnhub_breadth:{source}:{group}"

    def _fetch() -> pd.DataFrame:
        builder = _SOURCE_ROW_BUILDERS[source]
        frames: list[pd.DataFrame] = []
        for symbol in symbols:
            rows = builder(symbol)
            if not rows:
                continue
            frame = pd.DataFrame(rows).rename(columns={"value": symbol})
            frames.append(frame)
        if not frames:
            raise RuntimeError(f"No Finnhub breadth data for {_GROUP_LABELS[group]}")
        merged = frames[0]
        for frame in frames[1:]:
            merged = merged.merge(frame, on="date", how="outer")
        merged = merged.sort_values("date").reset_index(drop=True)
        value_cols = [col for col in merged.columns if col != "date"]
        if not value_cols:
            raise RuntimeError(f"No Finnhub breadth columns for {_GROUP_LABELS[group]}")
        return merged[["date", *value_cols]]

    return _breadth_cache.get_or_fetch(cache_key, _fetch)


def _breadth_series(source: str, group: str) -> pd.DataFrame:
    values = _group_value_frame(source, group).set_index("date")
    valid = values.notna()
    counts = valid.sum(axis=1)
    positives = _positive_mask(source, values).where(valid, False).sum(axis=1)
    breadth = (positives / counts).where(counts > 0)
    rows = [
        {"date": idx, "value": float(val)}
        for idx, val in breadth.dropna().items()
    ]
    return build_timeseries_df(rows, f"Finnhub breadth {source} {group}")


def _spread_series(source: str, left_group: str, right_group: str) -> pd.DataFrame:
    left = _breadth_series(source, left_group).rename(columns={"value": "left_value"})
    right = _breadth_series(source, right_group).rename(columns={"value": "right_value"})
    merged = left.merge(right, on="date", how="inner").sort_values("date")
    if merged.empty:
        raise RuntimeError(f"No overlapping Finnhub breadth history for {left_group}/{right_group}")
    merged["value"] = merged["left_value"] - merged["right_value"]
    return build_timeseries_df(
        merged[["date", "value"]].to_dict(orient="records"),
        f"Finnhub breadth spread {left_group}/{right_group}",
    )


def _make_breadth_collector(spec: _BreadthSpec) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=spec.name,
            display_name=spec.display_name,
            update_frequency=spec.update_frequency,
            api_docs_url="https://finnhub.io/docs/api/",
            requires_key=True,
            domain=spec.domain,
            category=spec.category,
        )

        def fetch(self) -> pd.DataFrame:
            return _breadth_series(spec.source, spec.group)

    _Collector.__name__ = f"FinnhubBreadth_{spec.name}"
    _Collector.__qualname__ = _Collector.__name__
    return _Collector


def _make_spread_collector(spec: _SpreadSpec) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=spec.name,
            display_name=spec.display_name,
            update_frequency=spec.update_frequency,
            api_docs_url="https://finnhub.io/docs/api/",
            requires_key=True,
            domain=spec.domain,
            category=spec.category,
        )

        def fetch(self) -> pd.DataFrame:
            return _spread_series(spec.source, spec.left_group, spec.right_group)

    _Collector.__name__ = f"FinnhubBreadth_{spec.name}"
    _Collector.__qualname__ = _Collector.__name__
    return _Collector


def get_finnhub_breadth_collectors() -> dict[str, type[BaseCollector]]:
    collectors = {spec.name: _make_breadth_collector(spec) for spec in _BREADTH_SPECS}
    collectors.update({spec.name: _make_spread_collector(spec) for spec in _SPREAD_SPECS})
    return collectors
