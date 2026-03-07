from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from signal_noise.collector._cache import SharedAPICache
from signal_noise.collector._utils import build_timeseries_df
from signal_noise.collector.base import BaseCollector, CollectorMeta
from signal_noise.collector.sec_form4 import _fetch_ticker_history

_form4_factor_cache = SharedAPICache(ttl=21_600)

_GROUPS: dict[str, tuple[str, ...]] = {
    "cross_sector": ("TSLA", "META", "NVDA", "GS", "JPM", "XOM", "CVX", "DHI", "LEN", "CAT", "DE"),
    "tech": ("TSLA", "META", "NVDA"),
    "financials": ("GS", "JPM"),
    "energy": ("XOM", "CVX"),
    "housing": ("DHI", "LEN"),
    "industrials": ("CAT", "DE"),
}

_GROUP_LABELS = {
    "cross_sector": "Cross-Sector",
    "tech": "Tech",
    "financials": "Financials",
    "energy": "Energy",
    "housing": "Housing",
    "industrials": "Industrials",
}


@dataclass(frozen=True)
class _BreadthSpec:
    kind: str
    group: str
    name: str
    display_name: str


@dataclass(frozen=True)
class _SpreadSpec:
    metric: str
    left_group: str
    right_group: str
    name: str
    display_name: str


_BREADTH_SPECS: list[_BreadthSpec] = [
    _BreadthSpec("net_buy_breadth", "cross_sector", "form4_cross_sector_net_buy_breadth", "SEC Form 4 Cross-Sector Net-Buy Breadth"),
    _BreadthSpec("sell_breadth", "cross_sector", "form4_cross_sector_sell_breadth", "SEC Form 4 Cross-Sector Sell Breadth"),
    _BreadthSpec("tx_cluster_count", "cross_sector", "form4_cross_sector_tx_cluster_count", "SEC Form 4 Cross-Sector Transaction Cluster Count"),
    _BreadthSpec("high_conviction_breadth", "cross_sector", "form4_cross_sector_high_conviction_breadth", "SEC Form 4 Cross-Sector High-Conviction Breadth"),
    _BreadthSpec("net_buy_breadth", "tech", "form4_tech_net_buy_breadth", "SEC Form 4 Tech Net-Buy Breadth"),
    _BreadthSpec("net_buy_breadth", "financials", "form4_financials_net_buy_breadth", "SEC Form 4 Financials Net-Buy Breadth"),
    _BreadthSpec("net_buy_breadth", "energy", "form4_energy_net_buy_breadth", "SEC Form 4 Energy Net-Buy Breadth"),
    _BreadthSpec("net_buy_breadth", "housing", "form4_housing_net_buy_breadth", "SEC Form 4 Housing Net-Buy Breadth"),
    _BreadthSpec("net_buy_breadth", "industrials", "form4_industrials_net_buy_breadth", "SEC Form 4 Industrials Net-Buy Breadth"),
]

_SPREAD_SPECS: list[_SpreadSpec] = [
    _SpreadSpec("net_share_ratio", "tech", "financials", "form4_tech_financials_net_buy_spread", "SEC Form 4 Tech - Financials Net-Buy Spread"),
    _SpreadSpec("net_share_ratio", "housing", "industrials", "form4_housing_industrials_net_buy_spread", "SEC Form 4 Housing - Industrials Net-Buy Spread"),
    _SpreadSpec("open_market_tx_count", "energy", "housing", "form4_energy_housing_tx_spread", "SEC Form 4 Energy - Housing Transaction Spread"),
]


def _group_frame(group: str) -> pd.DataFrame:
    cache_key = f"sec_form4_factor:{group}"

    def _fetch() -> pd.DataFrame:
        merged: pd.DataFrame | None = None
        for ticker in _GROUPS[group]:
            frame = _fetch_ticker_history(ticker).rename(
                columns={
                    "net_share_ratio": f"{ticker}_net_share_ratio",
                    "open_market_tx_count": f"{ticker}_open_market_tx_count",
                }
            )
            if merged is None:
                merged = frame
            else:
                merged = merged.merge(frame, on="date", how="outer")
        if merged is None:
            raise RuntimeError(f"No SEC Form 4 factor data for {_GROUP_LABELS[group]}")
        return merged.sort_values("date").reset_index(drop=True)

    return _form4_factor_cache.get_or_fetch(cache_key, _fetch)


def _net_buy_breadth(group: str) -> pd.DataFrame:
    frame = _group_frame(group).set_index("date")
    cols = [col for col in frame.columns if col.endswith("_net_share_ratio")]
    values = frame[cols]
    counts = values.notna().sum(axis=1)
    breadth = values.gt(0).sum(axis=1) / counts
    rows = [{"date": idx, "value": float(val)} for idx, val in breadth.where(counts > 0).dropna().items()]
    return build_timeseries_df(rows, f"SEC Form 4 net-buy breadth {group}")


def _sell_breadth(group: str) -> pd.DataFrame:
    frame = _group_frame(group).set_index("date")
    cols = [col for col in frame.columns if col.endswith("_net_share_ratio")]
    values = frame[cols]
    counts = values.notna().sum(axis=1)
    breadth = values.lt(0).sum(axis=1) / counts
    rows = [{"date": idx, "value": float(val)} for idx, val in breadth.where(counts > 0).dropna().items()]
    return build_timeseries_df(rows, f"SEC Form 4 sell breadth {group}")


def _tx_cluster_count(group: str) -> pd.DataFrame:
    frame = _group_frame(group).set_index("date")
    cols = [col for col in frame.columns if col.endswith("_open_market_tx_count")]
    counts = frame[cols].gt(0).sum(axis=1)
    rows = [{"date": idx, "value": float(val)} for idx, val in counts.items()]
    return build_timeseries_df(rows, f"SEC Form 4 tx cluster count {group}")


def _high_conviction_breadth(group: str) -> pd.DataFrame:
    frame = _group_frame(group).set_index("date")
    ratio_cols = [col for col in frame.columns if col.endswith("_net_share_ratio")]
    tx_cols = [col for col in frame.columns if col.endswith("_open_market_tx_count")]
    ratios = frame[ratio_cols]
    txs = frame[tx_cols]
    signals = pd.DataFrame(index=frame.index)
    for ticker in _GROUPS[group]:
        signals[ticker] = (
            ratios[f"{ticker}_net_share_ratio"].abs().ge(0.5)
            & txs[f"{ticker}_open_market_tx_count"].gt(0)
        )
    breadth = signals.sum(axis=1) / len(_GROUPS[group])
    rows = [{"date": idx, "value": float(val)} for idx, val in breadth.items()]
    return build_timeseries_df(rows, f"SEC Form 4 conviction breadth {group}")


def _group_average(group: str, metric: str) -> pd.Series:
    frame = _group_frame(group).set_index("date")
    cols = [col for col in frame.columns if col.endswith(metric)]
    return frame[cols].mean(axis=1, skipna=True)


def _spread_series(metric: str, left_group: str, right_group: str) -> pd.DataFrame:
    left = _group_average(left_group, metric)
    right = _group_average(right_group, metric)
    merged = pd.concat([left.rename("left"), right.rename("right")], axis=1).dropna()
    if merged.empty:
        raise RuntimeError(f"No overlapping SEC Form 4 spread history for {left_group}/{right_group}")
    merged["value"] = merged["left"] - merged["right"]
    rows = [{"date": idx, "value": float(val)} for idx, val in merged["value"].items()]
    return build_timeseries_df(rows, f"SEC Form 4 spread {left_group}/{right_group}")


def _build_breadth_frame(kind: str, group: str) -> pd.DataFrame:
    if kind == "net_buy_breadth":
        return _net_buy_breadth(group)
    if kind == "sell_breadth":
        return _sell_breadth(group)
    if kind == "tx_cluster_count":
        return _tx_cluster_count(group)
    if kind == "high_conviction_breadth":
        return _high_conviction_breadth(group)
    raise RuntimeError(f"Unknown SEC Form 4 breadth kind: {kind}")


def _make_breadth_collector(spec: _BreadthSpec) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=spec.name,
            display_name=spec.display_name,
            update_frequency="daily",
            api_docs_url="https://www.sec.gov/search-filings/edgar-application-programming-interfaces",
            domain="markets",
            category="regulatory",
        )

        def fetch(self) -> pd.DataFrame:
            return _build_breadth_frame(spec.kind, spec.group)

    _Collector.__name__ = f"SECForm4Factor_{spec.name}"
    _Collector.__qualname__ = _Collector.__name__
    return _Collector


def _make_spread_collector(spec: _SpreadSpec) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=spec.name,
            display_name=spec.display_name,
            update_frequency="daily",
            api_docs_url="https://www.sec.gov/search-filings/edgar-application-programming-interfaces",
            domain="markets",
            category="regulatory",
        )

        def fetch(self) -> pd.DataFrame:
            return _spread_series(spec.metric, spec.left_group, spec.right_group)

    _Collector.__name__ = f"SECForm4Factor_{spec.name}"
    _Collector.__qualname__ = _Collector.__name__
    return _Collector


def get_sec_form4_factor_collectors() -> dict[str, type[BaseCollector]]:
    collectors = {spec.name: _make_breadth_collector(spec) for spec in _BREADTH_SPECS}
    collectors.update({spec.name: _make_spread_collector(spec) for spec in _SPREAD_SPECS})
    return collectors
