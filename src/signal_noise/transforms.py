from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class Transform:
    name: str
    fn: Callable[[pd.Series], pd.Series]


def _z_score(series: pd.Series, window: int) -> pd.Series:
    rolling_mean = series.rolling(window, min_periods=1).mean()
    rolling_std = series.rolling(window, min_periods=1).std()
    return (series - rolling_mean) / rolling_std.replace(0, np.nan)


def _sma_ratio(series: pd.Series, window: int) -> pd.Series:
    sma = series.rolling(window, min_periods=1).mean()
    return series / sma.replace(0, np.nan)


def _momentum(series: pd.Series, window: int) -> pd.Series:
    return series.pct_change(window)


def _roc(series: pd.Series, window: int) -> pd.Series:
    shifted = series.shift(window)
    return (series - shifted) / shifted.replace(0, np.nan)


def _rolling_vol(series: pd.Series, window: int) -> pd.Series:
    return series.pct_change().rolling(window, min_periods=2).std()


def _rsi(series: pd.Series, window: int) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.rolling(window, min_periods=1).mean()
    avg_loss = loss.rolling(window, min_periods=1).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def _mean_reversion(series: pd.Series, window: int) -> pd.Series:
    sma = series.rolling(window, min_periods=1).mean()
    std = series.rolling(window, min_periods=1).std()
    return (series - sma) / std.replace(0, np.nan)


def _log_return(series: pd.Series) -> pd.Series:
    return np.log(series / series.shift(1))


def _bollinger_pct(series: pd.Series, window: int) -> pd.Series:
    sma = series.rolling(window, min_periods=1).mean()
    std = series.rolling(window, min_periods=1).std()
    upper = sma + 2 * std
    lower = sma - 2 * std
    bandwidth = upper - lower
    return (series - lower) / bandwidth.replace(0, np.nan)


def _ema_ratio(series: pd.Series, window: int) -> pd.Series:
    ema = series.ewm(span=window, min_periods=1).mean()
    return series / ema.replace(0, np.nan)


def _diff(series: pd.Series, window: int) -> pd.Series:
    return series.diff(window)


def _rank_pct(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window, min_periods=1).apply(
        lambda x: pd.Series(x).rank(pct=True).iloc[-1], raw=False
    )


def build_transforms() -> list[Transform]:
    transforms: list[Transform] = []

    for w in (10, 20, 50):
        transforms.append(Transform(f"z_{w}", lambda s, w=w: _z_score(s, w)))

    for w in (10, 20, 50):
        transforms.append(Transform(f"sma_{w}", lambda s, w=w: _sma_ratio(s, w)))

    for w in (10, 20):
        transforms.append(Transform(f"ema_{w}", lambda s, w=w: _ema_ratio(s, w)))

    for w in (5, 10, 20):
        transforms.append(Transform(f"mom_{w}", lambda s, w=w: _momentum(s, w)))

    for w in (5, 20):
        transforms.append(Transform(f"roc_{w}", lambda s, w=w: _roc(s, w)))

    for w in (10, 20):
        transforms.append(Transform(f"vol_{w}", lambda s, w=w: _rolling_vol(s, w)))

    for w in (5, 10):
        transforms.append(Transform(f"diff_{w}", lambda s, w=w: _diff(s, w)))

    transforms.append(Transform("rsi_14", lambda s: _rsi(s, 14)))
    transforms.append(Transform("log_ret", _log_return))
    transforms.append(Transform("boll_pct", lambda s: _bollinger_pct(s, 20)))
    transforms.append(Transform("mean_rev", lambda s: _mean_reversion(s, 20)))
    transforms.append(Transform("rank_pct", lambda s: _rank_pct(s, 20)))

    return transforms


TRANSFORMS = build_transforms()
