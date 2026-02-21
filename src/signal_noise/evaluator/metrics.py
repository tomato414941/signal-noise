from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy import stats


@dataclass
class SignalMetrics:
    source_name: str
    period: str
    ic: float
    ic_pvalue: float
    pearson_corr: float
    pearson_pvalue: float
    directional_accuracy: float
    best_lag: int
    best_lag_ic: float
    n_observations: int
    significant: bool = False


def compute_ic(signal: pd.Series, returns: pd.Series) -> tuple[float, float]:
    mask = signal.notna() & returns.notna()
    if mask.sum() < 10:
        return 0.0, 1.0
    corr, pval = stats.spearmanr(signal[mask], returns[mask])
    return float(corr), float(pval)


def compute_pearson(signal: pd.Series, returns: pd.Series) -> tuple[float, float]:
    mask = signal.notna() & returns.notna()
    if mask.sum() < 10:
        return 0.0, 1.0
    corr, pval = stats.pearsonr(signal[mask], returns[mask])
    return float(corr), float(pval)


def directional_accuracy(signal: pd.Series, returns: pd.Series) -> float:
    mask = signal.notna() & returns.notna() & (signal != 0) & (returns != 0)
    if mask.sum() < 10:
        return 0.5
    same_dir = np.sign(signal[mask].values) == np.sign(returns[mask].values)
    return float(same_dir.mean())


def lagged_ic(signal: pd.Series, returns: pd.Series, max_lag: int) -> tuple[int, float]:
    best_lag = 0
    best_abs_ic = 0.0
    for lag in range(1, max_lag + 1):
        shifted = signal.shift(lag)
        ic, _ = compute_ic(shifted, returns)
        if abs(ic) > best_abs_ic:
            best_abs_ic = abs(ic)
            best_lag = lag
    return best_lag, best_abs_ic


def evaluate_signal(
    signal: pd.Series,
    returns: pd.Series,
    source_name: str,
    period: str,
    max_lag: int = 24,
) -> SignalMetrics:
    ic, ic_pval = compute_ic(signal, returns)
    pearson, pearson_pval = compute_pearson(signal, returns)
    da = directional_accuracy(signal, returns)
    best_lag, best_lag_ic = lagged_ic(signal, returns, max_lag)
    mask = signal.notna() & returns.notna()
    return SignalMetrics(
        source_name=source_name,
        period=period,
        ic=ic,
        ic_pvalue=ic_pval,
        pearson_corr=pearson,
        pearson_pvalue=pearson_pval,
        directional_accuracy=da,
        best_lag=best_lag,
        best_lag_ic=best_lag_ic,
        n_observations=int(mask.sum()),
    )
