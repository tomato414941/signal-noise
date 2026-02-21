from __future__ import annotations

import logging

import pandas as pd

from signal_noise.collector import COLLECTORS
from signal_noise.config import DEFAULT_EVALUATION, EvaluationConfig, RAW_DIR
from signal_noise.evaluator.corrections import bonferroni, fdr_bh
from signal_noise.evaluator.metrics import SignalMetrics, evaluate_signal
from signal_noise.evaluator.returns import compute_forward_returns

log = logging.getLogger(__name__)


def _align_signal_to_target(
    signal_df: pd.DataFrame, target_df: pd.DataFrame
) -> pd.Series:
    ts_col = "timestamp" if "timestamp" in signal_df.columns else "date"
    sig = signal_df[[ts_col, "value"]].copy()
    sig = sig.rename(columns={ts_col: "timestamp"})
    sig["timestamp"] = pd.to_datetime(sig["timestamp"], utc=True).astype("datetime64[us, UTC]")

    target_ts = target_df[["timestamp"]].copy()
    target_ts["timestamp"] = pd.to_datetime(target_ts["timestamp"], utc=True).astype("datetime64[us, UTC]")

    if ts_col == "date":
        sig["_date"] = sig["timestamp"].dt.normalize()
        target_ts["_date"] = target_ts["timestamp"].dt.normalize()
        merged = pd.merge_asof(
            target_ts.sort_values("_date"),
            sig[["_date", "value"]].sort_values("_date"),
            on="_date",
            direction="backward",
        )
        return merged["value"].reset_index(drop=True)
    else:
        merged = pd.merge_asof(
            target_ts.sort_values("timestamp"),
            sig.sort_values("timestamp"),
            on="timestamp",
            direction="backward",
        )
        return merged["value"].reset_index(drop=True)


def run_evaluation(
    config: EvaluationConfig | None = None,
    target_source: str = "btc_ohlcv",
) -> list[SignalMetrics]:
    config = config or DEFAULT_EVALUATION

    target_path = RAW_DIR / f"{target_source}.parquet"
    if not target_path.exists():
        raise FileNotFoundError(
            f"Target data not found: {target_path}. "
            f"Run: python -m signal_noise collect -s {target_source}"
        )

    target_df = pd.read_parquet(target_path)
    returns_df = compute_forward_returns(target_df, config.return_periods)

    all_metrics: list[SignalMetrics] = []

    for source_name in COLLECTORS:
        if source_name == target_source:
            continue

        parquet = RAW_DIR / f"{source_name}.parquet"
        if not parquet.exists():
            log.info("Skipping %s (no data)", source_name)
            continue

        signal_df = pd.read_parquet(parquet)
        aligned = _align_signal_to_target(signal_df, target_df)

        for period in config.return_periods:
            ret_col = f"ret_{period}"
            if ret_col not in returns_df.columns:
                continue
            metrics = evaluate_signal(
                signal=aligned,
                returns=returns_df[ret_col],
                source_name=source_name,
                period=period,
                max_lag=config.max_lag_periods,
            )
            all_metrics.append(metrics)

    pvalues = [m.ic_pvalue for m in all_metrics]
    if config.correction_method == "bonferroni":
        significant = bonferroni(pvalues, config.significance_level)
    else:
        significant = fdr_bh(pvalues, config.significance_level)

    for m, sig in zip(all_metrics, significant):
        m.significant = sig

    all_metrics.sort(key=lambda m: abs(m.ic), reverse=True)
    return all_metrics
