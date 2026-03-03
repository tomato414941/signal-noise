"""Shared utilities for factory-generated collectors."""
from __future__ import annotations

import pandas as pd


def build_timeseries_df(rows: list[dict], source_label: str) -> pd.DataFrame:
    """Convert a list of {date, value} dicts into a sorted DataFrame.

    Raises RuntimeError if rows is empty.
    """
    if not rows:
        raise RuntimeError(f"No data for {source_label}")
    return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
