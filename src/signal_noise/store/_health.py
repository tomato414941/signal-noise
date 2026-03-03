"""Pure functions for signal health classification."""
from __future__ import annotations


def classify_signals(
    meta_rows: list[dict], threshold_factor: float = 2.0,
) -> dict[str, list[dict]]:
    """Classify signals into 4 states based on metadata.

    Each row must have: consecutive_failures, last_updated, age_seconds, interval.
    Returns dict with keys: never_seen, fresh, stale, failing.
    """
    result: dict[str, list[dict]] = {
        "never_seen": [], "fresh": [], "stale": [], "failing": [],
    }
    for d in meta_rows:
        if d["consecutive_failures"] > 0:
            result["failing"].append(d)
        elif d["last_updated"] is None:
            result["never_seen"].append(d)
        elif d["age_seconds"] > d["interval"] * threshold_factor:
            result["stale"].append(d)
        else:
            result["fresh"].append(d)
    return result


def filter_stale(
    meta_rows: list[dict], threshold_factor: float = 2.0,
) -> list[dict]:
    """Filter for signals exceeding age threshold.

    Each row must have: age_seconds, interval, last_updated (not None).
    """
    stale = []
    for d in meta_rows:
        if d.get("last_updated") is None:
            continue
        if d["age_seconds"] > d["interval"] * threshold_factor:
            d["expected_interval"] = d["interval"]
            stale.append(d)
    return stale
