"""Pure functions for signal health classification."""
from __future__ import annotations


def classify_signals(
    meta_rows: list[dict], threshold_factor: float = 2.0,
) -> dict[str, list[dict]]:
    """Classify signals into 5 states based on metadata.

    Each row must have: consecutive_failures, last_updated, age_seconds, interval.
    Returns dict with keys: suppressed, never_seen, fresh, stale, failing.
    """
    result: dict[str, list[dict]] = {
        "suppressed": [], "never_seen": [], "fresh": [], "stale": [], "failing": [],
    }
    for d in meta_rows:
        if d.get("suppressed"):
            result["suppressed"].append(d)
        elif d["consecutive_failures"] > 0:
            result["failing"].append(d)
        elif d["last_updated"] is None:
            result["never_seen"].append(d)
        elif d["age_seconds"] is None:
            # last_updated set but julianday() failed (corrupted timestamp)
            result["stale"].append(d)
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
