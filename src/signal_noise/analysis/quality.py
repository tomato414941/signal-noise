from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import numpy as np
from scipy.stats import ks_2samp

from ..store.sqlite_store import SignalStore

log = logging.getLogger(__name__)


@dataclass
class SignalQuality:
    name: str
    domain: str
    category: str
    completeness: float
    freshness: float
    stability: float
    independence: float
    health_score: float


@dataclass
class QualityResult:
    n_signals: int
    n_healthy: int
    n_degraded: int
    n_poor: int
    signals: list[SignalQuality]

    def summary(self) -> str:
        lines = []
        lines.append(f"Signals: {self.n_signals}")
        lines.append(
            f"  Healthy: {self.n_healthy}  "
            f"Degraded: {self.n_degraded}  "
            f"Poor: {self.n_poor}"
        )

        lines.append("\nWorst signals (bottom 15):")
        for sq in self.signals[:15]:
            lines.append(
                f"  {sq.name:40s} [{sq.category:15s}] "
                f"h={sq.health_score:.3f}  "
                f"c={sq.completeness:.2f} f={sq.freshness:.2f} "
                f"s={sq.stability:.2f} i={sq.independence:.2f}"
            )

        lines.append("\nBest signals (top 15):")
        for sq in self.signals[-15:][::-1]:
            lines.append(
                f"  {sq.name:40s} [{sq.category:15s}] "
                f"h={sq.health_score:.3f}  "
                f"c={sq.completeness:.2f} f={sq.freshness:.2f} "
                f"s={sq.stability:.2f} i={sq.independence:.2f}"
            )

        return "\n".join(lines)


def compute_quality(
    store: SignalStore,
    *,
    days: int = 90,
    domain: str | None = None,
) -> QualityResult:
    conn = store._conn

    # Load signal metadata
    if domain:
        rows = conn.execute(
            "SELECT name, domain, category, interval FROM signal_meta WHERE domain = ?",
            (domain,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT name, domain, category, interval FROM signal_meta"
        ).fetchall()

    if not rows:
        raise ValueError("No signals found in database")

    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(days=days)).strftime("%Y-%m-%d")

    signals: list[SignalQuality] = []
    for row in rows:
        name, sig_domain, category, interval = row["name"], row["domain"], row["category"], row["interval"]

        # Fetch data within the window
        data = conn.execute(
            "SELECT SUBSTR(timestamp, 1, 10) as date, value "
            "FROM signals WHERE name = ? AND timestamp >= ? ORDER BY timestamp",
            (name, cutoff),
        ).fetchall()

        if not data:
            signals.append(SignalQuality(
                name=name, domain=sig_domain, category=category,
                completeness=0.0, freshness=0.0, stability=0.5,
                independence=1.0, health_score=0.0,
            ))
            continue

        values = [r["value"] for r in data if r["value"] is not None]
        dates = sorted(set(r["date"] for r in data))

        # Completeness: observed days / expected days
        if interval == 86400:
            expected = days
        else:
            expected = days * (86400 // max(interval, 1))
        completeness = min(len(dates) / max(expected, 1), 1.0)

        # Freshness: exponential decay from latest data
        latest_date = max(dates)
        try:
            latest_dt = datetime.strptime(latest_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            days_old = max(0, (now - latest_dt).days)
        except ValueError:
            days_old = days
        freshness = math.exp(-days_old / 7)

        # Stability: KS test between first half and second half
        stability = 1.0
        if len(values) >= 20:
            mid = len(values) // 2
            first_half = np.array(values[:mid], dtype=float)
            second_half = np.array(values[mid:], dtype=float)
            first_half = first_half[np.isfinite(first_half)]
            second_half = second_half[np.isfinite(second_half)]
            if len(first_half) >= 5 and len(second_half) >= 5:
                ks_stat, _ = ks_2samp(first_half, second_half)
                stability = 1.0 - ks_stat

        # Independence: default 1.0 (spectrum provides this if run separately)
        independence = 1.0

        health = (
            0.35 * completeness
            + 0.30 * freshness
            + 0.20 * stability
            + 0.15 * independence
        )

        signals.append(SignalQuality(
            name=name, domain=sig_domain, category=category,
            completeness=completeness, freshness=freshness,
            stability=stability, independence=independence,
            health_score=health,
        ))

    signals.sort(key=lambda s: s.health_score)

    n_healthy = sum(1 for s in signals if s.health_score >= 0.7)
    n_poor = sum(1 for s in signals if s.health_score < 0.4)
    n_degraded = len(signals) - n_healthy - n_poor

    return QualityResult(
        n_signals=len(signals),
        n_healthy=n_healthy,
        n_degraded=n_degraded,
        n_poor=n_poor,
        signals=signals,
    )
