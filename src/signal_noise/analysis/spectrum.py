"""Spectral analysis of signal collection coverage.

Computes SVD on the signal matrix to measure effective dimensionality,
redundancy, and identify independent information axes.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd

from ..store.sqlite_store import SignalStore

log = logging.getLogger(__name__)


@dataclass
class PrincipalComponent:
    index: int
    variance_ratio: float
    cumulative_variance: float
    top_signals: list[tuple[str, float]]  # (name, loading)
    domain_composition: dict[str, int]


@dataclass
class SignalProfile:
    name: str
    domain: str
    category: str
    uniqueness: float  # residual variance after top-k PCs


@dataclass
class SpectrumResult:
    n_signals: int
    n_dates: int
    singular_values: np.ndarray
    variance_ratios: np.ndarray
    cumulative_variance: np.ndarray
    components: list[PrincipalComponent]
    effective_dims: dict[int, int]  # threshold% -> d
    participation_ratio: float
    spectral_entropy: float
    spectral_entropy_normalized: float
    redundant: list[SignalProfile]
    unique: list[SignalProfile]

    def summary(self) -> str:
        lines = []
        lines.append(f"Matrix: {self.n_dates} dates x {self.n_signals} signals")
        lines.append("")

        lines.append("Effective Dimensionality:")
        for pct, d in sorted(self.effective_dims.items()):
            lines.append(f"  d({pct}%) = {d:4d}  ({d / self.n_signals * 100:.1f}% of signals)")

        lines.append(f"\nParticipation Ratio: {self.participation_ratio:.1f} / {self.n_signals}")
        lines.append(
            f"Spectral Entropy: {self.spectral_entropy:.3f} "
            f"(normalized: {self.spectral_entropy_normalized:.3f})"
        )

        lines.append(f"\nPrincipal Components (top {len(self.components)}):")
        for pc in self.components:
            var_pct = pc.variance_ratio * 100
            dom = ", ".join(f"{k}:{v}" for k, v in
                           sorted(pc.domain_composition.items(), key=lambda x: -x[1])[:3])
            lines.append(f"  PC{pc.index:3d} ({var_pct:5.1f}%) — {dom}")
            for name, loading in pc.top_signals[:5]:
                sign = "+" if loading > 0 else "-"
                lines.append(f"    {sign} {name}")

        lines.append(f"\nMost Redundant (top {len(self.redundant)}):")
        for s in self.redundant[:10]:
            lines.append(
                f"  {s.name:40s} [{s.category:15s}] u={s.uniqueness:.4f}"
            )

        lines.append(f"\nMost Unique (top {len(self.unique)}):")
        for s in self.unique[:10]:
            lines.append(
                f"  {s.name:40s} [{s.category:15s}] u={s.uniqueness:.4f}"
            )

        return "\n".join(lines)


def compute_spectrum(
    store: SignalStore,
    *,
    min_rows: int = 200,
    min_fill_ratio: float = 0.5,
    n_components: int = 8,
    n_top_signals: int = 8,
    n_profiles: int = 15,
) -> SpectrumResult:
    """Run SVD spectral analysis on daily signals."""
    conn = store._conn

    # Load daily signals
    daily = conn.execute(
        "SELECT name FROM signal_meta WHERE interval = 86400"
    ).fetchall()
    daily_names = [r[0] for r in daily]
    log.info("Daily signals in meta: %d", len(daily_names))

    if not daily_names:
        raise ValueError("No daily signals found in database")

    placeholders = ",".join("?" for _ in daily_names)
    df = pd.read_sql_query(
        f"SELECT name, SUBSTR(timestamp, 1, 10) as date, value "
        f"FROM signals WHERE name IN ({placeholders})",
        conn, params=daily_names,
    )

    matrix = df.pivot_table(index="date", columns="name", values="value", aggfunc="last")

    # Filter signals by minimum data points
    good_cols = matrix.columns[matrix.notna().sum() >= min_rows]
    matrix = matrix[good_cols]
    log.info("Signals with >= %d points: %d", min_rows, len(good_cols))

    # Filter dates by fill ratio
    threshold = len(good_cols) * min_fill_ratio
    good_rows = matrix.index[matrix.notna().sum(axis=1) >= threshold]
    matrix = matrix.loc[good_rows]

    # Forward fill and drop remaining NaN columns
    matrix = matrix.sort_index().ffill().dropna(axis=1)
    log.info("Final matrix: %d dates x %d signals", *matrix.shape)

    if matrix.shape[1] < 3:
        raise ValueError(f"Too few signals after filtering: {matrix.shape[1]}")

    # Load metadata
    meta: dict[str, dict[str, str]] = {}
    for name in matrix.columns:
        row = conn.execute(
            "SELECT domain, category FROM signal_meta WHERE name = ?", (name,)
        ).fetchone()
        meta[name] = {
            "domain": row[0] if row else "unknown",
            "category": row[1] if row else "unknown",
        }

    # Z-score standardization
    stds = matrix.std().replace(0, 1)
    Z = (matrix - matrix.mean()) / stds

    # SVD
    U, sigma, Vt = np.linalg.svd(Z.values, full_matrices=False)

    var_explained = sigma**2 / (sigma**2).sum()
    cumvar = np.cumsum(var_explained)

    # Principal components
    components = []
    for i in range(min(n_components, len(sigma))):
        loadings = Vt[i, :]
        order = np.argsort(np.abs(loadings))[::-1]

        top_sigs = []
        domains: dict[str, int] = {}
        for idx in order[:n_top_signals]:
            name = matrix.columns[idx]
            top_sigs.append((name, float(loadings[idx])))
            d = meta[name]["domain"]
            domains[d] = domains.get(d, 0) + 1

        # Extend domain count to top 20 for more representative composition
        for idx in order[:20]:
            d = meta[matrix.columns[idx]]["domain"]
            if d not in domains:
                domains[d] = 0
            domains[d] = domains.get(d, 0)

        components.append(PrincipalComponent(
            index=i + 1,
            variance_ratio=float(var_explained[i]),
            cumulative_variance=float(cumvar[i]),
            top_signals=top_sigs,
            domain_composition=domains,
        ))

    # Effective dimensionality at various thresholds
    effective_dims = {}
    for pct in [50, 80, 90, 95, 99]:
        effective_dims[pct] = int(np.searchsorted(cumvar, pct / 100) + 1)

    # Participation ratio
    lambdas = sigma**2
    pr = float((lambdas.sum())**2 / (lambdas**2).sum())

    # Spectral entropy
    p = lambdas / lambdas.sum()
    entropy = float(-np.sum(p * np.log(p + 1e-15)))
    max_entropy = float(np.log(len(p)))
    entropy_norm = entropy / max_entropy if max_entropy > 0 else 0.0

    # Uniqueness: residual variance after top-k reconstruction
    k = min(n_components, len(sigma))
    Z_approx = U[:, :k] @ np.diag(sigma[:k]) @ Vt[:k, :]
    residual = np.mean((Z.values - Z_approx) ** 2, axis=0)
    total_var = np.mean(Z.values**2, axis=0)
    uniqueness = residual / (total_var + 1e-10)

    profiles = []
    for j, name in enumerate(matrix.columns):
        profiles.append(SignalProfile(
            name=name,
            domain=meta[name]["domain"],
            category=meta[name]["category"],
            uniqueness=float(uniqueness[j]),
        ))

    profiles_sorted = sorted(profiles, key=lambda s: s.uniqueness)
    redundant = profiles_sorted[:n_profiles]
    unique = profiles_sorted[-n_profiles:][::-1]

    return SpectrumResult(
        n_signals=matrix.shape[1],
        n_dates=matrix.shape[0],
        singular_values=sigma,
        variance_ratios=var_explained,
        cumulative_variance=cumvar,
        components=components,
        effective_dims=effective_dims,
        participation_ratio=pr,
        spectral_entropy=entropy,
        spectral_entropy_normalized=entropy_norm,
        redundant=redundant,
        unique=unique,
    )
