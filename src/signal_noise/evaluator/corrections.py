from __future__ import annotations


def bonferroni(pvalues: list[float], alpha: float = 0.05) -> list[bool]:
    n = len(pvalues)
    if n == 0:
        return []
    return [p < alpha / n for p in pvalues]


def fdr_bh(pvalues: list[float], alpha: float = 0.05) -> list[bool]:
    """Benjamini-Hochberg FDR correction."""
    n = len(pvalues)
    if n == 0:
        return []
    indexed = sorted(enumerate(pvalues), key=lambda x: x[1])
    significant = [False] * n
    max_k = 0
    for k, (orig_idx, p) in enumerate(indexed, 1):
        if p <= (k / n) * alpha:
            max_k = k
    for k in range(max_k):
        orig_idx = indexed[k][0]
        significant[orig_idx] = True
    return significant
