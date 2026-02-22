from __future__ import annotations

import json
from dataclasses import asdict

from signal_noise.config import REPORTS_DIR
from signal_noise.evaluator.metrics import SignalMetrics


def generate_report(metrics: list[SignalMetrics], top_n: int | None = None) -> str:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    json_path = REPORTS_DIR / "evaluation.json"
    json_path.write_text(json.dumps([asdict(m) for m in metrics], indent=2, default=str))

    lines = [
        "=" * 80,
        "  SIGNAL-NOISE EVALUATION REPORT",
        "  \"even noise is worth collecting -- the signal hides within\"",
        "=" * 80,
        "",
        f"Total signals evaluated: {len(metrics)}",
        f"Significant (after correction): {sum(1 for m in metrics if m.significant)}",
        "",
        f"{'Source':<32} {'Period':<6} {'IC':>7} {'p-value':>10} {'DirAcc':>7} "
        f"{'Lag':>4} {'LagIC':>7} {'Sig':>4} {'N':>6}",
        "-" * 80,
    ]

    display = metrics[:top_n] if top_n else metrics
    if top_n and len(metrics) > top_n:
        lines.append(f"(showing top {top_n} of {len(metrics)})")
        lines.append("")

    for m in display:
        sig_mark = "*" if m.significant else ""
        lines.append(
            f"{m.collector_name:<32} {m.period:<6} {m.ic:>+.4f} {m.ic_pvalue:>10.6f} "
            f"{m.directional_accuracy:>6.1%} {m.best_lag:>4} {m.best_lag_ic:>+.4f} "
            f"{sig_mark:>4} {m.n_observations:>6}"
        )

    text = "\n".join(lines)
    txt_path = REPORTS_DIR / "evaluation.txt"
    txt_path.write_text(text)
    return text
