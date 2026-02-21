from __future__ import annotations

import argparse
import logging


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="signal-noise",
        description="Collect worldwide signals and evaluate predictive power",
    )
    sub = parser.add_subparsers(dest="command")

    p_collect = sub.add_parser("collect", help="Fetch data from sources")
    p_collect.add_argument("--source", "-s", help="Specific source name")
    p_collect.add_argument("--force", action="store_true", help="Ignore cache")

    p_eval = sub.add_parser("evaluate", help="Run signal evaluation pipeline")
    p_eval.add_argument("--target", "-t", default="btc_ohlcv", help="Target source")
    p_eval.add_argument("--period", "-p", help="Specific return period (e.g. 1d)")

    sub.add_parser("report", help="Show latest evaluation report")
    sub.add_parser("list", help="List available sources with status")

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if args.command == "collect":
        _cmd_collect(args)
    elif args.command == "evaluate":
        _cmd_evaluate(args)
    elif args.command == "report":
        _cmd_report()
    elif args.command == "list":
        _cmd_list()
    else:
        parser.print_help()


def _cmd_collect(args: argparse.Namespace) -> None:
    from signal_noise.collector import COLLECTORS, collect_all
    from signal_noise.config import CACHE_DIR

    sources = [args.source] if args.source else None
    if args.force:
        for name in sources or COLLECTORS.keys():
            cache = CACHE_DIR / f"{name}.json"
            if cache.exists():
                cache.unlink()
    results = collect_all(sources)
    for name, df in results.items():
        print(f"  {name}: {len(df)} rows")


def _cmd_evaluate(args: argparse.Namespace) -> None:
    from signal_noise.config import EvaluationConfig
    from signal_noise.evaluator.pipeline import run_evaluation
    from signal_noise.reporter.report import generate_report

    config = EvaluationConfig()
    if args.period:
        config.return_periods = [args.period]
    metrics = run_evaluation(config, target_source=args.target)
    print(generate_report(metrics))


def _cmd_report() -> None:
    from signal_noise.config import REPORTS_DIR

    txt = REPORTS_DIR / "evaluation.txt"
    if txt.exists():
        print(txt.read_text())
    else:
        print("No report found. Run: python -m signal_noise evaluate")


def _cmd_list() -> None:
    from signal_noise.collector import COLLECTORS

    print(f"{'Source':<25} {'Type':<15} {'Freq':<10} {'Key?':<5} {'Data?':<6} {'Rows':>8}")
    print("-" * 75)
    for name, cls in COLLECTORS.items():
        c = cls()
        s = c.status()
        print(
            f"{s['display_name']:<25} {c.meta.data_type:<15} "
            f"{c.meta.update_frequency:<10} {'yes' if s['requires_key'] else 'no':<5} "
            f"{'yes' if s['has_data'] else 'no':<6} "
            f"{s['rows'] if s['has_data'] else '-':>8}"
        )
