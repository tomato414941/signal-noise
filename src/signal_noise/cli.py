from __future__ import annotations

import argparse
import logging


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="signal-noise",
        description="Collect worldwide signals and evaluate predictive power",
    )
    sub = parser.add_subparsers(dest="command")

    p_collect = sub.add_parser("collect", help="Fetch data from collectors")
    p_collect.add_argument("--collector", "-s", help="Specific collector name")
    p_collect.add_argument("--force", action="store_true", help="Ignore cache")

    p_eval = sub.add_parser("evaluate", help="Run signal evaluation pipeline")
    p_eval.add_argument("--target", "-t", default="btc_ohlcv", help="Target collector")
    p_eval.add_argument("--period", "-p", help="Specific return period (e.g. 1d)")
    p_eval.add_argument("--no-transforms", action="store_true", help="Raw signals only")
    p_eval.add_argument("--top", type=int, help="Show only top N signals")

    sub.add_parser("report", help="Show latest evaluation report")
    sub.add_parser("list", help="List available collectors with status")
    sub.add_parser("count", help="Show total signal count")

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
    elif args.command == "count":
        _cmd_count()
    else:
        parser.print_help()


def _cmd_collect(args: argparse.Namespace) -> None:
    from signal_noise.collector import COLLECTORS, collect_all
    from signal_noise.config import CACHE_DIR

    collectors = [args.collector] if args.collector else None
    if args.force:
        for name in collectors or COLLECTORS.keys():
            cache = CACHE_DIR / f"{name}.json"
            if cache.exists():
                cache.unlink()
    results = collect_all(collectors)
    for name, df in results.items():
        print(f"  {name}: {len(df)} rows")


def _cmd_evaluate(args: argparse.Namespace) -> None:
    from signal_noise.config import EvaluationConfig
    from signal_noise.evaluator.pipeline import run_evaluation
    from signal_noise.reporter.report import generate_report

    config = EvaluationConfig()
    if args.period:
        config.return_periods = [args.period]
    metrics = run_evaluation(
        config,
        target_collector=args.target,
        use_transforms=not args.no_transforms,
    )
    top_n = args.top if hasattr(args, "top") and args.top else None
    print(generate_report(metrics, top_n=top_n))


def _cmd_report() -> None:
    from signal_noise.config import REPORTS_DIR

    txt = REPORTS_DIR / "evaluation.txt"
    if txt.exists():
        print(txt.read_text())
    else:
        print("No report found. Run: python -m signal_noise evaluate")


def _cmd_list() -> None:
    from signal_noise.collector import COLLECTORS

    cols = ("Collector", "Category", "Freq", "Key?", "Data?", "Rows")
    print(f"Total collectors: {len(COLLECTORS)}")
    print()
    print(f"{cols[0]:<25} {cols[1]:<15} {cols[2]:<10} {cols[3]:<5} {cols[4]:<6} {cols[5]:>8}")
    print("-" * 75)
    for name, cls in COLLECTORS.items():
        c = cls()
        s = c.status()
        ks = "yes" if s["requires_key"] else "no"
        ds = "yes" if s["has_data"] else "no"
        rs = str(s["rows"]) if s["has_data"] else "-"
        dn = s["display_name"]
        dt = c.meta.category
        uf = c.meta.update_frequency
        print(f"{dn:<25} {dt:<15} {uf:<10} {ks:<5} {ds:<6} {rs:>8}")


def _cmd_count() -> None:
    from signal_noise.collector import COLLECTORS
    from signal_noise.transforms import TRANSFORMS

    n_collectors = len(COLLECTORS) - 1  # exclude target
    n_transforms = len(TRANSFORMS)
    raw_signals = n_collectors
    derived_signals = n_collectors * n_transforms
    total = raw_signals + derived_signals

    print(f"Collectors:        {n_collectors + 1}")
    print(f"Transforms:        {n_transforms}")
    print(f"Raw signals:       {raw_signals}")
    print(f"Derived signals:   {derived_signals}")
    print(f"Total signals:     {total}")
