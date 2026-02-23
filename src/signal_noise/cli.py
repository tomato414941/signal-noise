from __future__ import annotations

import argparse
import logging


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="signal-noise",
        description="Collect worldwide time series and deliver via REST API",
    )
    sub = parser.add_subparsers(dest="command")

    p_collect = sub.add_parser("collect", help="Fetch data from collectors")
    p_collect.add_argument("--collector", "-s", help="Specific collector name")
    p_collect.add_argument("--force", action="store_true", help="Ignore cache")

    sub.add_parser("list", help="List available collectors with status")
    sub.add_parser("count", help="Show collector count")

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if args.command == "collect":
        _cmd_collect(args)
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

    print(f"Collectors: {len(COLLECTORS)}")
