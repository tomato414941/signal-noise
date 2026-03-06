from __future__ import annotations

import argparse
import logging

from signal_noise import cli_commands as _cli_commands
from signal_noise.cli_commands import (
    _cmd_backfill,
    _cmd_collect,
    _cmd_count,
    _cmd_coverage,
    _cmd_list,
    _cmd_quality,
    _cmd_rebuild_manifest,
    _cmd_rollup_realtime,
    _cmd_scheduler,
    _cmd_serve,
    _cmd_spectrum,
)

_parse_excludes = _cli_commands._parse_excludes
_select_collectors = _cli_commands._select_collectors


def _build_parser() -> tuple[argparse.ArgumentParser, argparse.ArgumentParser]:
    parser = argparse.ArgumentParser(
        prog="signal-noise",
        description="Collect worldwide time series and deliver via REST API",
    )
    sub = parser.add_subparsers(dest="command")

    p_collect = sub.add_parser("collect", help="Fetch data from collectors")
    p_collect.add_argument("--collector", "-s", help="Specific collector name")
    p_collect.add_argument(
        "--frequency",
        "-f",
        help="Filter by update frequency (hourly/daily/weekly/monthly)",
    )
    p_collect.add_argument("--force", action="store_true", help="Ignore cache")

    sub.add_parser("list", help="List available collectors with status")
    sub.add_parser("count", help="Show collector count")

    p_backfill = sub.add_parser("backfill", help="Fetch extended historical data")
    p_backfill.add_argument("--collector", "-s", help="Specific collector name")
    p_backfill.add_argument("--category", "-c", help="Collector category (e.g. crypto)")
    p_backfill.add_argument("--total", "-t", type=int, default=20000, help="Total rows to fetch")

    p_analyze = sub.add_parser("analyze", help="Signal analysis tools")
    analyze_sub = p_analyze.add_subparsers(dest="analyze_command")

    p_spectrum = analyze_sub.add_parser("spectrum", help="SVD spectral analysis of signal coverage")
    p_spectrum.add_argument(
        "--min-rows",
        type=int,
        default=200,
        help="Minimum data points per signal (default: 200)",
    )
    p_spectrum.add_argument(
        "--components",
        "-k",
        type=int,
        default=8,
        help="Number of principal components to show (default: 8)",
    )
    p_spectrum.add_argument("--json", action="store_true", help="Output as JSON")

    p_quality = analyze_sub.add_parser("quality", help="Signal quality & health analysis")
    p_quality.add_argument(
        "--days",
        type=int,
        default=90,
        help="Analysis window in days (default: 90)",
    )
    p_quality.add_argument("--domain", help="Filter by domain")
    p_quality.add_argument("--json", action="store_true", help="Output as JSON")

    p_coverage = sub.add_parser("coverage", help="Show signal coverage summary")
    p_coverage.add_argument("--json", action="store_true", help="Output as JSON")

    sub.add_parser("rebuild-manifest", help="Rebuild collector discovery manifest")

    p_scheduler = sub.add_parser("scheduler", help="Run collection scheduler (long-lived)")
    p_scheduler.add_argument("--frequency", "-f", help="Only schedule collectors of this frequency")
    p_scheduler.add_argument(
        "--level",
        "-l",
        help="Only schedule collectors of this collection level (e.g. L5)",
    )
    p_scheduler.add_argument(
        "--exclude",
        type=str,
        default="",
        help="Comma-separated collector names to disable",
    )
    p_scheduler.add_argument(
        "--fetch-timeout",
        type=float,
        default=90.0,
        help="Per-collector fetch timeout in seconds (default: 90)",
    )

    p_rollup = sub.add_parser("rollup-realtime", help="Roll up realtime signals to daily + purge")
    p_rollup.add_argument("--days", type=int, default=30, help="Retention days for realtime data")

    p_serve = sub.add_parser("serve", help="Start scheduler + REST API")
    p_serve.add_argument("--host", default="0.0.0.0", help="API bind host")
    p_serve.add_argument("--port", type=int, default=8000, help="API bind port")
    p_serve.add_argument(
        "--no-scheduler",
        action="store_true",
        help="API only, skip collector scheduling",
    )
    p_serve.add_argument(
        "--migrate",
        action="store_true",
        help="Import existing Parquet files before starting",
    )
    p_serve.add_argument(
        "--exclude",
        type=str,
        default="",
        help="Comma-separated collector names to disable from scheduler",
    )

    return parser, p_analyze


def main(argv: list[str] | None = None) -> None:
    parser, analyze_parser = _build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    command_handlers = {
        "collect": lambda: _cmd_collect(args),
        "backfill": lambda: _cmd_backfill(args),
        "list": _cmd_list,
        "count": _cmd_count,
        "coverage": lambda: _cmd_coverage(args),
        "rebuild-manifest": _cmd_rebuild_manifest,
        "scheduler": lambda: _cmd_scheduler(args),
        "rollup-realtime": lambda: _cmd_rollup_realtime(args),
        "serve": lambda: _cmd_serve(args),
    }

    if args.command == "analyze":
        if args.analyze_command == "spectrum":
            _cmd_spectrum(args)
        elif args.analyze_command == "quality":
            _cmd_quality(args)
        else:
            analyze_parser.print_help()
        return

    handler = command_handlers.get(args.command)
    if handler is None:
        parser.print_help()
        return
    handler()
