from __future__ import annotations

import argparse
import logging

log = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="signal-noise",
        description="Collect worldwide time series and deliver via REST API",
    )
    sub = parser.add_subparsers(dest="command")

    p_collect = sub.add_parser("collect", help="Fetch data from collectors")
    p_collect.add_argument("--collector", "-s", help="Specific collector name")
    p_collect.add_argument("--frequency", "-f", help="Filter by update frequency (hourly/daily/weekly/monthly)")
    p_collect.add_argument("--force", action="store_true", help="Ignore cache")
    p_collect.add_argument("--parquet", action="store_true", help="Use legacy Parquet storage")

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
        "--min-rows", type=int, default=200,
        help="Minimum data points per signal (default: 200)",
    )
    p_spectrum.add_argument(
        "--components", "-k", type=int, default=8,
        help="Number of principal components to show (default: 8)",
    )
    p_spectrum.add_argument(
        "--json", action="store_true", help="Output as JSON",
    )

    p_quality = analyze_sub.add_parser("quality", help="Signal quality & health analysis")
    p_quality.add_argument(
        "--days", type=int, default=90,
        help="Analysis window in days (default: 90)",
    )
    p_quality.add_argument(
        "--domain", help="Filter by domain",
    )
    p_quality.add_argument(
        "--json", action="store_true", help="Output as JSON",
    )

    p_coverage = sub.add_parser("coverage", help="Show signal coverage summary")
    p_coverage.add_argument(
        "--json", action="store_true", help="Output as JSON",
    )

    sub.add_parser("rebuild-manifest", help="Rebuild collector discovery manifest")

    p_serve = sub.add_parser("serve", help="Start scheduler + REST API")
    p_serve.add_argument("--host", default="0.0.0.0", help="API bind host")
    p_serve.add_argument("--port", type=int, default=8000, help="API bind port")
    p_serve.add_argument(
        "--no-scheduler", action="store_true", help="API only, skip collector scheduling"
    )
    p_serve.add_argument(
        "--migrate", action="store_true", help="Import existing Parquet files before starting"
    )

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if args.command == "collect":
        _cmd_collect(args)
    elif args.command == "backfill":
        _cmd_backfill(args)
    elif args.command == "list":
        _cmd_list()
    elif args.command == "count":
        _cmd_count()
    elif args.command == "analyze":
        if args.analyze_command == "spectrum":
            _cmd_spectrum(args)
        elif args.analyze_command == "quality":
            _cmd_quality(args)
        else:
            p_analyze.print_help()
    elif args.command == "coverage":
        _cmd_coverage(args)
    elif args.command == "rebuild-manifest":
        _cmd_rebuild_manifest()
    elif args.command == "serve":
        _cmd_serve(args)
    else:
        parser.print_help()


def _cmd_collect(args: argparse.Namespace) -> None:
    from signal_noise.collector import COLLECTORS, collect_all
    from signal_noise.config import CACHE_DIR, DB_PATH
    from signal_noise.store.sqlite_store import SignalStore

    store = None if args.parquet else SignalStore(DB_PATH)

    if args.collector:
        collectors = [args.collector]
    elif args.frequency:
        collectors = [
            name for name, cls in COLLECTORS.items()
            if cls().meta.update_frequency == args.frequency
        ]
        log.info("Filtered to %d %s collectors", len(collectors), args.frequency)
    else:
        collectors = None
    if args.force:
        for name in collectors or COLLECTORS.keys():
            cache = CACHE_DIR / f"{name}.json"
            if cache.exists():
                cache.unlink()
    results = collect_all(collectors, store=store)
    for name, df in results.items():
        print(f"  {name}: {len(df)} rows")

    if store:
        store.close()


def _cmd_backfill(args: argparse.Namespace) -> None:
    from signal_noise.collector import COLLECTORS
    from signal_noise.config import CACHE_DIR, DB_PATH
    from signal_noise.store.sqlite_store import SignalStore

    store = SignalStore(DB_PATH)
    total = args.total

    targets: list[str] = []
    if args.collector:
        targets = [args.collector]
    elif args.category:
        for name, cls in COLLECTORS.items():
            if cls().meta.category == args.category:
                targets.append(name)
    else:
        targets = list(COLLECTORS.keys())

    if not targets:
        print("No collectors matched.")
        store.close()
        return

    print(f"Backfilling {len(targets)} collectors (total={total})...")
    for name in targets:
        cls = COLLECTORS.get(name)
        if not cls:
            log.warning("Unknown collector: %s", name)
            continue
        try:
            collector = cls(total=total)
        except TypeError:
            collector = cls()
        # Clear cache to force fresh fetch
        cache = CACHE_DIR / f"{name}.json"
        if cache.exists():
            cache.unlink()
        try:
            df = collector.collect(store=store)
            print(f"  {name}: {len(df)} rows")
        except Exception as e:
            log.warning("Failed to backfill %s: %s", name, e)

    store.close()


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


def _classify_level(name: str, meta) -> str:
    """Classify collector into Collection Spectrum level (L1-L7)."""
    if name.startswith("probe_"):
        return "L5"
    if meta.domain == "computed":
        return "L4"
    if meta.requires_key:
        return "L2"
    return "L1"


def _cmd_coverage(args: argparse.Namespace) -> None:
    import json as json_mod
    from collections import Counter

    from signal_noise.collector import COLLECTORS

    domain_freq: Counter[tuple[str, str]] = Counter()
    domain_count: Counter[str] = Counter()
    category_count: Counter[str] = Counter()
    level_count: Counter[str] = Counter()
    freq_count: Counter[str] = Counter()

    for name, cls in COLLECTORS.items():
        m = cls.meta
        domain_freq[(m.domain, m.update_frequency)] += 1
        domain_count[m.domain] += 1
        category_count[m.category] += 1
        freq_count[m.update_frequency] += 1
        level_count[_classify_level(name, m)] += 1

    total = len(COLLECTORS)

    if args.json:
        data = {
            "total": total,
            "by_domain": dict(domain_count.most_common()),
            "by_category": dict(category_count.most_common()),
            "by_frequency": dict(freq_count.most_common()),
            "by_level": {k: level_count[k] for k in sorted(level_count)},
            "domain_x_frequency": {
                f"{d}:{f}": c for (d, f), c in sorted(domain_freq.items())
            },
        }
        print(json_mod.dumps(data, indent=2))
        return

    # --- Domain × Frequency matrix ---
    domains = sorted(domain_count, key=lambda d: -domain_count[d])
    freqs = ["hourly", "daily", "weekly", "monthly", "quarterly", "yearly"]
    freqs = [f for f in freqs if freq_count.get(f, 0) > 0]

    print(f"Signal Coverage: {total} signals\n")
    print("── Domain × Frequency ──\n")

    hdr = f"{'Domain':<16}" + "".join(f"{f:>10}" for f in freqs) + f"{'Total':>10}"
    print(hdr)
    print("-" * len(hdr))
    for d in domains:
        row = f"{d:<16}"
        for f in freqs:
            c = domain_freq.get((d, f), 0)
            row += f"{c or '.':>10}"
        row += f"{domain_count[d]:>10}"
        print(row)
    # totals row
    row = f"{'Total':<16}"
    for f in freqs:
        row += f"{freq_count[f]:>10}"
    row += f"{total:>10}"
    print("-" * len(hdr))
    print(row)

    # --- Collection Level ---
    print("\n── Collection Level ──\n")
    level_labels = {
        "L1": "Free API",
        "L2": "Auth API",
        "L3": "Scraping",
        "L4": "Proxy/computed",
        "L5": "Active probing",
        "L6": "Physical sensors",
        "L7": "Undefined",
    }
    for lv in ["L1", "L2", "L3", "L4", "L5", "L6", "L7"]:
        c = level_count.get(lv, 0)
        if c > 0:
            pct = c * 100 / total
            bar = "#" * int(pct / 2)
            print(f"  {lv} {level_labels[lv]:<20} {c:>6}  ({pct:4.1f}%)  {bar}")

    # --- Top categories ---
    print("\n── Top Categories ──\n")
    for cat, c in category_count.most_common(15):
        pct = c * 100 / total
        print(f"  {cat:<20} {c:>6}  ({pct:4.1f}%)")
    if len(category_count) > 15:
        print(f"  ... and {len(category_count) - 15} more")


def _cmd_rebuild_manifest() -> None:
    from signal_noise.collector._manifest import build_manifest

    manifest = build_manifest()
    print(f"Manifest rebuilt: {len(manifest['collectors'])} collectors")


def _cmd_spectrum(args: argparse.Namespace) -> None:
    import json as json_mod

    from signal_noise.analysis.spectrum import compute_spectrum
    from signal_noise.config import DB_PATH
    from signal_noise.store.sqlite_store import SignalStore

    store = SignalStore(DB_PATH)
    result = compute_spectrum(
        store,
        min_rows=args.min_rows,
        n_components=args.components,
    )
    store.close()

    if args.json:
        data = {
            "n_signals": result.n_signals,
            "n_dates": result.n_dates,
            "effective_dims": result.effective_dims,
            "participation_ratio": result.participation_ratio,
            "spectral_entropy": result.spectral_entropy,
            "spectral_entropy_normalized": result.spectral_entropy_normalized,
            "components": [
                {
                    "index": pc.index,
                    "variance_ratio": pc.variance_ratio,
                    "cumulative_variance": pc.cumulative_variance,
                    "top_signals": [
                        {"name": n, "loading": v} for n, v in pc.top_signals
                    ],
                    "domain_composition": pc.domain_composition,
                }
                for pc in result.components
            ],
            "redundant": [
                {"name": s.name, "domain": s.domain, "category": s.category,
                 "uniqueness": s.uniqueness}
                for s in result.redundant
            ],
            "unique": [
                {"name": s.name, "domain": s.domain, "category": s.category,
                 "uniqueness": s.uniqueness}
                for s in result.unique
            ],
        }
        print(json_mod.dumps(data, indent=2))
    else:
        print(result.summary())


def _cmd_quality(args: argparse.Namespace) -> None:
    import json as json_mod

    from signal_noise.analysis.quality import compute_quality
    from signal_noise.config import DB_PATH
    from signal_noise.store.sqlite_store import SignalStore

    store = SignalStore(DB_PATH)
    result = compute_quality(store, days=args.days, domain=args.domain)
    store.close()

    if args.json:
        data = {
            "n_signals": result.n_signals,
            "n_healthy": result.n_healthy,
            "n_degraded": result.n_degraded,
            "n_poor": result.n_poor,
            "signals": [
                {
                    "name": sq.name,
                    "domain": sq.domain,
                    "category": sq.category,
                    "completeness": sq.completeness,
                    "freshness": sq.freshness,
                    "stability": sq.stability,
                    "independence": sq.independence,
                    "health_score": sq.health_score,
                }
                for sq in result.signals
            ],
        }
        print(json_mod.dumps(data, indent=2))
    else:
        print(result.summary())


def _cmd_serve(args: argparse.Namespace) -> None:
    import asyncio

    import uvicorn

    from signal_noise.config import DB_PATH, RAW_DIR
    from signal_noise.store.sqlite_store import SignalStore

    store = SignalStore(DB_PATH)

    if args.migrate:
        from signal_noise.collector import COLLECTORS
        from signal_noise.store.migration import migrate_parquet_to_sqlite

        count = migrate_parquet_to_sqlite(RAW_DIR, store, COLLECTORS)
        log.info("Migrated %d signals from Parquet", count)

    import signal_noise.api.app as api_mod

    api_mod._store = store

    async def _run() -> None:
        tasks = []
        if not args.no_scheduler:
            from signal_noise.scheduler.loop import run_scheduler

            tasks.append(asyncio.create_task(run_scheduler(store)))

        config = uvicorn.Config(
            api_mod.app,
            host=args.host,
            port=args.port,
            log_level="info",
        )
        server = uvicorn.Server(config)
        tasks.append(asyncio.create_task(server.serve()))

        await asyncio.gather(*tasks)

    asyncio.run(_run())
