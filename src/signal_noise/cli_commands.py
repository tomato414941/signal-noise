from __future__ import annotations

import argparse
import logging
import os
from collections import Counter
from typing import TYPE_CHECKING

from signal_noise.collector._registry import CollectorRegistry, ensure_registry
from signal_noise.suppression_registry import resolve_suppressions

if TYPE_CHECKING:
    from signal_noise.collector.base import BaseCollector
    from signal_noise.store.sqlite_store import SignalStore

log = logging.getLogger(__name__)


def _classify_level(name: str, meta: dict[str, object]) -> str:
    """Classify collector into Collection Spectrum level (L1-L7)."""
    collection_level = str(meta.get("collection_level") or "")
    if collection_level:
        return collection_level
    if name.startswith("probe_"):
        return "L5"
    if meta.get("requires_key"):
        return "L2"
    return "L1"


def _parse_excludes(raw: str | None) -> set[str]:
    if not raw:
        return set()
    return {v.strip() for v in raw.split(",") if v.strip()}


def _join_unique(*values: str | None) -> str | None:
    parts: list[str] = []
    for value in values:
        if not value:
            continue
        for part in str(value).split("+"):
            normalized = part.strip()
            if normalized and normalized not in parts:
                parts.append(normalized)
    return "+".join(parts) if parts else None


def _join_detail(*values: str | None) -> str | None:
    parts: list[str] = []
    for value in values:
        normalized = str(value or "").strip()
        if normalized and normalized not in parts:
            parts.append(normalized)
    return " ".join(parts) if parts else None


def _build_override_suppressed_entries(
    names: set[str],
    *,
    source: str,
    reason: str,
    detail: str,
    scope: str = "runtime",
) -> dict[str, dict[str, str]]:
    return {
        name: {
            "source": source,
            "reason": reason,
            "detail": detail,
            "scope": scope,
        }
        for name in sorted(names)
    }


def _merge_suppressed_entries(
    *entry_sets: dict[str, dict[str, str | None]],
) -> dict[str, dict[str, str | None]]:
    merged: dict[str, dict[str, str | None]] = {}
    for entry_set in entry_sets:
        for name, detail in entry_set.items():
            current = merged.get(name)
            if current is None:
                merged[name] = dict(detail)
                continue

            current["source"] = _join_unique(current.get("source"), detail.get("source"))
            current["scope"] = _join_unique(current.get("scope"), detail.get("scope"))
            current["detail"] = _join_detail(current.get("detail"), detail.get("detail"))
            if not current.get("review_after") and detail.get("review_after"):
                current["review_after"] = detail.get("review_after")

            current_reason = current.get("reason")
            next_reason = detail.get("reason")
            if next_reason and current_reason not in {next_reason, "legacy_env_override", "cli_override"}:
                current["detail"] = _join_detail(
                    current.get("detail"),
                    f"Additional override active: {next_reason}.",
                )
            elif next_reason and not current_reason:
                current["reason"] = next_reason

            if current_reason in {"legacy_env_override", "cli_override"} and next_reason:
                current["reason"] = next_reason

    return merged


def _sync_suppressed_meta(
    store: SignalStore,
    registry: object,
    suppressed: dict[str, dict[str, str]],
) -> None:
    resolved = ensure_registry(registry)
    if not suppressed:
        store.sync_suppressed({})
        return

    for name, detail in suppressed.items():
        meta = resolved.get_meta(name)
        if meta is None:
            continue
        store.save_meta(
            name,
            str(meta.get("domain", "")),
            str(meta.get("category", "")),
            int(meta.get("interval", 86400)),
            str(meta.get("signal_type", "scalar")),
            suppressed=True,
            suppressed_reason=detail.get("reason"),
            suppressed_detail=detail.get("detail"),
            suppressed_scope=detail.get("scope"),
            suppressed_review_after=detail.get("review_after"),
            suppressed_source=detail.get("source"),
        )
    store.sync_suppressed(suppressed)


def _select_collectors_from_registry(
    registry: CollectorRegistry | dict[str, type[BaseCollector]],
    *,
    frequency: str | None = None,
    level: str | None = None,
    exclude: set[str] | None = None,
) -> dict[str, type[BaseCollector]]:
    resolved = ensure_registry(registry)
    excluded = exclude or set()
    selected: dict[str, type[BaseCollector]] = {}

    for name in resolved.keys():
        if name in excluded:
            continue

        meta = resolved.get_meta(name)
        if meta is None:
            continue
        if frequency and meta.get("update_frequency") != frequency:
            continue
        if level and _classify_level(name, meta) != level:
            continue

        cls = resolved.load(name)
        if cls is None:
            continue
        selected[name] = cls
    return selected


def _select_collectors(
    *,
    frequency: str | None = None,
    level: str | None = None,
    exclude: set[str] | None = None,
) -> dict[str, type[BaseCollector]]:
    from signal_noise.collector import COLLECTORS

    return _select_collectors_from_registry(
        COLLECTORS,
        frequency=frequency,
        level=level,
        exclude=exclude,
    )


def _prepare_scheduler_targets(
    store: SignalStore,
    registry: CollectorRegistry | dict[str, type[BaseCollector]],
    *,
    frequency: str | None = None,
    level: str | None = None,
    exclude: str | None = None,
) -> tuple[dict[str, type[BaseCollector]], set[str]]:
    resolved = ensure_registry(registry)
    known_names = set(resolved.keys())
    known_names.update(signal["name"] for signal in store.list_signals())
    registry_entries = resolve_suppressions(sorted(known_names))
    env_excludes = _parse_excludes(os.getenv("SIGNAL_NOISE_EXCLUDE", ""))
    cli_excludes = _parse_excludes(exclude)
    env_entries = _build_override_suppressed_entries(
        env_excludes,
        source="env",
        reason="legacy_env_override",
        detail="Explicit SIGNAL_NOISE_EXCLUDE runtime override.",
    )
    cli_entries = _build_override_suppressed_entries(
        cli_excludes,
        source="cli",
        reason="cli_override",
        detail="Explicit --exclude runtime override.",
    )
    suppressed_entries = _merge_suppressed_entries(registry_entries, env_entries, cli_entries)
    excludes = set(suppressed_entries)
    _sync_suppressed_meta(store, resolved, suppressed_entries)
    targets = _select_collectors_from_registry(
        resolved,
        frequency=frequency,
        level=level,
        exclude=excludes,
    )
    return targets, excludes


def _collector_list_rows(
    store: SignalStore,
    registry: CollectorRegistry | dict[str, type[BaseCollector]],
) -> list[dict[str, object]]:
    resolved = ensure_registry(registry)
    row_counts = store.get_signal_row_counts()
    rows: list[dict[str, object]] = []

    for name in resolved.keys():
        meta = resolved.get_meta(name)
        if meta is None:
            continue
        row_count = row_counts.get(name, 0)
        rows.append({
            "display_name": str(meta.get("display_name", name)),
            "category": str(meta.get("category", "")),
            "update_frequency": str(meta.get("update_frequency", "")),
            "requires_key": bool(meta.get("requires_key", False)),
            "has_data": row_count > 0,
            "rows": row_count,
        })

    return rows


def _cmd_collect(args: argparse.Namespace) -> None:
    from signal_noise.collector import COLLECTORS, collect_all
    from signal_noise.config import CACHE_DIR, DB_PATH
    from signal_noise.store.sqlite_store import SignalStore

    store = SignalStore(DB_PATH)
    registry = ensure_registry(COLLECTORS)

    if args.collector:
        collectors = [args.collector]
    elif args.frequency:
        collectors = [
            name for name in registry.keys()
            if (registry.get_meta(name) or {}).get("update_frequency") == args.frequency
        ]
        log.info("Filtered to %d %s collectors", len(collectors), args.frequency)
    else:
        collectors = None

    if args.force:
        for name in collectors or registry.keys():
            cache = CACHE_DIR / f"{name}.json"
            if cache.exists():
                cache.unlink()

    results = collect_all(collectors, store=store)
    for name, df in results.items():
        print(f"  {name}: {len(df)} rows")

    store.close()


def _cmd_backfill(args: argparse.Namespace) -> None:
    from signal_noise.collector import COLLECTORS
    from signal_noise.config import CACHE_DIR, DB_PATH
    from signal_noise.store.sqlite_store import SignalStore

    store = SignalStore(DB_PATH)
    total = args.total
    registry = ensure_registry(COLLECTORS)

    targets: list[str] = []
    if args.collector:
        targets = [args.collector]
    elif args.category:
        targets = [
            name for name in registry.keys()
            if (registry.get_meta(name) or {}).get("category") == args.category
        ]
    else:
        targets = list(registry.keys())

    if not targets:
        print("No collectors matched.")
        store.close()
        return

    print(f"Backfilling {len(targets)} collectors (total={total})...")
    for name in targets:
        cls = registry.load(name)
        if not cls:
            log.warning("Unknown collector: %s", name)
            continue
        try:
            collector = cls(total=total)
        except TypeError:
            collector = cls()

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
    from signal_noise.config import DB_PATH
    from signal_noise.store.sqlite_store import SignalStore

    store = SignalStore(DB_PATH)
    rows = _collector_list_rows(store, COLLECTORS)

    cols = ("Collector", "Category", "Freq", "Key?", "Data?", "Rows")
    print(f"Total collectors: {len(COLLECTORS)}")
    print()
    print(f"{cols[0]:<25} {cols[1]:<15} {cols[2]:<10} {cols[3]:<5} {cols[4]:<6} {cols[5]:>8}")
    print("-" * 75)
    for row in rows:
        key_state = "yes" if row["requires_key"] else "no"
        data_state = "yes" if row["has_data"] else "no"
        row_count = str(row["rows"]) if row["has_data"] else "-"
        print(
            f"{row['display_name']:<25} {row['category']:<15} "
            f"{row['update_frequency']:<10} {key_state:<5} {data_state:<6} {row_count:>8}"
        )

    store.close()


def _cmd_count() -> None:
    from signal_noise.collector import COLLECTORS

    print(f"Collectors: {len(COLLECTORS)}")


def _cmd_coverage(args: argparse.Namespace) -> None:
    import json as json_mod

    from signal_noise.collector import COLLECTORS

    registry = ensure_registry(COLLECTORS)
    domain_freq: Counter[tuple[str, str]] = Counter()
    domain_count: Counter[str] = Counter()
    category_count: Counter[str] = Counter()
    level_count: Counter[str] = Counter()
    freq_count: Counter[str] = Counter()

    for name in registry.keys():
        meta = registry.get_meta(name)
        if meta is None:
            continue
        domain = str(meta.get("domain", ""))
        category = str(meta.get("category", ""))
        update_frequency = str(meta.get("update_frequency", ""))
        domain_freq[(domain, update_frequency)] += 1
        domain_count[domain] += 1
        category_count[category] += 1
        freq_count[update_frequency] += 1
        level_count[_classify_level(name, meta)] += 1

    total = len(registry)

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

    domains = sorted(domain_count, key=lambda d: -domain_count[d])
    freqs = ["hourly", "daily", "weekly", "monthly", "quarterly", "yearly"]
    freqs = [f for f in freqs if freq_count.get(f, 0) > 0]

    print(f"Signal Coverage: {total} signals\n")
    print("── Domain × Frequency ──\n")

    hdr = f"{'Domain':<16}" + "".join(f"{f:>10}" for f in freqs) + f"{'Total':>10}"
    print(hdr)
    print("-" * len(hdr))
    for domain in domains:
        row = f"{domain:<16}"
        for frequency in freqs:
            count = domain_freq.get((domain, frequency), 0)
            row += f"{count or '.':>10}"
        row += f"{domain_count[domain]:>10}"
        print(row)

    row = f"{'Total':<16}"
    for frequency in freqs:
        row += f"{freq_count[frequency]:>10}"
    row += f"{total:>10}"
    print("-" * len(hdr))
    print(row)

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
    for level in ["L1", "L2", "L3", "L4", "L5", "L6", "L7"]:
        count = level_count.get(level, 0)
        if count > 0:
            pct = count * 100 / total
            bar = "#" * int(pct / 2)
            print(f"  {level} {level_labels[level]:<20} {count:>6}  ({pct:4.1f}%)  {bar}")

    print("\n── Top Categories ──\n")
    for category, count in category_count.most_common(15):
        pct = count * 100 / total
        print(f"  {category:<20} {count:>6}  ({pct:4.1f}%)")
    if len(category_count) > 15:
        print(f"  ... and {len(category_count) - 15} more")


def _cmd_scheduler(args: argparse.Namespace) -> None:
    import asyncio

    from signal_noise.config import DB_PATH
    from signal_noise.collector import COLLECTORS
    from signal_noise.scheduler.loop import run_scheduler
    from signal_noise.store.sqlite_store import SignalStore

    store = SignalStore(DB_PATH)
    targets, excludes = _prepare_scheduler_targets(
        store,
        COLLECTORS,
        frequency=args.frequency,
        level=args.level,
        exclude=args.exclude,
    )
    log.info(
        "Scheduler: %d collectors selected (%d excluded)",
        len(targets), len(excludes),
    )
    if excludes:
        log.info("Excluded collectors: %s", ", ".join(sorted(excludes)))

    asyncio.run(run_scheduler(
        store, collectors=targets, fetch_timeout=args.fetch_timeout,
    ))


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
                        {"name": name, "loading": value} for name, value in pc.top_signals
                    ],
                    "domain_composition": pc.domain_composition,
                }
                for pc in result.components
            ],
            "redundant": [
                {"name": signal.name, "domain": signal.domain, "category": signal.category,
                 "uniqueness": signal.uniqueness}
                for signal in result.redundant
            ],
            "unique": [
                {"name": signal.name, "domain": signal.domain, "category": signal.category,
                 "uniqueness": signal.uniqueness}
                for signal in result.unique
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
                    "name": signal.name,
                    "domain": signal.domain,
                    "category": signal.category,
                    "completeness": signal.completeness,
                    "freshness": signal.freshness,
                    "stability": signal.stability,
                    "independence": signal.independence,
                    "health_score": signal.health_score,
                }
                for signal in result.signals
            ],
        }
        print(json_mod.dumps(data, indent=2))
    else:
        print(result.summary())


def _cmd_rollup_realtime(args: argparse.Namespace) -> None:
    from signal_noise.config import DB_PATH
    from signal_noise.store.sqlite_store import SignalStore

    store = SignalStore(DB_PATH)
    metas = store.query_meta(interval=60)
    if not metas:
        print("No realtime signals found (interval=60).")
        store.close()
        return

    total_rolled = 0
    for meta in metas:
        rows = store.rollup_daily(meta["name"])
        if rows > 0:
            print(f"  {meta['name']}: {rows} daily rows")
            total_rolled += rows

    deleted = store.purge_realtime(days=args.days)
    print(f"Rolled up {total_rolled} daily rows from {len(metas)} signals.")
    print(f"Purged {deleted} realtime rows older than {args.days} days.")
    store.close()


def _cmd_serve(args: argparse.Namespace) -> None:
    import asyncio

    import uvicorn

    from signal_noise.collector import COLLECTORS
    from signal_noise.config import DB_PATH, RAW_DIR
    from signal_noise.store.sqlite_store import SignalStore

    store = SignalStore(DB_PATH)

    if args.migrate:
        from signal_noise.store.migration import migrate_parquet_to_sqlite

        count = migrate_parquet_to_sqlite(RAW_DIR, store, COLLECTORS)
        log.info("Migrated %d signals from Parquet", count)

    import signal_noise.api.app as api_mod
    from signal_noise.store.event_bus import EventBus

    event_bus = EventBus()
    api_mod._store = store
    api_mod._event_bus = event_bus

    async def _run() -> None:
        tasks = []
        if not args.no_scheduler:
            targets, excludes = _prepare_scheduler_targets(
                store,
                COLLECTORS,
                exclude=args.exclude,
            )
            log.info(
                "Serve scheduler: %d collectors selected (%d excluded)",
                len(targets), len(excludes),
            )
            if excludes:
                log.info("Excluded collectors: %s", ", ".join(sorted(excludes)))

            from signal_noise.scheduler.loop import run_scheduler

            tasks.append(asyncio.create_task(
                run_scheduler(store, collectors=targets, event_bus=event_bus),
            ))

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
