from __future__ import annotations

import pandas as pd

from signal_noise.cli import _parse_excludes, _select_collectors
from signal_noise.cli_commands import _collector_list_rows, _prepare_scheduler_targets
from signal_noise.collector import COLLECTORS
from signal_noise.collector.base import CollectorMeta
from signal_noise.store.sqlite_store import SignalStore


class _DailyCollector:
    meta = CollectorMeta(
        name="daily_demo",
        display_name="Daily Demo",
        update_frequency="daily",
        api_docs_url="https://example.com/daily",
        domain="markets",
        category="crypto",
    )


class _StreamingCollector:
    meta = CollectorMeta(
        name="stream_demo",
        display_name="Stream Demo",
        update_frequency="hourly",
        api_docs_url="https://example.com/stream",
        domain="technology",
        category="internet",
        signal_type="scalar",
    )


def test_parse_excludes_csv():
    assert _parse_excludes("a,b, c ,,") == {"a", "b", "c"}
    assert _parse_excludes("") == set()
    assert _parse_excludes(None) == set()


def test_select_collectors_respects_exclude():
    first = next(iter(COLLECTORS.keys()))
    selected = _select_collectors(exclude={first})
    assert first not in selected
    assert len(selected) == len(COLLECTORS) - 1


def test_collector_list_rows_uses_combined_store_counts(tmp_path):
    store = SignalStore(tmp_path / "test.db")
    store.save(
        "daily_demo",
        pd.DataFrame({"timestamp": ["2024-01-01", "2024-01-02"], "value": [1.0, 2.0]}),
    )
    store.save_realtime(
        "stream_demo",
        pd.DataFrame({"timestamp": ["2024-01-01T00:00:00Z"], "value": [3.0]}),
    )

    rows = _collector_list_rows(
        store,
        {
            "daily_demo": _DailyCollector,
            "stream_demo": _StreamingCollector,
        },
    )
    by_name = {row["display_name"]: row for row in rows}

    assert by_name["Daily Demo"]["has_data"] is True
    assert by_name["Daily Demo"]["rows"] == 2
    assert by_name["Stream Demo"]["has_data"] is True
    assert by_name["Stream Demo"]["rows"] == 1

    store.close()


def test_prepare_scheduler_targets_syncs_registry_suppressed(tmp_path, monkeypatch):
    store = SignalStore(tmp_path / "test.db")
    registry = {
        "daily_demo": _DailyCollector,
        "stream_demo": _StreamingCollector,
    }

    registry_path = tmp_path / "suppressions.toml"
    registry_path.write_text(
        """
[[rules]]
selectors = ["daily_demo"]
scopes = ["tests"]
reason_code = "missing_api_key"
detail = "Demo key not configured."
review_after = "2026-04-15"
""".strip()
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("SIGNAL_NOISE_SUPPRESSION_FILE", str(registry_path))
    monkeypatch.setenv("SIGNAL_NOISE_SUPPRESSION_SCOPE", "tests")
    targets, excludes = _prepare_scheduler_targets(store, registry)

    assert targets == {"stream_demo": _StreamingCollector}
    assert excludes == {"daily_demo"}
    daily_meta = store.get_meta("daily_demo")

    assert daily_meta["suppressed"] == 1
    assert daily_meta["suppressed_reason"] == "missing_api_key"
    assert daily_meta["suppressed_detail"] == "Demo key not configured."
    assert daily_meta["suppressed_scope"] == "tests"
    assert daily_meta["suppressed_review_after"] == "2026-04-15"
    assert daily_meta["suppressed_source"] == "registry"
    assert daily_meta["suppressed_at"] is not None

    store.close()


def test_prepare_scheduler_targets_syncs_runtime_overrides(tmp_path, monkeypatch):
    store = SignalStore(tmp_path / "test.db")
    registry = {
        "daily_demo": _DailyCollector,
        "stream_demo": _StreamingCollector,
    }

    monkeypatch.setenv("SIGNAL_NOISE_EXCLUDE", "daily_demo")
    targets, excludes = _prepare_scheduler_targets(store, registry, exclude="stream_demo")

    assert targets == {}
    assert excludes == {"daily_demo", "stream_demo"}

    daily_meta = store.get_meta("daily_demo")
    stream_meta = store.get_meta("stream_demo")

    assert daily_meta["suppressed"] == 1
    assert daily_meta["suppressed_reason"] == "legacy_env_override"
    assert daily_meta["suppressed_detail"] == "Explicit SIGNAL_NOISE_EXCLUDE runtime override."
    assert daily_meta["suppressed_scope"] == "runtime"
    assert daily_meta["suppressed_source"] == "env"
    assert daily_meta["suppressed_at"] is not None

    assert stream_meta["suppressed"] == 1
    assert stream_meta["suppressed_reason"] == "cli_override"
    assert stream_meta["suppressed_detail"] == "Explicit --exclude runtime override."
    assert stream_meta["suppressed_scope"] == "runtime"
    assert stream_meta["suppressed_source"] == "cli"
    assert stream_meta["suppressed_at"] is not None

    store.close()


def test_prepare_scheduler_targets_keeps_registry_suppression_for_legacy_meta(
    tmp_path,
    monkeypatch,
):
    store = SignalStore(tmp_path / "test.db")
    store.save_meta("legacy_demo", "society", "policy", 86400)
    registry = {
        "daily_demo": _DailyCollector,
        "stream_demo": _StreamingCollector,
    }

    registry_path = tmp_path / "suppressions.toml"
    registry_path.write_text(
        """
[[rules]]
selectors = ["legacy_demo"]
scopes = ["tests"]
reason_code = "source_removed"
detail = "Legacy signal kept suppressed while meta remains."
""".strip()
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("SIGNAL_NOISE_SUPPRESSION_FILE", str(registry_path))
    monkeypatch.setenv("SIGNAL_NOISE_SUPPRESSION_SCOPE", "tests")

    targets, excludes = _prepare_scheduler_targets(store, registry)

    assert targets == registry
    assert excludes == {"legacy_demo"}
    legacy_meta = store.get_meta("legacy_demo")
    assert legacy_meta["suppressed"] == 1
    assert legacy_meta["suppressed_reason"] == "source_removed"
    assert legacy_meta["suppressed_source"] == "registry"

    store.close()
