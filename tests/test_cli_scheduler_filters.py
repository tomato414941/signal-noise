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


class _FakeRegistry(dict):
    def __init__(self, mapping: dict, manifest_entries: dict[str, dict]) -> None:
        super().__init__(mapping)
        self._manifest_entries = manifest_entries

    def get_manifest_entry(self, name: str) -> dict | None:
        return self._manifest_entries.get(name)


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


def test_prepare_scheduler_targets_syncs_suppressed(tmp_path, monkeypatch):
    store = SignalStore(tmp_path / "test.db")
    registry = _FakeRegistry(
        {
            "daily_demo": _DailyCollector,
            "stream_demo": _StreamingCollector,
        },
        {
            "daily_demo": {
                "meta": {
                    "display_name": "Daily Demo",
                    "domain": "markets",
                    "category": "crypto",
                    "update_frequency": "daily",
                    "requires_key": False,
                    "signal_type": "scalar",
                    "collection_level": "",
                    "interval": 86400,
                }
            },
            "stream_demo": {
                "meta": {
                    "display_name": "Stream Demo",
                    "domain": "technology",
                    "category": "internet",
                    "update_frequency": "hourly",
                    "requires_key": False,
                    "signal_type": "scalar",
                    "collection_level": "",
                    "interval": 3600,
                }
            },
        },
    )

    monkeypatch.setenv("SIGNAL_NOISE_EXCLUDE", "daily_demo")
    targets, excludes = _prepare_scheduler_targets(store, registry, exclude="stream_demo")

    assert targets == {}
    assert excludes == {"daily_demo", "stream_demo"}
    assert store.get_meta("daily_demo")["suppressed"] == 1
    assert store.get_meta("stream_demo")["suppressed"] == 1

    store.close()
