"""Tests for signals_realtime table and realtime store methods."""
from __future__ import annotations

import pandas as pd
import pytest

from signal_noise.store.sqlite_store import SignalStore


@pytest.fixture
def store(tmp_path):
    s = SignalStore(tmp_path / "test.db")
    yield s
    s.close()


def _make_df(timestamps, values):
    return pd.DataFrame({"timestamp": timestamps, "value": values})


def test_save_and_get_realtime(store):
    df = _make_df(
        ["2026-03-01T10:00:00Z", "2026-03-01T10:01:00Z"],
        [0.5, 0.6],
    )
    n = store.save_realtime("test_sig", df)
    assert n == 2

    result = store.get_realtime_data("test_sig")
    assert len(result) == 2
    assert result.iloc[0]["value"] == pytest.approx(0.5)


def test_realtime_upsert(store):
    df1 = _make_df(["2026-03-01T10:00:00Z"], [0.5])
    store.save_realtime("sig", df1)

    df2 = _make_df(["2026-03-01T10:00:00Z"], [0.9])
    store.save_realtime("sig", df2)

    result = store.get_realtime_data("sig")
    assert len(result) == 1
    assert result.iloc[0]["value"] == pytest.approx(0.9)


def test_get_realtime_since(store):
    df = _make_df(
        ["2026-03-01T10:00:00Z", "2026-03-01T11:00:00Z", "2026-03-01T12:00:00Z"],
        [1.0, 2.0, 3.0],
    )
    store.save_realtime("sig", df)

    result = store.get_realtime_data("sig", since="2026-03-01T11:00:00Z")
    assert len(result) == 2
    assert result.iloc[0]["value"] == pytest.approx(2.0)


def test_get_realtime_latest(store):
    df = _make_df(
        ["2026-03-01T10:00:00Z", "2026-03-01T10:01:00Z"],
        [0.5, 0.8],
    )
    store.save_realtime("sig", df)

    latest = store.get_realtime_latest("sig")
    assert latest is not None
    assert latest["value"] == pytest.approx(0.8)


def test_get_realtime_latest_empty(store):
    assert store.get_realtime_latest("nonexistent") is None


def test_save_realtime_collection_result(store):
    df = _make_df(["2026-03-01T10:00:00Z"], [0.42])
    n = store.save_realtime_collection_result(
        "rt_sig", df, "markets", "microstructure", 60, "scalar",
    )
    assert n == 1

    meta = store.get_meta("rt_sig")
    assert meta is not None
    assert meta["category"] == "microstructure"
    assert meta["interval"] == 60

    audit = store.get_audit_log("rt_sig", limit=1)
    assert len(audit) == 1
    assert audit[0]["event"] == "collected"


def test_rollup_daily(store):
    df = _make_df(
        [
            "2026-03-01T10:00:00Z", "2026-03-01T10:01:00Z",
            "2026-03-01T10:02:00Z", "2026-03-02T10:00:00Z",
        ],
        [1.0, 2.0, 3.0, 10.0],
    )
    store.save_realtime("sig", df)

    n = store.rollup_daily("sig")
    assert n == 2

    # Check daily values in signals table
    data = store.get_data("sig")
    assert len(data) == 2
    # Day 1: mean of [1.0, 2.0, 3.0] = 2.0
    assert data.iloc[0]["value"] == pytest.approx(2.0)
    # Day 2: mean of [10.0] = 10.0
    assert data.iloc[1]["value"] == pytest.approx(10.0)


def test_purge_realtime(store):
    df = _make_df(
        ["2020-01-01T00:00:00Z", "2026-03-01T10:00:00Z"],
        [1.0, 2.0],
    )
    store.save_realtime("sig", df)

    deleted = store.purge_realtime(days=30)
    assert deleted == 1

    remaining = store.get_realtime_data("sig")
    assert len(remaining) == 1
    assert remaining.iloc[0]["value"] == pytest.approx(2.0)
