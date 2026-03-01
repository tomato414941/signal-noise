from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from signal_noise.store.sqlite_store import SignalStore


@pytest.fixture
def store(tmp_path: Path) -> SignalStore:
    s = SignalStore(tmp_path / "test.db")
    yield s
    s.close()


class TestSignalStore:
    def test_create_tables(self, store: SignalStore) -> None:
        tables = store._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        names = [t[0] for t in tables]
        assert "signals" in names
        assert "signal_meta" in names

    def test_wal_mode(self, store: SignalStore) -> None:
        mode = store._conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "wal"

    def test_save_and_get_data(self, store: SignalStore) -> None:
        df = pd.DataFrame({"timestamp": ["2024-01-01", "2024-01-02"], "value": [1.0, 2.0]})
        store.save("test_signal", df)
        result = store.get_data("test_signal")
        assert len(result) == 2
        assert list(result.columns) == ["timestamp", "value"]

    def test_save_upsert(self, store: SignalStore) -> None:
        df1 = pd.DataFrame({"timestamp": ["2024-01-01"], "value": [1.0]})
        df2 = pd.DataFrame({"timestamp": ["2024-01-01"], "value": [9.0]})
        store.save("s", df1)
        store.save("s", df2)
        result = store.get_data("s")
        assert len(result) == 1
        assert result.iloc[0]["value"] == 9.0

    def test_get_data_since(self, store: SignalStore) -> None:
        df = pd.DataFrame({
            "timestamp": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "value": [1.0, 2.0, 3.0],
        })
        store.save("s", df)
        result = store.get_data("s", since="2024-01-02")
        assert len(result) == 2

    def test_get_latest(self, store: SignalStore) -> None:
        df = pd.DataFrame({"timestamp": ["2024-01-01", "2024-01-03", "2024-01-02"], "value": [1.0, 3.0, 2.0]})
        store.save("s", df)
        latest = store.get_latest("s")
        assert latest is not None
        assert latest["timestamp"] == "2024-01-03"
        assert latest["value"] == 3.0

    def test_get_latest_empty(self, store: SignalStore) -> None:
        assert store.get_latest("nonexistent") is None

    def test_save_meta_and_list(self, store: SignalStore) -> None:
        store.save_meta("btc", "financial", "crypto", 3600)
        store.save_meta("sp500", "financial", "equity", 86400)
        signals = store.list_signals()
        assert len(signals) == 2
        assert signals[0]["name"] == "btc"
        assert signals[0]["domain"] == "financial"
        assert signals[0]["interval"] == 3600

    def test_get_meta(self, store: SignalStore) -> None:
        store.save_meta("btc", "financial", "crypto", 3600)
        meta = store.get_meta("btc")
        assert meta is not None
        assert meta["category"] == "crypto"

    def test_get_meta_not_found(self, store: SignalStore) -> None:
        assert store.get_meta("nonexistent") is None

    def test_save_empty_df(self, store: SignalStore) -> None:
        store.save("s", pd.DataFrame())
        result = store.get_data("s")
        assert result.empty

    def test_save_with_date_column(self, store: SignalStore) -> None:
        df = pd.DataFrame({"date": ["2024-01-01"], "value": [42.0]})
        store.save("s", df)
        result = store.get_data("s")
        assert len(result) == 1
        assert result.iloc[0]["value"] == 42.0

    def test_get_data_empty(self, store: SignalStore) -> None:
        result = store.get_data("nonexistent")
        assert result.empty
        assert list(result.columns) == ["timestamp", "value"]


class TestOHLCV:
    def test_save_ohlcv_data(self, store: SignalStore) -> None:
        df = pd.DataFrame({
            "timestamp": ["2024-01-01", "2024-01-02"],
            "value": [100.0, 110.0],
            "open": [95.0, 105.0],
            "high": [105.0, 115.0],
            "low": [90.0, 100.0],
            "volume": [1000.0, 1200.0],
        })
        store.save("btc", df)
        result = store.get_data("btc")
        assert len(result) == 2
        assert "open" in result.columns
        assert "high" in result.columns
        assert "low" in result.columns
        assert "volume" in result.columns
        assert result.iloc[0]["open"] == 95.0
        assert result.iloc[1]["high"] == 115.0

    def test_scalar_data_no_ohlcv_columns(self, store: SignalStore) -> None:
        df = pd.DataFrame({"timestamp": ["2024-01-01"], "value": [42.0]})
        store.save("scalar_sig", df)
        result = store.get_data("scalar_sig")
        assert list(result.columns) == ["timestamp", "value"]

    def test_get_data_columns_filter(self, store: SignalStore) -> None:
        df = pd.DataFrame({
            "timestamp": ["2024-01-01"],
            "value": [100.0],
            "open": [95.0],
            "high": [105.0],
            "low": [90.0],
            "volume": [1000.0],
        })
        store.save("btc", df)
        result = store.get_data("btc", columns=["timestamp", "high", "low"])
        assert list(result.columns) == ["timestamp", "high", "low"]

    def test_get_latest_ohlcv(self, store: SignalStore) -> None:
        df = pd.DataFrame({
            "timestamp": ["2024-01-01"],
            "value": [100.0],
            "open": [95.0],
            "high": [105.0],
            "low": [90.0],
            "volume": [1000.0],
        })
        store.save("btc", df)
        latest = store.get_latest("btc")
        assert latest is not None
        assert latest["open"] == 95.0
        assert latest["volume"] == 1000.0

    def test_get_latest_scalar_no_ohlcv(self, store: SignalStore) -> None:
        df = pd.DataFrame({"timestamp": ["2024-01-01"], "value": [42.0]})
        store.save("s", df)
        latest = store.get_latest("s")
        assert latest is not None
        assert "open" not in latest
        assert "high" not in latest


class TestSignalType:
    def test_save_meta_with_signal_type(self, store: SignalStore) -> None:
        store.save_meta("btc", "financial", "crypto", 3600, "ohlcv")
        meta = store.get_meta("btc")
        assert meta is not None
        assert meta["signal_type"] == "ohlcv"

    def test_signal_type_default(self, store: SignalStore) -> None:
        store.save_meta("fear_greed", "sentiment", "sentiment", 86400)
        meta = store.get_meta("fear_greed")
        assert meta is not None
        assert meta["signal_type"] == "scalar"

    def test_signal_type_in_list(self, store: SignalStore) -> None:
        store.save_meta("btc", "financial", "crypto", 3600, "ohlcv")
        store.save_meta("fear", "sentiment", "sentiment", 86400)
        signals = store.list_signals()
        types = {s["name"]: s["signal_type"] for s in signals}
        assert types["btc"] == "ohlcv"
        assert types["fear"] == "scalar"


class TestTimestampNormalization:
    def test_save_normalizes_space_to_t(self, store: SignalStore) -> None:
        df = pd.DataFrame({
            "timestamp": pd.to_datetime(["2024-01-01 12:00:00+00:00"]),
            "value": [1.0],
        })
        store.save("s", df)
        row = store._conn.execute("SELECT timestamp FROM signals WHERE name = 's'").fetchone()
        assert "T" in row[0]
        assert " " not in row[0][:19]

    def test_save_normalizes_pandas_timestamp(self, store: SignalStore) -> None:
        df = pd.DataFrame({
            "timestamp": [pd.Timestamp("2024-06-15 08:30:00", tz="UTC")],
            "value": [42.0],
        })
        store.save("s", df)
        row = store._conn.execute("SELECT timestamp FROM signals WHERE name = 's'").fetchone()
        assert row[0].startswith("2024-06-15T08:30:00")

    def test_since_works_with_t_format(self, store: SignalStore) -> None:
        df = pd.DataFrame({
            "timestamp": ["2024-01-01T00:00:00", "2024-01-02T00:00:00", "2024-01-03T00:00:00"],
            "value": [1.0, 2.0, 3.0],
        })
        store.save("s", df)
        result = store.get_data("s", since="2024-01-02T00:00:00")
        assert len(result) == 2

    def test_since_normalizes_space_format(self, store: SignalStore) -> None:
        df = pd.DataFrame({
            "timestamp": ["2024-01-01T00:00:00", "2024-01-02T00:00:00"],
            "value": [1.0, 2.0],
        })
        store.save("s", df)
        # Pass space-separated since — should still work
        result = store.get_data("s", since="2024-01-02 00:00:00")
        assert len(result) == 1

    def test_migrate_normalizes_existing_space_timestamps(self, tmp_path) -> None:
        import sqlite3

        db_path = tmp_path / "legacy.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute(
            "CREATE TABLE signals (name TEXT, timestamp TEXT, value REAL,"
            " open REAL, high REAL, low REAL, volume REAL,"
            " PRIMARY KEY (name, timestamp))"
        )
        conn.execute(
            "CREATE TABLE signal_meta (name TEXT PRIMARY KEY, domain TEXT DEFAULT '',"
            " category TEXT DEFAULT '', interval INTEGER DEFAULT 86400,"
            " signal_type TEXT DEFAULT 'scalar', last_updated TEXT)"
        )
        # Insert space-separated timestamps
        conn.execute(
            "INSERT INTO signals VALUES ('s', '2024-01-01 12:00:00+00:00', 1.0,"
            " NULL, NULL, NULL, NULL)"
        )
        conn.commit()
        conn.close()

        # Opening SignalStore triggers _migrate which normalizes timestamps
        s = SignalStore(db_path)
        row = s._conn.execute("SELECT timestamp FROM signals WHERE name = 's'").fetchone()
        assert row[0] == "2024-01-01T12:00:00+00:00"
        s.close()

    def test_save_meta_does_not_set_last_updated(self, store: SignalStore) -> None:
        """save_meta() should not touch last_updated; only save() sets it."""
        store.save_meta("btc", "financial", "crypto", 3600)
        meta = store.get_meta("btc")
        assert meta is not None
        assert meta["last_updated"] is None

    def test_save_sets_last_updated_iso_format(self, store: SignalStore) -> None:
        """save() should set last_updated in ISO 8601 format."""
        store.save_meta("btc", "financial", "crypto", 3600)
        df = pd.DataFrame({"timestamp": ["2024-01-01T00:00:00+00:00"], "value": [50000.0]})
        store.save("btc", df)
        meta = store.get_meta("btc")
        assert meta is not None
        assert meta["last_updated"] is not None
        assert "T" in meta["last_updated"]
        assert meta["last_updated"].endswith("Z")


class TestFreshness:
    def test_check_freshness_no_stale(self, store: SignalStore) -> None:
        store.save_meta("btc", "financial", "crypto", 86400)
        stale = store.check_freshness()
        assert stale == []

    def test_check_freshness_stale(self, store: SignalStore) -> None:
        store.save_meta("btc", "financial", "crypto", 3600)
        # Backdate last_updated by 3 hours (> 3600 * 2.0 = 7200s)
        store._conn.execute(
            "UPDATE signal_meta SET last_updated = strftime('%Y-%m-%dT%H:%M:%SZ',"
            " 'now', '-3 hours') WHERE name = 'btc'"
        )
        store._conn.commit()
        stale = store.check_freshness()
        assert len(stale) == 1
        assert stale[0]["name"] == "btc"
        assert stale[0]["expected_interval"] == 3600

    def test_check_freshness_custom_threshold(self, store: SignalStore) -> None:
        store.save_meta("btc", "financial", "crypto", 3600)
        store._conn.execute(
            "UPDATE signal_meta SET last_updated = strftime('%Y-%m-%dT%H:%M:%SZ',"
            " 'now', '-2 hours') WHERE name = 'btc'"
        )
        store._conn.commit()
        # threshold_factor=1.0 → stale after 3600s (1h), so 2h old = stale
        stale = store.check_freshness(threshold_factor=1.0)
        assert len(stale) == 1
        # threshold_factor=3.0 → stale after 10800s (3h), so 2h old = fresh
        stale = store.check_freshness(threshold_factor=3.0)
        assert len(stale) == 0

    def test_reset_failures(self, store: SignalStore) -> None:
        store.save_meta("btc", "financial", "crypto", 3600)
        store.increment_failures("btc")
        store.increment_failures("btc")
        meta = store.get_meta("btc")
        assert meta["consecutive_failures"] == 2
        store.reset_failures("btc")
        meta = store.get_meta("btc")
        assert meta["consecutive_failures"] == 0

    def test_increment_failures(self, store: SignalStore) -> None:
        store.save_meta("btc", "financial", "crypto", 3600)
        store.increment_failures("btc")
        meta = store.get_meta("btc")
        assert meta["consecutive_failures"] == 1

    def test_check_freshness_includes_failures(self, store: SignalStore) -> None:
        store.save_meta("btc", "financial", "crypto", 3600)
        store.increment_failures("btc")
        store.increment_failures("btc")
        store._conn.execute(
            "UPDATE signal_meta SET last_updated = strftime('%Y-%m-%dT%H:%M:%SZ',"
            " 'now', '-3 hours') WHERE name = 'btc'"
        )
        store._conn.commit()
        stale = store.check_freshness()
        assert len(stale) == 1
        assert stale[0]["consecutive_failures"] == 2


class TestMigration:
    def test_migrate_parquet_files(self, store: SignalStore, tmp_path: Path) -> None:
        from signal_noise.store.migration import migrate_parquet_to_sqlite

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        df = pd.DataFrame({"timestamp": ["2024-01-01", "2024-01-02"], "value": [1.0, 2.0]})
        df.to_parquet(raw_dir / "test_signal.parquet", index=False)

        count = migrate_parquet_to_sqlite(raw_dir, store, {})
        assert count == 1
        result = store.get_data("test_signal")
        assert len(result) == 2

    def test_migrate_empty_dir(self, store: SignalStore, tmp_path: Path) -> None:
        from signal_noise.store.migration import migrate_parquet_to_sqlite

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        count = migrate_parquet_to_sqlite(raw_dir, store, {})
        assert count == 0

    def test_migrate_nonexistent_dir(self, store: SignalStore, tmp_path: Path) -> None:
        from signal_noise.store.migration import migrate_parquet_to_sqlite

        count = migrate_parquet_to_sqlite(tmp_path / "nope", store, {})
        assert count == 0


class TestAnomalies:
    def test_no_anomaly_insufficient_history(self, store: SignalStore) -> None:
        df = pd.DataFrame({"timestamp": ["2024-01-01"], "value": [100.0]})
        result = store.check_anomalies("s", df)
        assert result == []

    def test_no_anomaly_normal_value(self, store: SignalStore) -> None:
        # Seed history
        hist = pd.DataFrame({
            "timestamp": [f"2024-01-{i+1:02d}" for i in range(20)],
            "value": [100.0 + i * 0.1 for i in range(20)],
        })
        store.save("s", hist)
        # New data within normal range
        new = pd.DataFrame({"timestamp": ["2024-02-01"], "value": [101.0]})
        result = store.check_anomalies("s", new)
        assert result == []

    def test_detects_anomaly(self, store: SignalStore) -> None:
        hist = pd.DataFrame({
            "timestamp": [f"2024-01-{i+1:02d}" for i in range(20)],
            "value": [100.0 + i * 0.01 for i in range(20)],
        })
        store.save("s", hist)
        # Extreme outlier
        new = pd.DataFrame({"timestamp": ["2024-02-01"], "value": [999999.0]})
        result = store.check_anomalies("s", new)
        assert len(result) == 1
        assert result[0]["value"] == 999999.0
        assert result[0]["z_score"] > 4.0

    def test_custom_threshold(self, store: SignalStore) -> None:
        hist = pd.DataFrame({
            "timestamp": [f"2024-01-{i+1:02d}" for i in range(20)],
            "value": [100.0 + i for i in range(20)],
        })
        store.save("s", hist)
        new = pd.DataFrame({"timestamp": ["2024-02-01"], "value": [200.0]})
        # Strict threshold should flag more
        strict = store.check_anomalies("s", new, z_threshold=1.0)
        # Lenient threshold should flag fewer
        lenient = store.check_anomalies("s", new, z_threshold=20.0)
        assert len(strict) >= len(lenient)

    def test_zero_std(self, store: SignalStore) -> None:
        hist = pd.DataFrame({
            "timestamp": [f"2024-01-{i+1:02d}" for i in range(20)],
            "value": [100.0] * 20,
        })
        store.save("s", hist)
        # Different value but MAD=0 → no anomaly reported (can't compute z)
        new = pd.DataFrame({"timestamp": ["2024-02-01"], "value": [200.0]})
        result = store.check_anomalies("s", new)
        assert result == []

    def test_robust_to_outlier_contamination(self, store: SignalStore) -> None:
        """Prior outliers in lookback should not inflate the baseline."""
        # 18 normal values + 2 extreme outliers in history
        values = [100.0 + i * 0.01 for i in range(18)] + [99999.0, 99999.0]
        hist = pd.DataFrame({
            "timestamp": [f"2024-01-{i+1:02d}" for i in range(20)],
            "value": values,
        })
        store.save("outlier_test", hist)
        # A moderately abnormal value should still be detected
        new = pd.DataFrame({"timestamp": ["2024-02-01"], "value": [500.0]})
        result = store.check_anomalies("outlier_test", new, z_threshold=4.0)
        assert len(result) == 1
        assert result[0]["median"] < 200  # median unaffected by outliers

    def test_anomaly_returns_median_and_mad(self, store: SignalStore) -> None:
        hist = pd.DataFrame({
            "timestamp": [f"2024-01-{i+1:02d}" for i in range(20)],
            "value": [100.0 + i * 0.01 for i in range(20)],
        })
        store.save("s", hist)
        new = pd.DataFrame({"timestamp": ["2024-02-01"], "value": [999999.0]})
        result = store.check_anomalies("s", new)
        assert len(result) == 1
        assert "median" in result[0]
        assert "mad" in result[0]
        assert "mean" not in result[0]
        assert "std" not in result[0]


class TestCheckHealth:
    def test_empty_store(self, store: SignalStore) -> None:
        h = store.check_health()
        assert h == {"never_seen": [], "fresh": [], "stale": [], "failing": []}

    def test_never_seen(self, store: SignalStore) -> None:
        store.save_meta("s", "financial", "crypto", 3600)
        h = store.check_health()
        assert len(h["never_seen"]) == 1
        assert h["never_seen"][0]["name"] == "s"

    def test_fresh(self, store: SignalStore) -> None:
        store.save_meta("s", "financial", "crypto", 3600)
        df = pd.DataFrame({"timestamp": ["2024-01-01"], "value": [1.0]})
        store.save("s", df)
        h = store.check_health()
        assert len(h["fresh"]) == 1
        assert h["fresh"][0]["name"] == "s"

    def test_stale(self, store: SignalStore) -> None:
        store.save_meta("s", "financial", "crypto", 3600)
        store._conn.execute(
            "UPDATE signal_meta SET last_updated = strftime('%Y-%m-%dT%H:%M:%SZ',"
            " 'now', '-3 hours') WHERE name = 's'"
        )
        store._conn.commit()
        h = store.check_health()
        assert len(h["stale"]) == 1

    def test_failing_takes_priority(self, store: SignalStore) -> None:
        store.save_meta("s", "financial", "crypto", 3600)
        store._conn.execute(
            "UPDATE signal_meta SET last_updated = strftime('%Y-%m-%dT%H:%M:%SZ',"
            " 'now', '-3 hours') WHERE name = 's'"
        )
        store._conn.commit()
        store.increment_failures("s")
        h = store.check_health()
        assert len(h["failing"]) == 1
        assert len(h["stale"]) == 0

    def test_all_four_states(self, store: SignalStore) -> None:
        # fresh
        store.save_meta("a", "financial", "crypto", 3600)
        store.save("a", pd.DataFrame({"timestamp": ["2024-01-01"], "value": [1.0]}))
        # never_seen
        store.save_meta("b", "earth", "weather", 86400)
        # stale
        store.save_meta("c", "macro", "economic", 3600)
        store._conn.execute(
            "UPDATE signal_meta SET last_updated = strftime('%Y-%m-%dT%H:%M:%SZ',"
            " 'now', '-3 hours') WHERE name = 'c'"
        )
        store._conn.commit()
        # failing
        store.save_meta("d", "financial", "crypto", 3600)
        store.increment_failures("d")
        h = store.check_health()
        assert [s["name"] for s in h["fresh"]] == ["a"]
        assert [s["name"] for s in h["never_seen"]] == ["b"]
        assert [s["name"] for s in h["stale"]] == ["c"]
        assert [s["name"] for s in h["failing"]] == ["d"]


class TestBatchMethods:
    def test_save_collection_result_single_commit(self, store: SignalStore) -> None:
        df = pd.DataFrame({"timestamp": ["2024-01-01", "2024-01-02"], "value": [1.0, 2.0]})
        rows = store.save_collection_result("s", df, "financial", "crypto", 3600)
        assert rows == 2
        # Data saved
        result = store.get_data("s")
        assert len(result) == 2
        # Meta upserted
        meta = store.get_meta("s")
        assert meta is not None
        assert meta["domain"] == "financial"
        assert meta["category"] == "crypto"
        assert meta["interval"] == 3600
        assert meta["last_updated"] is not None
        assert meta["consecutive_failures"] == 0
        # Audit logged
        logs = store.get_audit_log("s")
        assert len(logs) == 1
        assert logs[0]["event"] == "collected"
        assert logs[0]["rows"] == 2

    def test_save_collection_result_resets_failures(self, store: SignalStore) -> None:
        store.save_meta("s", "financial", "crypto", 3600)
        store.increment_failures("s")
        store.increment_failures("s")
        assert store.get_meta("s")["consecutive_failures"] == 2
        df = pd.DataFrame({"timestamp": ["2024-01-01"], "value": [1.0]})
        store.save_collection_result("s", df, "financial", "crypto", 3600)
        assert store.get_meta("s")["consecutive_failures"] == 0

    def test_save_collection_result_empty_df(self, store: SignalStore) -> None:
        rows = store.save_collection_result("s", pd.DataFrame(), "financial", "crypto", 3600)
        assert rows == 0

    def test_save_collection_result_custom_event(self, store: SignalStore) -> None:
        df = pd.DataFrame({"timestamp": ["2024-01-01"], "value": [1.0]})
        store.save_collection_result("s", df, "financial", "crypto", 3600, event="half_open_recovered")
        logs = store.get_audit_log("s")
        assert logs[0]["event"] == "half_open_recovered"

    def test_save_collection_failure_single_commit(self, store: SignalStore) -> None:
        store.save_meta("s", "financial", "crypto", 3600)
        store.save_collection_failure("s", "timeout error")
        meta = store.get_meta("s")
        assert meta["consecutive_failures"] == 1
        logs = store.get_audit_log("s")
        assert len(logs) == 1
        assert logs[0]["event"] == "failed"
        assert "timeout" in logs[0]["detail"]

    def test_save_collection_failure_increments(self, store: SignalStore) -> None:
        store.save_meta("s", "financial", "crypto", 3600)
        store.save_collection_failure("s", "err1")
        store.save_collection_failure("s", "err2")
        assert store.get_meta("s")["consecutive_failures"] == 2


class TestAuditLog:
    def test_log_event(self, store: SignalStore) -> None:
        store.log_event("btc", "collected", rows=10)
        logs = store.get_audit_log("btc")
        assert len(logs) == 1
        assert logs[0]["name"] == "btc"
        assert logs[0]["event"] == "collected"
        assert logs[0]["rows"] == 10

    def test_log_multiple_events(self, store: SignalStore) -> None:
        store.log_event("btc", "collected", rows=10)
        store.log_event("btc", "failed", detail="timeout")
        store.log_event("eth", "collected", rows=5)
        all_logs = store.get_audit_log()
        assert len(all_logs) == 3
        btc_logs = store.get_audit_log("btc")
        assert len(btc_logs) == 2

    def test_log_limit(self, store: SignalStore) -> None:
        for i in range(10):
            store.log_event("btc", "collected", rows=i)
        logs = store.get_audit_log("btc", limit=3)
        assert len(logs) == 3
        # Most recent first
        assert logs[0]["rows"] == 9

    def test_log_timestamp_format(self, store: SignalStore) -> None:
        store.log_event("btc", "collected")
        logs = store.get_audit_log("btc")
        assert "T" in logs[0]["timestamp"]


class TestResample:
    def test_resample_scalar_to_1h(self, store: SignalStore) -> None:
        # 60 one-minute rows → 1 hourly row
        timestamps = pd.date_range("2024-01-01T00:00:00Z", periods=60, freq="min")
        df = pd.DataFrame({
            "timestamp": [ts.isoformat() for ts in timestamps],
            "value": range(60),
        })
        store.save("s", df)
        result = store.get_data("s", resolution="1h")
        assert len(result) == 1
        # last value in the hour bucket
        assert result.iloc[0]["value"] == 59

    def test_resample_ohlcv_to_4h(self, store: SignalStore) -> None:
        # 4 hourly rows → 1 four-hour row
        timestamps = pd.date_range("2024-01-01T00:00:00Z", periods=4, freq="h")
        df = pd.DataFrame({
            "timestamp": [ts.isoformat() for ts in timestamps],
            "value": [100.0, 110.0, 90.0, 105.0],
            "open": [99.0, 109.0, 89.0, 104.0],
            "high": [105.0, 115.0, 95.0, 110.0],
            "low": [95.0, 105.0, 85.0, 100.0],
            "volume": [1000.0, 2000.0, 1500.0, 1800.0],
        })
        store.save("btc", df)
        result = store.get_data("btc", resolution="4h")
        assert len(result) == 1
        row = result.iloc[0]
        assert row["open"] == 99.0       # first
        assert row["high"] == 115.0      # max
        assert row["low"] == 85.0        # min
        assert row["volume"] == 6300.0   # sum
        assert row["value"] == 105.0     # last

    def test_resample_noop(self, store: SignalStore) -> None:
        df = pd.DataFrame({
            "timestamp": ["2024-01-01T00:00:00", "2024-01-01T01:00:00"],
            "value": [1.0, 2.0],
        })
        store.save("s", df)
        result_none = store.get_data("s", resolution=None)
        result_orig = store.get_data("s")
        assert len(result_none) == len(result_orig)

    def test_resample_invalid_resolution(self, store: SignalStore) -> None:
        df = pd.DataFrame({
            "timestamp": ["2024-01-01T00:00:00"],
            "value": [1.0],
        })
        store.save("s", df)
        result = store.get_data("s", resolution="invalid")
        assert len(result) == 1  # no resampling, returns as-is
