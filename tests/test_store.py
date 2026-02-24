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
