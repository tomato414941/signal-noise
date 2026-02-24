from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from signal_noise.api.app import app, get_store
from signal_noise.store.sqlite_store import SignalStore


@pytest.fixture
def store(tmp_path: Path) -> SignalStore:
    s = SignalStore(tmp_path / "test.db")
    yield s
    s.close()


@pytest.fixture
def client(store: SignalStore) -> TestClient:
    app.dependency_overrides[get_store] = lambda: store
    # Also patch the module-level _store
    import signal_noise.api.app as api_mod
    api_mod._store = store
    yield TestClient(app)
    app.dependency_overrides.clear()
    api_mod._store = None


class TestHealth:
    def test_health_ok(self, client: TestClient) -> None:
        r = client.get("/health")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "ok"
        assert data["stale_count"] == 0

    def test_health_degraded(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "financial", "crypto", 3600)
        # Backdate last_updated to make it stale (3600s interval * 2.0 threshold = 7200s)
        store._conn.execute(
            "UPDATE signal_meta SET last_updated = strftime('%Y-%m-%dT%H:%M:%SZ',"
            " 'now', '-3 hours') WHERE name = 'btc'"
        )
        store._conn.commit()
        r = client.get("/health")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "degraded"
        assert data["stale_count"] == 1

    def test_health_signals(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "financial", "crypto", 3600)
        store._conn.execute(
            "UPDATE signal_meta SET last_updated = strftime('%Y-%m-%dT%H:%M:%SZ',"
            " 'now', '-3 hours') WHERE name = 'btc'"
        )
        store._conn.commit()
        r = client.get("/health/signals")
        assert r.status_code == 200
        data = r.json()
        assert data["stale_count"] == 1
        assert data["stale_signals"][0]["name"] == "btc"

    def test_health_signals_empty(self, client: TestClient) -> None:
        r = client.get("/health/signals")
        assert r.status_code == 200
        assert r.json() == {"stale_count": 0, "stale_signals": []}


class TestSignals:
    def test_list_empty(self, client: TestClient) -> None:
        r = client.get("/signals")
        assert r.status_code == 200
        assert r.json() == []

    def test_list_with_data(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "financial", "crypto", 3600)
        r = client.get("/signals")
        assert r.status_code == 200
        data = r.json()
        assert len(data) == 1
        assert data[0]["name"] == "btc"

    def test_meta_found(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "financial", "crypto", 3600)
        r = client.get("/signals/btc")
        assert r.status_code == 200
        assert r.json()["category"] == "crypto"

    def test_meta_not_found(self, client: TestClient) -> None:
        r = client.get("/signals/nonexistent")
        assert r.status_code == 404

    def test_data(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "financial", "crypto", 3600)
        df = pd.DataFrame({"timestamp": ["2024-01-01", "2024-01-02"], "value": [1.0, 2.0]})
        store.save("btc", df)
        r = client.get("/signals/btc/data")
        assert r.status_code == 200
        data = r.json()
        assert len(data) == 2

    def test_data_since(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "financial", "crypto", 3600)
        df = pd.DataFrame({
            "timestamp": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "value": [1.0, 2.0, 3.0],
        })
        store.save("btc", df)
        r = client.get("/signals/btc/data?since=2024-01-02")
        assert r.status_code == 200
        assert len(r.json()) == 2

    def test_data_not_found(self, client: TestClient) -> None:
        r = client.get("/signals/nonexistent/data")
        assert r.status_code == 404

    def test_latest(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "financial", "crypto", 3600)
        df = pd.DataFrame({"timestamp": ["2024-01-01", "2024-01-02"], "value": [1.0, 2.0]})
        store.save("btc", df)
        r = client.get("/signals/btc/latest")
        assert r.status_code == 200
        assert r.json()["value"] == 2.0

    def test_latest_not_found(self, client: TestClient) -> None:
        r = client.get("/signals/nonexistent/latest")
        assert r.status_code == 404

    def test_data_columns_filter(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "financial", "crypto", 3600, "ohlcv")
        df = pd.DataFrame({
            "timestamp": ["2024-01-01"],
            "value": [100.0],
            "open": [95.0],
            "high": [105.0],
            "low": [90.0],
            "volume": [1000.0],
        })
        store.save("btc", df)
        r = client.get("/signals/btc/data?columns=timestamp,high,low")
        assert r.status_code == 200
        row = r.json()[0]
        assert set(row.keys()) == {"timestamp", "high", "low"}

    def test_data_ohlcv_full(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "financial", "crypto", 3600, "ohlcv")
        df = pd.DataFrame({
            "timestamp": ["2024-01-01"],
            "value": [100.0],
            "open": [95.0],
            "high": [105.0],
            "low": [90.0],
            "volume": [1000.0],
        })
        store.save("btc", df)
        r = client.get("/signals/btc/data")
        row = r.json()[0]
        assert row["open"] == 95.0
        assert row["volume"] == 1000.0


class TestFilters:
    def test_filter_by_domain(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "financial", "crypto", 3600, "ohlcv")
        store.save_meta("temp", "earth", "weather", 86400)
        r = client.get("/signals?domain=financial")
        data = r.json()
        assert len(data) == 1
        assert data[0]["name"] == "btc"

    def test_filter_by_category(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "financial", "crypto", 3600)
        store.save_meta("sp500", "financial", "equity", 86400)
        r = client.get("/signals?category=crypto")
        data = r.json()
        assert len(data) == 1
        assert data[0]["name"] == "btc"

    def test_filter_by_signal_type(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "financial", "crypto", 3600, "ohlcv")
        store.save_meta("fear", "sentiment", "sentiment", 86400)
        r = client.get("/signals?signal_type=ohlcv")
        data = r.json()
        assert len(data) == 1
        assert data[0]["name"] == "btc"

    def test_filter_combined(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "financial", "crypto", 3600, "ohlcv")
        store.save_meta("sp500", "financial", "equity", 86400, "ohlcv")
        store.save_meta("fear", "sentiment", "sentiment", 86400)
        r = client.get("/signals?domain=financial&signal_type=ohlcv")
        data = r.json()
        assert len(data) == 2


class TestBatch:
    def test_batch_basic(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "financial", "crypto", 3600)
        store.save_meta("sp500", "financial", "equity", 86400)
        df1 = pd.DataFrame({"timestamp": ["2024-01-01"], "value": [100.0]})
        df2 = pd.DataFrame({"timestamp": ["2024-01-01"], "value": [4800.0]})
        store.save("btc", df1)
        store.save("sp500", df2)
        r = client.post("/signals/batch", json={"names": ["btc", "sp500"]})
        assert r.status_code == 200
        data = r.json()
        assert "btc" in data
        assert "sp500" in data
        assert len(data["btc"]) == 1
        assert data["sp500"][0]["value"] == 4800.0

    def test_batch_unknown_signal_skipped(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "financial", "crypto", 3600)
        df = pd.DataFrame({"timestamp": ["2024-01-01"], "value": [100.0]})
        store.save("btc", df)
        r = client.post("/signals/batch", json={"names": ["btc", "nonexistent"]})
        assert r.status_code == 200
        data = r.json()
        assert "btc" in data
        assert "nonexistent" not in data

    def test_batch_with_since(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "financial", "crypto", 3600)
        df = pd.DataFrame({
            "timestamp": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "value": [1.0, 2.0, 3.0],
        })
        store.save("btc", df)
        r = client.post("/signals/batch", json={"names": ["btc"], "since": "2024-01-02"})
        assert len(r.json()["btc"]) == 2

    def test_batch_empty_names(self, client: TestClient) -> None:
        r = client.post("/signals/batch", json={"names": []})
        assert r.status_code == 200
        assert r.json() == {}

    def test_batch_with_columns(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "financial", "crypto", 3600, "ohlcv")
        df = pd.DataFrame({
            "timestamp": ["2024-01-01"],
            "value": [100.0],
            "open": [95.0],
            "high": [105.0],
            "low": [90.0],
            "volume": [1000.0],
        })
        store.save("btc", df)
        r = client.post("/signals/batch", json={
            "names": ["btc"],
            "columns": ["timestamp", "high", "low"],
        })
        row = r.json()["btc"][0]
        assert set(row.keys()) == {"timestamp", "high", "low"}
