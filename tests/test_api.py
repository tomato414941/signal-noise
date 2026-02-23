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
        assert r.json() == {"status": "ok"}


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
