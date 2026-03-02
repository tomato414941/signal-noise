"""Tests for realtime signal API endpoints."""
from __future__ import annotations

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from signal_noise.api.app import app, get_store
from signal_noise.store.sqlite_store import SignalStore


@pytest.fixture
def store(tmp_path):
    s = SignalStore(tmp_path / "test.db")
    yield s
    s.close()


@pytest.fixture
def client(store):
    app.dependency_overrides[get_store] = lambda: store
    # Inject store directly
    import signal_noise.api.app as api_mod
    api_mod._store = store
    with TestClient(app) as c:
        yield c
    api_mod._store = None


def test_signal_realtime_endpoint(client, store):
    df = pd.DataFrame({
        "timestamp": ["2026-03-01T10:00:00Z", "2026-03-01T10:01:00Z"],
        "value": [0.5, 0.6],
    })
    store.save_realtime_collection_result(
        "book_imbalance_btc", df,
        "financial", "microstructure", 60,
    )

    resp = client.get("/signals/book_imbalance_btc/realtime")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["value"] == pytest.approx(0.5)


def test_signal_realtime_with_since(client, store):
    df = pd.DataFrame({
        "timestamp": ["2026-03-01T10:00:00Z", "2026-03-01T11:00:00Z"],
        "value": [0.5, 0.6],
    })
    store.save_realtime_collection_result(
        "vpin_btc", df,
        "financial", "microstructure", 60,
    )

    resp = client.get("/signals/vpin_btc/realtime?since=2026-03-01T11:00:00Z")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1


def test_signal_realtime_not_found(client):
    resp = client.get("/signals/nonexistent/realtime")
    assert resp.status_code == 404


def test_signal_data_fallback_to_realtime(client, store):
    """signal_data endpoint falls back to signals_realtime for microstructure."""
    df = pd.DataFrame({
        "timestamp": ["2026-03-01T10:00:00Z"],
        "value": [0.42],
    })
    store.save_realtime_collection_result(
        "spread_bps_btc", df,
        "financial", "microstructure", 60,
    )

    resp = client.get("/signals/spread_bps_btc/data")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["value"] == pytest.approx(0.42)


def test_signal_data_no_fallback_for_non_microstructure(client, store):
    """Non-microstructure signals don't fall back to realtime."""
    store.save_meta("daily_sig", "financial", "crypto", 86400)

    resp = client.get("/signals/daily_sig/data")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 0
