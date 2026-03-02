"""Tests for realtime API endpoints."""
from __future__ import annotations

import pandas as pd
import pytest
from fastapi.testclient import TestClient

import signal_noise.api.app as api_mod
from signal_noise.api.app import app
from signal_noise.store.sqlite_store import SignalStore


@pytest.fixture
def _setup_store(tmp_path):
    store = SignalStore(tmp_path / "test.db")

    # Register a microstructure signal
    store.save_meta("book_imbalance_btc", "markets", "microstructure", 60)

    # Insert realtime data
    df = pd.DataFrame({
        "timestamp": ["2026-03-01T10:00:00Z", "2026-03-01T10:01:00Z"],
        "value": [0.15, 0.22],
    })
    store.save_realtime("book_imbalance_btc", df)

    old = api_mod._store
    api_mod._store = store
    yield store
    api_mod._store = old
    store.close()


def test_realtime_endpoint(_setup_store):
    client = TestClient(app)
    r = client.get("/signals/book_imbalance_btc/realtime")
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 2
    assert data[0]["value"] == pytest.approx(0.15)


def test_realtime_endpoint_since(_setup_store):
    client = TestClient(app)
    r = client.get("/signals/book_imbalance_btc/realtime?since=2026-03-01T10:01:00Z")
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 1
    assert data[0]["value"] == pytest.approx(0.22)


def test_realtime_endpoint_not_found(_setup_store):
    client = TestClient(app)
    r = client.get("/signals/nonexistent/realtime")
    assert r.status_code == 404


def test_signal_data_fallback_to_realtime(_setup_store):
    """signal_data falls back to realtime for microstructure signals."""
    client = TestClient(app)
    r = client.get("/signals/book_imbalance_btc/data")
    assert r.status_code == 200
    data = r.json()
    # Should fall back to realtime since signals table is empty
    assert len(data) == 2
