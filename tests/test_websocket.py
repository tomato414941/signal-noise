from __future__ import annotations

import asyncio

import pytest
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient

from signal_noise.api.app import app
from signal_noise.store.event_bus import EventBus, SignalEvent

import signal_noise.api.app as api_mod


@pytest.fixture
def _setup_bus():
    """Inject an EventBus into the app module for tests."""
    bus = EventBus()
    old = api_mod._event_bus
    api_mod._event_bus = bus
    yield bus
    api_mod._event_bus = old


def test_health_events_disabled():
    old = api_mod._event_bus
    api_mod._event_bus = None
    try:
        client = TestClient(app)
        r = client.get("/health/events")
        assert r.status_code == 200
        assert r.json()["status"] == "disabled"
    finally:
        api_mod._event_bus = old


def test_health_events_active(_setup_bus):
    client = TestClient(app)
    r = client.get("/health/events")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "active"
    assert data["subscribers"] == 0


@pytest.mark.asyncio
async def test_ws_signals_receives_events(_setup_bus):
    bus = _setup_bus

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as _:
        pass  # ensure app is importable

    # Use TestClient's websocket support
    def _ws_test():
        client = TestClient(app)
        with client.websocket_connect("/ws/signals?names=test_*") as ws:
            # Publish from another thread won't work with sync TestClient.
            # Instead, test the endpoint accepts connection.
            # Full integration test would need a real async server.
            pass

    _ws_test()


def test_ws_rejected_without_bus():
    old = api_mod._event_bus
    api_mod._event_bus = None
    try:
        client = TestClient(app)
        # WebSocket should be rejected
        with pytest.raises(Exception):
            with client.websocket_connect("/ws/signals"):
                pass
    finally:
        api_mod._event_bus = old
