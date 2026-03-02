"""Tests for SignalClient WebSocket subscribe method."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import patch

import pytest

from signal_noise.client import SignalClient


class FakeWsConnection:
    """Mock websocket connection."""

    def __init__(self, messages: list[dict]):
        self._messages = [json.dumps(m) for m in messages]
        self._idx = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._idx >= len(self._messages):
            raise StopAsyncIteration
        msg = self._messages[self._idx]
        self._idx += 1
        return msg


@pytest.mark.asyncio
async def test_subscribe_receives_events():
    client = SignalClient(base_url="http://localhost:8000")

    events = [
        {"name": "funding_rate_btc", "timestamp": "2026-03-01T00:00", "value": 0.001,
         "event_type": "update", "detail": ""},
        {"name": "liq_ratio_btc", "timestamp": "2026-03-01T00:01", "value": 0.6,
         "event_type": "update", "detail": ""},
    ]

    with patch("signal_noise.client.websockets") as mock_ws:
        mock_ws.connect.return_value = FakeWsConnection(events)

        received = []
        async for event in client.subscribe("funding_rate_*,liq_*", reconnect=False):
            received.append(event)

    assert len(received) == 2
    assert received[0]["name"] == "funding_rate_btc"
    assert received[1]["value"] == 0.6


@pytest.mark.asyncio
async def test_subscribe_url_construction():
    client = SignalClient(base_url="http://myhost:9000")

    with patch("signal_noise.client.websockets") as mock_ws:
        mock_ws.connect.return_value = FakeWsConnection([])

        async for _ in client.subscribe("test_*", reconnect=False):
            pass

        mock_ws.connect.assert_called_once()
        url = mock_ws.connect.call_args[0][0]
        assert url == "ws://myhost:9000/ws/signals?names=test_*"


@pytest.mark.asyncio
async def test_subscribe_reconnect_disabled():
    client = SignalClient(base_url="http://localhost:8000")

    with patch("signal_noise.client.websockets") as mock_ws:
        mock_ws.connect.side_effect = ConnectionError("refused")

        with pytest.raises(ConnectionError):
            async for _ in client.subscribe("*", reconnect=False):
                pass
