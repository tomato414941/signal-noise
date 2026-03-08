from __future__ import annotations

import pandas as pd

from signal_noise.client import SignalClient


def test_get_data_parses_mixed_iso8601_timestamps(monkeypatch):
    client = SignalClient(base_url="http://localhost:8000")

    def fake_get(path: str, params: dict | None = None):
        assert path == "/signals/btc_ohlcv/data"
        return [
            {"timestamp": "2025-12-25T08:00:00+00:00", "value": 1.0},
            {"timestamp": "2025-12-25T08:00:00.001000+00:00", "value": 2.0},
        ]

    monkeypatch.setattr(client, "_get", fake_get)

    df = client.get_data("btc_ohlcv")

    assert list(df["value"]) == [1.0, 2.0]
    assert str(df["timestamp"].dtype).endswith(", UTC]")
    assert df["timestamp"].iloc[1] == pd.Timestamp("2025-12-25T08:00:00.001000+00:00")


def test_get_batch_parses_mixed_timestamps_and_drops_invalid_rows(monkeypatch):
    client = SignalClient(base_url="http://localhost:8000")

    def fake_post(path: str, **kwargs):
        assert path == "/signals/batch"
        return {
            "btc_ohlcv": [
                {"timestamp": "2025-12-25T08:00:00+00:00", "value": 1.0},
                {"timestamp": "not-a-timestamp", "value": 99.0},
                {"timestamp": "2025-12-25T08:00:00.001000+00:00", "value": 2.0},
            ],
        }

    monkeypatch.setattr(client, "_post", fake_post)

    batch = client.get_batch(["btc_ohlcv"])
    df = batch["btc_ohlcv"]

    assert list(df["value"]) == [1.0, 2.0]
    assert str(df["timestamp"].dtype).endswith(", UTC]")
    assert df["timestamp"].iloc[0] == pd.Timestamp("2025-12-25T08:00:00+00:00")


def test_get_batch_chunks_large_requests(monkeypatch):
    client = SignalClient(base_url="http://localhost:8000", batch_chunk_size=2)
    seen_chunks: list[list[str]] = []

    def fake_post(path: str, **kwargs):
        assert path == "/signals/batch"
        names = kwargs["json"]["names"]
        seen_chunks.append(names)
        return {
            name: [{"timestamp": "2025-12-25T08:00:00+00:00", "value": float(i)}]
            for i, name in enumerate(names, start=1)
        }

    monkeypatch.setattr(client, "_post", fake_post)

    batch = client.get_batch(["a", "b", "c", "d", "e"])

    assert seen_chunks == [["a", "b"], ["c", "d"], ["e"]]
    assert sorted(batch.keys()) == ["a", "b", "c", "d", "e"]


def test_health_detail_returns_api_shape(monkeypatch):
    client = SignalClient(base_url="http://localhost:8000")

    def fake_get(path: str, params: dict | None = None):
        assert path == "/health"
        assert params is None
        return {
            "status": "ok",
            "fresh": 10,
            "stale": 1,
            "failing": 2,
            "never_seen": 3,
            "suppressed": 4,
        }

    monkeypatch.setattr(client, "_get", fake_get)

    assert client.health_detail() == {
        "status": "ok",
        "fresh": 10,
        "stale": 1,
        "failing": 2,
        "never_seen": 3,
        "suppressed": 4,
    }


def test_health_detail_returns_unreachable_shape_on_error(monkeypatch):
    client = SignalClient(base_url="http://localhost:8000")

    def fake_get(path: str, params: dict | None = None):
        raise RuntimeError("network down")

    monkeypatch.setattr(client, "_get", fake_get)

    assert client.health_detail() == {
        "status": "unreachable",
        "fresh": -1,
        "stale": -1,
        "failing": -1,
        "never_seen": -1,
        "suppressed": -1,
    }


def test_stale_signals_reads_stale_key(monkeypatch):
    client = SignalClient(base_url="http://localhost:8000")

    def fake_get(path: str, params: dict | None = None):
        assert path == "/health/signals"
        assert params is None
        return {
            "fresh": 10,
            "suppressed": [],
            "stale": [{"name": "btc", "age_seconds": 7200, "interval": 3600}],
            "failing": [],
            "never_seen": [],
        }

    monkeypatch.setattr(client, "_get", fake_get)

    assert client.stale_signals() == [{"name": "btc", "age_seconds": 7200, "interval": 3600}]
