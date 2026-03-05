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
