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
    def test_root_redirects_to_ops(self, client: TestClient) -> None:
        r = client.get("/", follow_redirects=False)
        assert r.status_code == 307
        assert r.headers["location"] == "/ops"

    def test_ops_board_html(self, client: TestClient) -> None:
        r = client.get("/ops")
        assert r.status_code == 200
        assert "text/html" in r.headers["content-type"]
        body = r.text
        assert "signal-noise ops board" in body
        assert "/health/signals" in body
        assert "Suppressed Ledger" in body

    def test_health_ok(self, client: TestClient) -> None:
        r = client.get("/health")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "ok"
        assert data["fresh"] == 0
        assert data["stale"] == 0
        assert data["failing"] == 0
        assert data["never_seen"] == 0
        assert data["suppressed"] == 0

    def test_health_degraded_stale(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "markets", "crypto", 3600)
        store._conn.execute(
            "UPDATE signal_meta SET last_updated = strftime('%Y-%m-%dT%H:%M:%SZ',"
            " 'now', '-3 hours') WHERE name = 'btc'"
        )
        store._conn.commit()
        r = client.get("/health")
        data = r.json()
        assert data["status"] == "degraded"
        assert data["stale"] == 1

    def test_health_degraded_failing(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "markets", "crypto", 3600)
        store.increment_failures("btc")
        r = client.get("/health")
        data = r.json()
        assert data["status"] == "degraded"
        assert data["failing"] == 1

    def test_health_never_seen(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "markets", "crypto", 3600)
        r = client.get("/health")
        data = r.json()
        assert data["status"] == "ok"
        assert data["never_seen"] == 1

    def test_health_suppressed(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "markets", "crypto", 3600, suppressed=True)
        r = client.get("/health")
        data = r.json()
        assert data["status"] == "ok"
        assert data["suppressed"] == 1
        assert data["never_seen"] == 0

    def test_health_fresh(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "markets", "crypto", 3600)
        df = pd.DataFrame({"timestamp": ["2024-01-01"], "value": [1.0]})
        store.save("btc", df)
        r = client.get("/health")
        data = r.json()
        assert data["status"] == "ok"
        assert data["fresh"] == 1

    def test_health_signals_4states(self, client: TestClient, store: SignalStore) -> None:
        # stale
        store.save_meta("btc", "markets", "crypto", 3600)
        store._conn.execute(
            "UPDATE signal_meta SET last_updated = strftime('%Y-%m-%dT%H:%M:%SZ',"
            " 'now', '-3 hours') WHERE name = 'btc'"
        )
        store._conn.commit()
        # never_seen
        store.save_meta("new_sig", "environment", "weather", 86400)
        # failing
        store.save_meta("broken", "markets", "crypto", 3600)
        store.save_collection_failure("broken", "read timeout")
        # suppressed
        store.save_meta(
            "ignored",
            "society",
            "legislation",
            86400,
            suppressed=True,
            suppressed_reason="SIGNAL_NOISE_EXCLUDE",
            suppressed_source="env",
            suppressed_at="2026-03-08T00:00:00Z",
        )
        r = client.get("/health/signals")
        assert r.status_code == 200
        data = r.json()
        assert data["suppressed"] == [{
            "name": "ignored",
            "reason": "SIGNAL_NOISE_EXCLUDE",
            "source": "env",
            "suppressed_at": "2026-03-08T00:00:00Z",
        }]
        assert len(data["stale"]) == 1
        assert data["stale"][0]["name"] == "btc"
        assert "new_sig" in data["never_seen"]
        assert len(data["failing"]) == 1
        assert data["failing"][0]["name"] == "broken"
        assert data["failing"][0]["error"] == "read timeout"
        assert data["failing"][0]["error_at"] is not None

    def test_health_signals_empty(self, client: TestClient) -> None:
        r = client.get("/health/signals")
        assert r.status_code == 200
        data = r.json()
        assert data["fresh"] == 0
        assert data["suppressed"] == []
        assert data["stale"] == []
        assert data["failing"] == []
        assert data["never_seen"] == []


class TestSignals:
    def test_list_empty(self, client: TestClient) -> None:
        r = client.get("/signals")
        assert r.status_code == 200
        assert r.json() == []

    def test_list_with_data(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "markets", "crypto", 3600)
        r = client.get("/signals")
        assert r.status_code == 200
        data = r.json()
        assert len(data) == 1
        assert data[0]["name"] == "btc"

    def test_meta_found(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "markets", "crypto", 3600)
        r = client.get("/signals/btc")
        assert r.status_code == 200
        assert r.json()["category"] == "crypto"

    def test_meta_not_found(self, client: TestClient) -> None:
        r = client.get("/signals/nonexistent")
        assert r.status_code == 404

    def test_data(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "markets", "crypto", 3600)
        df = pd.DataFrame({"timestamp": ["2024-01-01", "2024-01-02"], "value": [1.0, 2.0]})
        store.save("btc", df)
        r = client.get("/signals/btc/data")
        assert r.status_code == 200
        data = r.json()
        assert len(data) == 2

    def test_data_since(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "markets", "crypto", 3600)
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
        store.save_meta("btc", "markets", "crypto", 3600)
        df = pd.DataFrame({"timestamp": ["2024-01-01", "2024-01-02"], "value": [1.0, 2.0]})
        store.save("btc", df)
        r = client.get("/signals/btc/latest")
        assert r.status_code == 200
        assert r.json()["value"] == 2.0

    def test_latest_not_found(self, client: TestClient) -> None:
        r = client.get("/signals/nonexistent/latest")
        assert r.status_code == 404

    def test_data_columns_filter(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "markets", "crypto", 3600, "ohlcv")
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
        store.save_meta("btc", "markets", "crypto", 3600, "ohlcv")
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
        store.save_meta("btc", "markets", "crypto", 3600, "ohlcv")
        store.save_meta("temp", "environment", "weather", 86400)
        r = client.get("/signals?domain=markets")
        data = r.json()
        assert len(data) == 1
        assert data[0]["name"] == "btc"

    def test_filter_by_category(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "markets", "crypto", 3600)
        store.save_meta("sp500", "markets", "equity", 86400)
        r = client.get("/signals?category=crypto")
        data = r.json()
        assert len(data) == 1
        assert data[0]["name"] == "btc"

    def test_filter_by_signal_type(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "markets", "crypto", 3600, "ohlcv")
        store.save_meta("fear", "sentiment", "sentiment", 86400)
        r = client.get("/signals?signal_type=ohlcv")
        data = r.json()
        assert len(data) == 1
        assert data[0]["name"] == "btc"

    def test_filter_by_new_signal_type(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("probe_network_state", "technology", "internet", 300, "state")
        store.save_meta("probe_ping_distribution_core", "technology", "internet", 300, "distribution")
        r = client.get("/signals?signal_type=state")
        data = r.json()
        assert len(data) == 1
        assert data[0]["name"] == "probe_network_state"

    def test_filter_combined(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "markets", "crypto", 3600, "ohlcv")
        store.save_meta("sp500", "markets", "equity", 86400, "ohlcv")
        store.save_meta("fear", "sentiment", "sentiment", 86400)
        r = client.get("/signals?domain=markets&signal_type=ohlcv")
        data = r.json()
        assert len(data) == 2


class TestBatch:
    def test_batch_basic(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "markets", "crypto", 3600)
        store.save_meta("sp500", "markets", "equity", 86400)
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
        store.save_meta("btc", "markets", "crypto", 3600)
        df = pd.DataFrame({"timestamp": ["2024-01-01"], "value": [100.0]})
        store.save("btc", df)
        r = client.post("/signals/batch", json={"names": ["btc", "nonexistent"]})
        assert r.status_code == 200
        data = r.json()
        assert "btc" in data
        assert "nonexistent" not in data

    def test_batch_with_since(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "markets", "crypto", 3600)
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
        store.save_meta("btc", "markets", "crypto", 3600, "ohlcv")
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


class TestAnomaliesAPI:
    def test_anomalies_not_found(self, client: TestClient) -> None:
        r = client.get("/signals/nonexistent/anomalies")
        assert r.status_code == 404

    def test_anomalies_empty(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "markets", "crypto", 3600)
        r = client.get("/signals/btc/anomalies")
        assert r.status_code == 200
        assert r.json()["anomalies"] == []

    def test_anomalies_detected(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("btc", "markets", "crypto", 3600)
        # Seed history with small variance
        hist = pd.DataFrame({
            "timestamp": [f"2024-01-{i+1:02d}" for i in range(20)],
            "value": [100.0 + i * 0.1 for i in range(20)],
        })
        store.save("btc", hist)
        # Add extreme outlier as latest
        outlier = pd.DataFrame({"timestamp": ["2024-02-01"], "value": [999999.0]})
        store.save("btc", outlier)
        r = client.get("/signals/btc/anomalies")
        assert r.status_code == 200
        data = r.json()
        assert len(data["anomalies"]) == 1
        assert data["anomalies"][0]["z_score"] > 4.0

    def test_anomalies_auto_lookback_default(self, client: TestClient, store: SignalStore) -> None:
        """Without explicit lookback, auto-calculate from meta interval."""
        store.save_meta("daily_sig", "markets", "equity", 86400)
        hist = pd.DataFrame({
            "timestamp": [f"2024-01-{i+1:02d}" for i in range(20)],
            "value": [100.0 + i * 0.1 for i in range(20)],
        })
        store.save("daily_sig", hist)
        r = client.get("/signals/daily_sig/anomalies")
        assert r.status_code == 200
        assert r.json()["name"] == "daily_sig"

    def test_anomalies_explicit_lookback(self, client: TestClient, store: SignalStore) -> None:
        """Explicit lookback should override auto-calculation."""
        store.save_meta("btc", "markets", "crypto", 3600)
        hist = pd.DataFrame({
            "timestamp": [f"2024-01-{i+1:02d}" for i in range(20)],
            "value": [100.0 + i * 0.1 for i in range(20)],
        })
        store.save("btc", hist)
        r = client.get("/signals/btc/anomalies?lookback=50")
        assert r.status_code == 200
        assert r.json()["name"] == "btc"

    def test_anomalies_auto_lookback_weekly(self, client: TestClient, store: SignalStore) -> None:
        """Weekly signal (604800s) should get lookback = max(100, 7*30) = 210."""
        store.save_meta("weekly_sig", "economy", "economic", 604800)
        r = client.get("/signals/weekly_sig/anomalies")
        assert r.status_code == 200
        assert r.json()["anomalies"] == []


class TestAuditAPI:
    def test_audit_empty(self, client: TestClient) -> None:
        r = client.get("/audit")
        assert r.status_code == 200
        assert r.json() == []

    def test_audit_with_events(self, client: TestClient, store: SignalStore) -> None:
        store.log_event("btc", "collected", rows=10)
        store.log_event("eth", "failed", detail="timeout")
        r = client.get("/audit")
        assert r.status_code == 200
        assert len(r.json()) == 2

    def test_audit_filter_by_name(self, client: TestClient, store: SignalStore) -> None:
        store.log_event("btc", "collected", rows=10)
        store.log_event("eth", "collected", rows=5)
        r = client.get("/audit?name=btc")
        assert r.status_code == 200
        data = r.json()
        assert len(data) == 1
        assert data[0]["name"] == "btc"


class TestResolutionAPI:
    def test_signal_data_with_resolution(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("s", "markets", "crypto", 3600)
        timestamps = pd.date_range("2024-01-01T00:00:00Z", periods=60, freq="min")
        df = pd.DataFrame({
            "timestamp": [ts.isoformat() for ts in timestamps],
            "value": range(60),
        })
        store.save("s", df)
        r = client.get("/signals/s/data?resolution=1h")
        assert r.status_code == 200
        data = r.json()
        assert len(data) == 1
        assert data[0]["value"] == 59

    def test_signal_data_without_resolution(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("s", "markets", "crypto", 3600)
        df = pd.DataFrame({
            "timestamp": ["2024-01-01T00:00:00", "2024-01-01T01:00:00"],
            "value": [1.0, 2.0],
        })
        store.save("s", df)
        r = client.get("/signals/s/data")
        assert r.status_code == 200
        assert len(r.json()) == 2

    def test_batch_with_resolution(self, client: TestClient, store: SignalStore) -> None:
        store.save_meta("s", "markets", "crypto", 3600)
        timestamps = pd.date_range("2024-01-01T00:00:00Z", periods=4, freq="h")
        df = pd.DataFrame({
            "timestamp": [ts.isoformat() for ts in timestamps],
            "value": [10.0, 20.0, 30.0, 40.0],
        })
        store.save("s", df)
        r = client.post("/signals/batch", json={
            "names": ["s"],
            "resolution": "4h",
        })
        assert r.status_code == 200
        data = r.json()
        assert len(data["s"]) == 1
        assert data["s"][0]["value"] == 40.0
