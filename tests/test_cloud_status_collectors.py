"""Tests for cloud outage/status collectors."""
from __future__ import annotations

from unittest.mock import patch

from signal_noise.collector.cloud_status import (
    CloudIncidentCount24hCollector,
    CloudMajorStateCollector,
    CloudRecoveryTimeDistributionCollector,
)


def _snapshot(active: int, recent: int, durations: list[float]) -> dict:
    return {
        "aws": {"active": active, "recent_24h": recent, "durations_h": durations},
        "gcp": {"active": 0, "recent_24h": 0, "durations_h": []},
        "azure": {"active": 0, "recent_24h": 0, "durations_h": []},
        "cloudflare": {"active": 0, "recent_24h": 0, "durations_h": []},
        "openai": {"active": 0, "recent_24h": 0, "durations_h": []},
    }


def test_cloud_major_state_normal():
    with patch("signal_noise.collector.cloud_status._compute_provider_snapshot") as snap:
        snap.return_value = _snapshot(active=0, recent=0, durations=[])
        df = CloudMajorStateCollector().fetch()
    assert df.iloc[0]["value"] == 0.0


def test_cloud_major_state_degraded():
    with patch("signal_noise.collector.cloud_status._compute_provider_snapshot") as snap:
        snap.return_value = _snapshot(active=2, recent=4, durations=[1.2, 3.4])
        df = CloudMajorStateCollector().fetch()
    assert df.iloc[0]["value"] == 1.0


def test_cloud_major_state_severe():
    s = _snapshot(active=2, recent=1, durations=[])
    s["gcp"]["active"] = 1
    s["openai"]["active"] = 1
    with patch("signal_noise.collector.cloud_status._compute_provider_snapshot", return_value=s):
        df = CloudMajorStateCollector().fetch()
    assert df.iloc[0]["value"] == 2.0


def test_cloud_incident_count_24h():
    s = _snapshot(active=1, recent=3, durations=[])
    s["gcp"]["recent_24h"] = 2
    s["cloudflare"]["recent_24h"] = 1
    with patch("signal_noise.collector.cloud_status._compute_provider_snapshot", return_value=s):
        df = CloudIncidentCount24hCollector().fetch()
    assert df.iloc[0]["value"] == 6.0


def test_cloud_recovery_distribution():
    s = _snapshot(active=0, recent=0, durations=[1.0, 2.0, 3.0, 10.0])
    s["gcp"]["durations_h"] = [4.0, 5.0]
    with patch("signal_noise.collector.cloud_status._compute_provider_snapshot", return_value=s):
        df = CloudRecoveryTimeDistributionCollector().fetch()
    payload = df.iloc[0]["payload"]
    assert payload["n"] == 6
    assert payload["p99_h"] >= payload["p90_h"] >= payload["p50_h"]


def test_cloud_recovery_distribution_empty():
    with patch("signal_noise.collector.cloud_status._compute_provider_snapshot", return_value=_snapshot(0, 0, [])):
        df = CloudRecoveryTimeDistributionCollector().fetch()
    assert df.iloc[0]["payload"]["n"] == 0


def test_cloud_collectors_registered():
    from signal_noise.collector import COLLECTORS

    for name in [
        "cloud_major_state",
        "cloud_incident_count_24h",
        "cloud_recovery_time_distribution",
    ]:
        assert name in COLLECTORS, f"{name} not registered"
