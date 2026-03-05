"""Tests for state/distribution probe collectors."""
from __future__ import annotations

from unittest.mock import patch

from signal_noise.collector.probe_network import (
    ProbeDnsStateCollector,
    ProbeHttpDistributionCollector,
    ProbeNetworkStateCollector,
    ProbePingDistributionCollector,
    get_probe_collectors,
)


def test_probe_network_state_normal():
    with patch("signal_noise.collector.probe_network._http_response_ms") as mock_http:
        mock_http.side_effect = [120.0, 180.0, 210.0]
        df = ProbeNetworkStateCollector().fetch()
    assert df.iloc[0]["value"] == 0.0
    assert df.iloc[0]["payload"]["ok_count"] == 3


def test_probe_network_state_degraded():
    with patch("signal_noise.collector.probe_network._http_response_ms") as mock_http:
        mock_http.side_effect = [120.0, None, 1900.0]
        df = ProbeNetworkStateCollector().fetch()
    assert df.iloc[0]["value"] == 1.0
    assert df.iloc[0]["payload"]["ok_count"] == 2


def test_probe_network_state_down():
    with patch("signal_noise.collector.probe_network._http_response_ms") as mock_http:
        mock_http.side_effect = [None, None, None]
        df = ProbeNetworkStateCollector().fetch()
    assert df.iloc[0]["value"] == 2.0


def test_probe_ping_distribution_with_samples():
    with patch("signal_noise.collector.probe_network._ping_samples_ms") as mock_ping:
        mock_ping.side_effect = [
            [10.0, 12.0, 11.0],
            [20.0, 21.0, 22.0],
            [30.0, 31.0, 29.0],
        ]
        df = ProbePingDistributionCollector().fetch()
    assert df.iloc[0]["value"] > 0.0
    payload = df.iloc[0]["payload"]
    assert payload["n"] == 9
    assert payload["p99"] >= payload["p90"] >= payload["p50"]


def test_probe_ping_distribution_empty():
    with patch("signal_noise.collector.probe_network._ping_samples_ms") as mock_ping:
        mock_ping.side_effect = [[], [], []]
        df = ProbePingDistributionCollector().fetch()
    assert df.iloc[0]["payload"]["n"] == 0


def test_probe_collectors_include_new_signal_types():
    collectors = get_probe_collectors()
    assert "probe_network_state" in collectors
    assert "probe_ping_distribution_core" in collectors
    assert "probe_dns_state" in collectors
    assert "probe_http_distribution_core" in collectors
    assert collectors["probe_network_state"].meta.signal_type == "state"
    assert collectors["probe_dns_state"].meta.signal_type == "state"
    assert collectors["probe_ping_distribution_core"].meta.signal_type == "distribution"
    assert collectors["probe_http_distribution_core"].meta.signal_type == "distribution"


def test_probe_dns_state_normal():
    with patch("signal_noise.collector.probe_network._dns_resolve_ms") as mock_dns:
        mock_dns.side_effect = [20.0, 18.0, 22.0, 25.0]
        df = ProbeDnsStateCollector().fetch()
    assert df.iloc[0]["value"] == 0.0


def test_probe_dns_state_down():
    with patch("signal_noise.collector.probe_network._dns_resolve_ms") as mock_dns:
        mock_dns.side_effect = [None, None, None, None]
        df = ProbeDnsStateCollector().fetch()
    assert df.iloc[0]["value"] == 2.0


def test_probe_http_distribution_with_samples():
    with patch("signal_noise.collector.probe_network._http_response_ms") as mock_http:
        mock_http.side_effect = [120.0, 110.0, 130.0, 200.0, 150.0]
        df = ProbeHttpDistributionCollector().fetch()
    payload = df.iloc[0]["payload"]
    assert payload["n"] == 5
    assert payload["p99"] >= payload["p90"] >= payload["p50"]


def test_probe_http_distribution_empty():
    with patch("signal_noise.collector.probe_network._http_response_ms") as mock_http:
        mock_http.side_effect = [None, None, None, None, None]
        df = ProbeHttpDistributionCollector().fetch()
    assert df.iloc[0]["payload"]["n"] == 0
