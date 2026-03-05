"""Tests for infra weird collectors."""
from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch

from signal_noise.collector.infra_weird import (
    APIErrorSurface24hCollector,
    AirportDisruptionStateCollector,
    BGPPathInstabilityIndexCollector,
    CDNDivergenceStateCollector,
    DNSSECValidationStateCollector,
    DependencyBlastRadiusCollector,
    EnergyGridStressDistributionCollector,
    PaymentRailStateCollector,
    RepoSupplyChainAlertsCollector,
    TLSExpiryRiskDistributionCollector,
)


def test_dnssec_validation_state():
    with patch("signal_noise.collector.infra_weird._dnssec_validated") as fn:
        fn.side_effect = [True, True, False, True]
        df = DNSSECValidationStateCollector().fetch()
    assert df.iloc[0]["value"] == 1.0


def test_tls_expiry_distribution():
    with patch("signal_noise.collector.infra_weird._tls_days_to_expiry") as fn:
        fn.side_effect = [200.0, 150.0, 120.0, 80.0, 60.0, 30.0]
        df = TLSExpiryRiskDistributionCollector().fetch()
    payload = df.iloc[0]["payload"]
    assert payload["n"] == 6
    assert payload["p10_days"] <= payload["p50_days"] <= payload["p90_days"]


def test_bgp_instability_distribution():
    with patch("signal_noise.collector.infra_weird._bgp_updates_count") as fn:
        fn.side_effect = [100, 120, 80, 150]
        df = BGPPathInstabilityIndexCollector().fetch()
    payload = df.iloc[0]["payload"]
    assert payload["n"] == 4
    assert payload["p90"] >= payload["p50"]


def test_cdn_divergence_state():
    with patch("signal_noise.collector.infra_weird._http_ms") as fn:
        fn.side_effect = [100.0, 350.0, 120.0]
        df = CDNDivergenceStateCollector().fetch()
    assert df.iloc[0]["value"] == 1.0


def test_api_error_surface_distribution():
    def fake_statuspage(url: str):
        if "unresolved" in url:
            return [{"id": "a"}]
        return [{"created_at": "2026-03-05T00:00:00Z"}]

    with patch("signal_noise.collector.infra_weird._statuspage_incidents", side_effect=fake_statuspage):
        df = APIErrorSurface24hCollector().fetch()
    payload = df.iloc[0]["payload"]
    assert payload["n"] == 3
    assert payload["total"] >= 0.0


def test_dependency_blast_radius():
    with patch("signal_noise.collector.infra_weird._statuspage_incidents") as sp, patch(
        "signal_noise.collector.infra_weird._get_json"
    ) as gj:
        sp.return_value = [{"id": "x"}, {"id": "y"}]
        gj.return_value = [{"end": None}, {"end": "2026-03-04T00:00:00Z"}]
        df = DependencyBlastRadiusCollector().fetch()
    payload = df.iloc[0]["payload"]
    assert payload["n"] == 4
    assert payload["providers_impacted"] >= 1


def test_payment_rail_state_down():
    with patch("signal_noise.collector.infra_weird._http_ms", return_value=None):
        df = PaymentRailStateCollector().fetch()
    assert df.iloc[0]["value"] == 2.0


def test_repo_supply_chain_alerts_scalar():
    now_iso = datetime.now(UTC).isoformat().replace("+00:00", "Z")

    with patch("signal_noise.collector.infra_weird._statuspage_incidents") as sp, patch(
        "signal_noise.collector.infra_weird._recent_python_incidents_24h"
    ) as pycnt:
        sp.return_value = [{"created_at": now_iso}]
        pycnt.return_value = 2
        df = RepoSupplyChainAlertsCollector().fetch()
    assert df.iloc[0]["value"] == 4.0


def test_airport_disruption_state():
    with patch("signal_noise.collector.infra_weird._faa_counts", return_value=(20, 3)):
        df = AirportDisruptionStateCollector().fetch()
    assert df.iloc[0]["value"] == 1.0


def test_energy_grid_stress_distribution():
    with patch(
        "signal_noise.collector.infra_weird._recent_uk_carbon_values_24h",
        return_value=[100.0, 200.0, 350.0, 400.0, 120.0],
    ):
        df = EnergyGridStressDistributionCollector().fetch()
    payload = df.iloc[0]["payload"]
    assert payload["n"] == 5
    assert payload["spike_count"] == 2


def test_infra_weird_collectors_registered():
    from signal_noise.collector import COLLECTORS

    expected = [
        "dnssec_validation_state",
        "tls_expiry_risk_distribution",
        "bgp_path_instability_index",
        "cdn_divergence_state",
        "api_error_surface_24h",
        "dependency_blast_radius",
        "payment_rail_state",
        "repo_supply_chain_alerts",
        "airport_disruption_state",
        "energy_grid_stress_distribution",
    ]
    for name in expected:
        assert name in COLLECTORS, f"{name} not registered"
