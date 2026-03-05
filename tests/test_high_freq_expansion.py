"""Tests for high-frequency collector expansions."""
from __future__ import annotations

from datetime import datetime, timezone

from signal_noise.collector.binance_ws import (
    _compute_orderbook_signals,
    get_binance_ws_collectors,
)
from signal_noise.collector.open_meteo_realtime import (
    REALTIME_CITIES,
    get_realtime_collectors,
)
from signal_noise.collector.probe_network import (
    DNS_TARGETS,
    HTTP_TARGETS,
    PING_TARGETS,
    get_probe_collectors,
)


def test_binance_ws_collectors_include_eth_sol_bnb_xrp():
    collectors = get_binance_ws_collectors()
    expected = {
        "liq_stream_btc", "funding_rate_stream_btc", "orderbook_btc", "trade_flow_btc",
        "liq_stream_eth", "funding_rate_stream_eth", "orderbook_eth", "trade_flow_eth",
        "liq_stream_sol", "funding_rate_stream_sol", "orderbook_sol", "trade_flow_sol",
        "liq_stream_bnb", "funding_rate_stream_bnb", "orderbook_bnb", "trade_flow_bnb",
        "liq_stream_xrp", "funding_rate_stream_xrp", "orderbook_xrp", "trade_flow_xrp",
    }
    for name in expected:
        assert name in collectors
        assert collectors[name].meta.interval == 60


def test_orderbook_signal_names_support_asset_suffix():
    ts = datetime(2026, 3, 1, 10, 0, tzinfo=timezone.utc)
    snapshot = {
        "b": [["2000", "3.0"], ["1999", "2.0"]],
        "a": [["2001", "1.0"], ["2002", "4.0"]],
    }
    rows = _compute_orderbook_signals(ts, snapshot, "eth")
    names = {r["name"] for r in rows}
    assert names == {"book_imbalance_eth", "book_depth_ratio_eth", "spread_bps_eth"}


def test_probe_collectors_include_new_targets():
    collectors = get_probe_collectors()
    aggregate_collectors = {
        "probe_network_state",
        "probe_ping_distribution_core",
        "probe_dns_state",
        "probe_http_distribution_core",
    }
    assert len(collectors) == (
        len(PING_TARGETS)
        + len(DNS_TARGETS)
        + len(HTTP_TARGETS)
        + len(aggregate_collectors)
    )
    for name in [
        "probe_ping_quad9_dns",
        "probe_ping_opendns",
        "probe_ping_comodo_dns",
        "probe_ping_adguard_dns",
        "probe_dns_openai",
        "probe_dns_wikipedia",
        "probe_dns_reddit",
        "probe_dns_microsoft",
        "probe_http_openai",
        "probe_http_wikipedia",
        "probe_http_reddit",
        "probe_http_microsoft",
    ]:
        assert name in collectors
        assert collectors[name].meta.interval == 300
    for name in aggregate_collectors:
        assert name in collectors
        assert collectors[name].meta.interval == 300


def test_realtime_collectors_include_added_cities():
    collectors = get_realtime_collectors()
    assert len(collectors) == len(REALTIME_CITIES)
    for key in [
        "realtime_temp_la",
        "realtime_temp_toronto",
        "realtime_temp_amsterdam",
        "realtime_temp_jakarta",
        "realtime_temp_mumbai",
        "realtime_temp_houston",
        "realtime_temp_istanbul",
        "realtime_temp_bangkok",
        "realtime_temp_buenos_aires",
        "realtime_temp_stockholm",
        "realtime_temp_miami",
        "realtime_temp_vancouver",
        "realtime_temp_rome",
        "realtime_temp_helsinki",
        "realtime_temp_tel_aviv",
        "realtime_temp_doha",
        "realtime_temp_nairobi",
        "realtime_temp_auckland",
    ]:
        assert key in collectors
