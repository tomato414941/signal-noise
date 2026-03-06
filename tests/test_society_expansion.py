"""Tests for society-category expansions."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from signal_noise.collector.nyc_311 import (
    NYC_311_SERIES,
    get_nyc311_collectors,
)
from signal_noise.collector.owid_excess_mortality import (
    OWID_EXCESS_SERIES,
    get_owid_excess_collectors,
)
from signal_noise.collector.unhcr import (
    UNHCR_SERIES,
    get_unhcr_collectors,
)
from signal_noise.collector.worldbank_generic import WORLDBANK_SERIES


def test_unhcr_collectors_count_and_category():
    collectors = get_unhcr_collectors()
    assert len(collectors) == len(UNHCR_SERIES)
    for _, name, _, _ in UNHCR_SERIES:
        assert name in collectors
        assert collectors[name].meta.category == "displacement"


@patch("signal_noise.collector.unhcr.requests.get")
def test_unhcr_fetch_parses_fields(mock_get):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "items": [
            {"year": 2022, "refugees": 100, "idps": 200, "asylum_seekers": 30, "ooc": 10, "oip": 5},
            {"year": 2023, "refugees": 110, "idps": 220, "asylum_seekers": 35, "ooc": 12, "oip": 7},
        ]
    }
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp
    cls = get_unhcr_collectors()["unhcr_displaced"]
    df = cls().fetch()
    assert len(df) == 2
    assert df["value"].iloc[0] == 345.0


@patch("signal_noise.collector.unhcr.requests.get")
def test_unhcr_forced_total_handles_dash_values(mock_get):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "items": [
            {"year": 2023, "refugees": 10, "idps": 20, "asylum_seekers": 5, "ooc": 1, "oip": "-"},
        ]
    }
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    cls = get_unhcr_collectors()["unhcr_ukraine_displaced"]
    df = cls().fetch()
    assert df["value"].iloc[0] == 36.0


@patch("signal_noise.collector.unhcr.requests.get")
def test_unhcr_country_filter_sets_params(mock_get):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "items": [
            {"year": 2023, "refugees": 10, "idps": 20, "asylum_seekers": 5, "ooc": 0, "oip": 0},
        ]
    }
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp

    cls = get_unhcr_collectors()["unhcr_ukraine_displaced"]
    df = cls().fetch()

    assert len(df) == 1
    params = mock_get.call_args.kwargs["params"]
    assert params["coo"] == "UKR"
    assert params["cf_type"] == "ISO"


def test_owid_excess_collectors_count_and_category():
    collectors = get_owid_excess_collectors()
    assert len(collectors) == len(OWID_EXCESS_SERIES)
    for _, name, _ in OWID_EXCESS_SERIES:
        assert name in collectors
        assert collectors[name].meta.category == "excess_deaths"


@patch("signal_noise.collector.owid_excess_mortality.requests.get")
def test_owid_excess_fetch(mock_get):
    csv_text = (
        "location,date,p_scores_all_ages\n"
        "World,2025-01-01,10.0\n"
        "World,2025-01-08,12.0\n"
        "United States,2025-01-01,8.0\n"
    )
    mock_resp = MagicMock()
    mock_resp.text = csv_text
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp
    cls = get_owid_excess_collectors()["owid_excess_mortality"]
    df = cls().fetch()
    assert len(df) == 2
    assert df["value"].iloc[0] == 10.0


def test_nyc311_collectors_count_and_category():
    collectors = get_nyc311_collectors()
    assert len(collectors) == len(NYC_311_SERIES)
    for name, _, _ in NYC_311_SERIES:
        assert name in collectors
        assert collectors[name].meta.category == "city_stats"


@patch("signal_noise.collector.nyc_311.requests.get")
def test_nyc311_fetch(mock_get):
    mock_resp = MagicMock()
    mock_resp.json.return_value = [
        {"day": "2025-01-01T00:00:00.000", "cnt": "100"},
        {"day": "2025-01-02T00:00:00.000", "cnt": "120"},
    ]
    mock_resp.raise_for_status = MagicMock()
    mock_get.return_value = mock_resp
    cls = get_nyc311_collectors()["nyc_311_noise"]
    df = cls().fetch()
    assert len(df) == 2
    assert df["value"].iloc[1] == 120


def test_worldbank_series_has_new_society_entries():
    names = {t[2] for t in WORLDBANK_SERIES}
    expected = {
        "wb_forced_displaced_world",
        "wb_idp_world",
        "wb_asylum_world",
        "wb_adult_mort_female_world",
        "wb_death_injury_share_us",
    }
    for name in expected:
        assert name in names


def test_society_collectors_registered():
    from signal_noise.collector import COLLECTORS

    expected = [
        "unhcr_refugees",
        "unhcr_idps",
        "unhcr_returned_refugees",
        "unhcr_ukraine_displaced",
        "owid_excess_mortality_us",
        "owid_excess_mortality_jp",
        "nyc_311_noise",
        "nyc_311_heat_hot_water",
        "wb_forced_displaced_world",
        "wb_adult_mort_male_world",
    ]
    for name in expected:
        assert name in COLLECTORS, f"{name} not registered"
