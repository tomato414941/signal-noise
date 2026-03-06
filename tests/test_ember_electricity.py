from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from signal_noise.collector.ember_electricity import _ember_cache, _get_ember_data, get_ember_collectors


_SAMPLE_CSV = """Area,Year,Variable,Unit,Value
World,2022,Coal,%,35.41
World,2023,Coal,%,35.04
World,2024,Solar,%,6.93
France,2024,Coal,%,2.10
"""


@patch("signal_noise.collector.ember_electricity.requests.get")
def test_get_ember_data_filters_world_rows(mock_get):
    _ember_cache.clear()
    response = MagicMock()
    response.raise_for_status = MagicMock()
    response.text = _SAMPLE_CSV
    mock_get.return_value = response

    rows = _get_ember_data(timeout=10)

    assert len(rows) == 3
    assert all(row["Area"] == "World" for row in rows)
    _ember_cache.clear()


@patch("signal_noise.collector.ember_electricity.requests.get")
def test_ember_collector_fetch_filters_variable(mock_get):
    _ember_cache.clear()
    response = MagicMock()
    response.raise_for_status = MagicMock()
    response.text = _SAMPLE_CSV
    mock_get.return_value = response

    cls = get_ember_collectors()["ember_coal_share"]
    df = cls().fetch()

    assert len(df) == 2
    assert df["value"].iloc[0] == pytest.approx(35.41)
    assert df["value"].iloc[-1] == pytest.approx(35.04)
    _ember_cache.clear()


def test_ember_collectors_registered():
    collectors = get_ember_collectors()
    assert set(collectors) == {
        "ember_coal_share",
        "ember_gas_share",
        "ember_nuclear_share",
        "ember_wind_share",
        "ember_solar_share",
        "ember_hydro_share",
    }
