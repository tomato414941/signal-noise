from __future__ import annotations

from unittest.mock import patch

from signal_noise.collector.world_bank import get_wb_collectors


def test_world_bank_collectors_registered():
    collectors = get_wb_collectors()
    assert "wb_internet_users_pct" in collectors
    assert "wb_gdp_growth" in collectors


@patch("signal_noise.collector.world_bank._fetch_worldbank_json")
def test_world_bank_fetch_uses_shared_helper(mock_fetch):
    mock_fetch.return_value = [
        {"page": 1, "pages": 1, "per_page": 100, "total": 2},
        [
            {"date": "2024", "value": 68.5},
            {"date": "2023", "value": 66.1},
        ],
    ]

    cls = get_wb_collectors()["wb_internet_users_pct"]
    df = cls().fetch()

    assert len(df) == 2
    assert df["value"].iloc[-1] == 68.5
