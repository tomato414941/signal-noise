"""Tests for additional society expansion (education/legislation/armed_conflict)."""
from __future__ import annotations

from unittest.mock import patch

from signal_noise.collector.acled import ACLED_SERIES, get_acled_collectors
from signal_noise.collector.congress import get_congress_collectors
from signal_noise.collector.worldbank_generic import WORLDBANK_SERIES


def test_congress_collectors_include_new_legislation_series():
    collectors = get_congress_collectors()
    for name in [
        "congress_hres_count",
        "congress_sres_count",
        "congress_hconres_count",
        "congress_sconres_count",
    ]:
        assert name in collectors
        assert collectors[name].meta.category == "legislation"


def test_worldbank_series_include_new_education_entries():
    names = {t[2] for t in WORLDBANK_SERIES}
    for name in [
        "wb_sec_enrollment_us",
        "wb_sec_enrollment_in",
        "wb_ter_enrollment_cn",
        "wb_literacy_br",
    ]:
        assert name in names


def test_acled_factory_count_and_category():
    collectors = get_acled_collectors()
    assert len(collectors) == len(ACLED_SERIES)
    for name, _, _ in ACLED_SERIES:
        assert name in collectors
        assert collectors[name].meta.category == "armed_conflict"


@patch("signal_noise.collector.acled._load_rows")
def test_acled_event_type_filter(mock_rows):
    mock_rows.return_value = [
        {"event_date": "2025-01-01", "event_type": "Battles"},
        {"event_date": "2025-01-02", "event_type": "Riots"},
        {"event_date": "2025-01-03", "event_type": "Battles"},
    ]
    cls = get_acled_collectors()["acled_battles_global"]
    df = cls().fetch()
    assert len(df) >= 1
    assert float(df["value"].sum()) == 2.0


@patch("signal_noise.collector.acled._load_rows")
def test_acled_global_no_filter(mock_rows):
    mock_rows.return_value = [
        {"event_date": "2025-01-01", "event_type": "Battles"},
        {"event_date": "2025-01-02", "event_type": "Riots"},
    ]
    cls = get_acled_collectors()["acled_events_global"]
    df = cls().fetch()
    assert float(df["value"].sum()) == 2.0


def test_registered_new_society_collectors():
    from signal_noise.collector import COLLECTORS

    expected = [
        "congress_hres_count",
        "congress_sres_count",
        "wb_sec_enrollment_us",
        "wb_ter_enrollment_us",
        "wb_literacy_in",
        "acled_battles_global",
        "acled_violence_civilians_global",
        "acled_explosions_global",
        "acled_riots_global",
    ]
    for name in expected:
        assert name in COLLECTORS, f"{name} not registered"
