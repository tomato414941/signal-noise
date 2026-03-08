from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from signal_noise.collector.inaturalist import (
    INATURALIST_SERIES,
    get_inaturalist_collectors,
    _make_inaturalist_collector,
)


class TestINaturalistFactory:
    def test_series_count(self):
        assert len(INATURALIST_SERIES) >= 8

    def test_no_duplicate_names(self):
        names = [entry[2] for entry in INATURALIST_SERIES]
        assert len(names) == len(set(names))

    def test_get_collectors_returns_dict(self):
        collectors = get_inaturalist_collectors()
        assert len(collectors) == len(INATURALIST_SERIES)

    def test_selected_collectors_are_registered(self):
        from signal_noise.collector import COLLECTORS

        for name, category in [
            ("inaturalist_observations_birds", "wildlife"),
            ("inaturalist_observations_reptiles", "wildlife"),
            ("inaturalist_species_total", "biodiversity"),
            ("inaturalist_species_insects", "biodiversity"),
        ]:
            assert name in COLLECTORS
            assert COLLECTORS[name].meta.category == category


class TestINaturalistFetch:
    @patch("signal_noise.collector.inaturalist.requests.get")
    def test_fetch_parses_total_results(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = {"total_results": 12345, "results": []}
        mock_get.return_value = mock_resp

        cls = _make_inaturalist_collector(
            "observations/species_counts",
            "Plantae",
            "test_inaturalist",
            "Test iNaturalist",
            "biodiversity",
        )
        df = cls().fetch()

        assert len(df) == 1
        assert df["value"].iloc[0] == 12345
        _, kwargs = mock_get.call_args
        assert kwargs["params"]["iconic_taxa"] == "Plantae"
        assert kwargs["params"]["verifiable"] == "true"

    @patch("signal_noise.collector.inaturalist.requests.get")
    def test_missing_total_results_raises(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = {"results": []}
        mock_get.return_value = mock_resp

        cls = _make_inaturalist_collector(
            "observations",
            None,
            "test_inaturalist",
            "Test iNaturalist",
            "wildlife",
        )
        with pytest.raises(RuntimeError, match="No iNaturalist total_results"):
            cls().fetch()
