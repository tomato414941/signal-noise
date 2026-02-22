from __future__ import annotations

import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta

# Reference New Moon: 2000-01-06 18:14 UTC
_REF_NEW_MOON = pd.Timestamp("2000-01-06 18:14:00", tz="UTC")
_SYNODIC_MONTH = 29.53058770576  # days


class MoonPhaseCollector(BaseCollector):
    """Moon phase from 0.0 (new moon) through 0.5 (full moon) back to ~1.0.

    Pure computation — no API call needed.
    Academic reference: Dichev & Janes (2003) "Lunar Cycle Effects in Stock Returns"
    """

    meta = SourceMeta(
        name="moon_phase",
        display_name="Moon Phase (0=New, 0.5=Full)",
        update_frequency="daily",
        api_docs_url="N/A",
        domain="geophysical",
        category="celestial",
    )

    def fetch(self) -> pd.DataFrame:
        dates = pd.date_range(
            end=pd.Timestamp.now(tz="UTC").normalize(),
            periods=365 * 3,
            freq="D",
        )
        days_since = (dates - _REF_NEW_MOON).total_seconds() / 86400
        phase = (days_since % _SYNODIC_MONTH) / _SYNODIC_MONTH
        return pd.DataFrame({"date": dates, "value": phase})
