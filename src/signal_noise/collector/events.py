from __future__ import annotations

import pandas as pd

from signal_noise.collector.base import BaseCollector, SourceMeta

# Major global events that absorb mass attention.
# Format: (start_date, end_date) inclusive, spanning 2022-2026+.
# Value = 1 during event, 0 otherwise.

_WORLD_CUP = [
    # FIFA Men's World Cup 2022 (Qatar)
    ("2022-11-20", "2022-12-18"),
    # FIFA Women's World Cup 2023 (Australia/NZ)
    ("2023-07-20", "2023-08-20"),
    # FIFA Men's World Cup 2026 (USA/Mexico/Canada)
    ("2026-06-11", "2026-07-19"),
]

_OLYMPICS = [
    # Beijing Winter 2022
    ("2022-02-04", "2022-02-20"),
    # Paris Summer 2024
    ("2024-07-26", "2024-08-11"),
    # Milano-Cortina Winter 2026
    ("2026-02-06", "2026-02-22"),
    # LA Summer 2028
    ("2028-07-14", "2028-07-30"),
]

_SUPER_BOWL = [
    # Super Bowl dates (single day events, but hype week matters)
    ("2022-02-13", "2022-02-13"),
    ("2023-02-12", "2023-02-12"),
    ("2024-02-11", "2024-02-11"),
    ("2025-02-09", "2025-02-09"),
    ("2026-02-08", "2026-02-08"),
]

_EURO = [
    # UEFA Euro 2024 (Germany)
    ("2024-06-14", "2024-07-14"),
]

_MAJOR_EVENTS = _WORLD_CUP + _OLYMPICS + _SUPER_BOWL + _EURO


def _build_event_series(
    events: list[tuple[str, str]], years: int = 4
) -> pd.DataFrame:
    dates = pd.date_range(
        end=pd.Timestamp.now(tz="UTC").normalize(),
        periods=365 * years,
        freq="D",
    )
    values = pd.Series(0.0, index=dates)
    for start, end in events:
        mask = (dates >= pd.Timestamp(start, tz="UTC")) & (
            dates <= pd.Timestamp(end, tz="UTC")
        )
        values[mask] = 1.0
    return pd.DataFrame({"date": dates, "value": values.values})


class MajorSportsEventCollector(BaseCollector):
    """Binary flag: 1 = major global sporting event in progress, 0 = none.

    Covers FIFA World Cup, Olympics, Super Bowl, UEFA Euro.
    Hypothesis: mass attention diversion reduces market participation.
    """

    meta = SourceMeta(
        name="major_sports_event",
        display_name="Major Sports Event (0/1)",
        update_frequency="daily",
        data_type="entertainment",
        api_docs_url="N/A",
        domain="sentiment",
        category="attention",
    )

    def fetch(self) -> pd.DataFrame:
        return _build_event_series(_MAJOR_EVENTS)


class SuperBowlCollector(BaseCollector):
    """Binary flag for Super Bowl day.

    Single biggest TV event in the US — ~115M viewers.
    """

    meta = SourceMeta(
        name="super_bowl",
        display_name="Super Bowl Day (0/1)",
        update_frequency="daily",
        data_type="entertainment",
        api_docs_url="N/A",
        domain="sentiment",
        category="attention",
    )

    def fetch(self) -> pd.DataFrame:
        return _build_event_series(_SUPER_BOWL)


class OlympicsCollector(BaseCollector):
    """Binary flag: 1 during Olympic Games."""

    meta = SourceMeta(
        name="olympics",
        display_name="Olympic Games (0/1)",
        update_frequency="daily",
        data_type="entertainment",
        api_docs_url="N/A",
        domain="sentiment",
        category="attention",
    )

    def fetch(self) -> pd.DataFrame:
        return _build_event_series(_OLYMPICS)
