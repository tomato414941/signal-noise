# signal-noise

Collect worldwide signals and evaluate predictive power against any target time series.

> "even noise is worth collecting -- the signal hides within"

## Terminology

| Term | Definition | Example |
|------|-----------|---------|
| **Provider** | External API / service | Yahoo Finance, FRED, CoinGecko |
| **Collector** | Class that fetches one time series | `BtcOhlcvCollector`, `FearGreedCollector` |
| **CollectorMeta** | Collector metadata (name, domain, category) | `CollectorMeta(name="fear_greed", ...)` |
| **Signal** | Data entering the evaluation pipeline (raw or transformed) | `fear_greed`, `fear_greed__z_20` |
| **Transform** | Function applied to a raw signal to derive a new signal | z-score, SMA ratio, RSI |
| **Domain** | Stable top-level grouping (9 types) | financial, earth, macro |
| **Category** | Concrete classification (~27 types) | equity, weather, labor |

### Domain List

| Domain | Description | Categories |
|--------|-----------|------------|
| financial | Equities, FX, bonds, commodities, crypto | equity, crypto, forex, rates, commodity |
| macro | GDP, employment, inflation, trade, fiscal | economic, labor, inflation, trade, fiscal |
| sentiment | Sentiment indices, social media, attention | sentiment, attention |
| earth | Weather, climate, ocean, air quality, hydrology | weather, climate, marine, air_quality, hydrology, satellite |
| geophysical | Solar, geomagnetic, seismic, lunar | space_weather, seismic, celestial |
| infrastructure | Logistics, aviation, internet | logistics, aviation, internet |
| real_estate | Housing, property prices | real_estate |
| developer | GitHub, npm, StackOverflow | developer |
| computed | Calculated features (temporal, etc.) | temporal |

### Scale

- **Providers**: ~58 external APIs
- **Collectors**: ~1,000 time series
- **Transforms**: 22 functions
- **Signals**: ~23,000 (collectors × (1 + transforms))

## Architecture

```
Provider (API) → Collector → Parquet/Cache → Evaluator → Report
                                               ↑
                                          Transforms
```

### Modules

| Module | Role |
|--------|------|
| `collector/base.py` | `BaseCollector` ABC, `CollectorMeta`, taxonomy constants |
| `collector/__init__.py` | Collector registry (`COLLECTORS` dict), `collect_all()` |
| `collector/*.py` | Individual collector implementations |
| `evaluator/pipeline.py` | Main evaluation loop (align signals, compute metrics) |
| `evaluator/metrics.py` | `SignalMetrics`, IC/Pearson/directional accuracy |
| `evaluator/returns.py` | Forward return computation |
| `evaluator/corrections.py` | Multiple testing correction (FDR, Bonferroni) |
| `transforms.py` | Signal transform functions (z-score, SMA, RSI, etc.) |
| `reporter/report.py` | Text/JSON report generation |
| `cli.py` | CLI entrypoint |
| `config.py` | Paths, `CollectorConfig`, `EvaluationConfig` |

## CLI

```bash
python -m signal_noise collect              # Fetch all collectors
python -m signal_noise collect -s fear_greed # Fetch specific collector
python -m signal_noise evaluate             # Run evaluation pipeline
python -m signal_noise evaluate --top 20    # Show top 20 signals
python -m signal_noise report               # Show latest report
python -m signal_noise list                 # List collectors with status
python -m signal_noise count                # Show signal count
```

## Adding a New Collector

1. Create `src/signal_noise/collector/<name>.py`
2. Subclass `BaseCollector`, define `meta: CollectorMeta` with correct `domain` and `category`
3. Implement `fetch()` → return `DataFrame` with `[timestamp|date, value]` columns
4. Register in `collector/__init__.py` (`COLLECTORS` dict or via factory function)
5. Add tests in `tests/`

## Development

```bash
pip install -e ".[dev]"
pytest tests/ -v
ruff check src/ tests/
```
