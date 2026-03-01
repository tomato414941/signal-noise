# signal-noise

Collect worldwide time series and deliver via REST API.

> "even noise is worth collecting -- the signal hides within"

## Terminology

| Term | Definition | Example |
|------|-----------|---------|
| **Provider** | External API / service | Yahoo Finance, FRED, CoinGecko |
| **Collector** | Class that fetches one time series | `BtcOhlcvCollector`, `FearGreedCollector` |
| **CollectorMeta** | Collector metadata (name, domain, category, interval) | `CollectorMeta(name="fear_greed", ...)` |
| **Signal** | A raw time series delivered via API | `fear_greed`, `btc_ohlcv` |
| **Domain** | Stable top-level grouping (10 types) | financial, earth, macro |
| **Category** | Concrete classification (~29 types) | equity, weather, labor |

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
| developer | GitHub, npm, StackOverflow, package registries | developer |
| health | Epidemiology, public health, disease surveillance | epidemiology, public_health |
| computed | Calculated features (temporal, etc.) | temporal |

### Scale

- **Providers**: ~40 external APIs
- **Collectors**: ~1,214 time series

## Architecture

```
Scheduler ──→ Collector ──→ SQLite Store ──→ REST API ──→ Consumer
(per-collector   (API        (WAL mode)      (FastAPI)
 frequency)       fetch)
```

### Modules

| Module | Role |
|--------|------|
| `collector/base.py` | `BaseCollector` ABC, `CollectorMeta`, taxonomy constants |
| `collector/__init__.py` | Collector registry (`COLLECTORS` dict), `collect_all()` |
| `collector/*.py` | Individual collector implementations |
| `store/sqlite_store.py` | `SignalStore` — SQLite WAL storage for time series + metadata |
| `store/migration.py` | Parquet → SQLite migration |
| `scheduler/loop.py` | asyncio scheduler — per-collector intervals |
| `api/app.py` | FastAPI REST API for data delivery |
| `cli.py` | CLI entrypoint |
| `config.py` | Paths, `CollectorConfig` |

## CLI

```bash
python -m signal_noise collect              # Fetch all collectors (SQLite)
python -m signal_noise collect -s fear_greed # Fetch specific collector
python -m signal_noise collect --parquet    # Use legacy Parquet storage
python -m signal_noise list                 # List collectors with status
python -m signal_noise count                # Show collector count
python -m signal_noise serve                # Start scheduler + REST API
python -m signal_noise serve --migrate      # Import Parquet data first
python -m signal_noise serve --no-scheduler # API only
```

## REST API

```
GET /health                        # Service health check
GET /signals                       # List all signals (name, domain, category, ...)
GET /signals/{name}                # Signal metadata
GET /signals/{name}/data?since=... # Time series data (timestamp, value)
GET /signals/{name}/latest         # Latest value + timestamp
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
