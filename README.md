# signal-noise

> "even noise is worth collecting -- the signal hides within"

Collect worldwide time series and deliver via REST API.

**1,256+ signals** across **10 domains** and **29 categories** — from stock prices and GDP to earthquake magnitudes and solar wind speed.

## Features

- **Broad coverage** — financial, macro, sentiment, earth, geophysical, infrastructure, real estate, developer, health, computed
- **Collection Spectrum** — L1 (free APIs) through L6 (physical sensors). See [DESIGN.md](DESIGN.md) for the full spectrum
- **REST API** — FastAPI with signal discovery, time series data, anomaly detection, and batch queries
- **Smart scheduling** — asyncio-based per-collector intervals with jitter and circuit breakers
- **SQLite storage** — WAL mode, append-efficient, range-queryable, no external database
- **Quality analysis** — health scoring, anomaly detection (z-score), spectral redundancy analysis (SVD)

## Quick Start

```bash
pip install -e ".[dev]"

# Start API server with scheduler
python -m signal_noise serve

# Or collect once and query
python -m signal_noise collect
curl http://localhost:8000/signals/fear_greed/latest
```

## CLI

```bash
python -m signal_noise collect                # Fetch all collectors
python -m signal_noise collect -s fear_greed  # Fetch one collector
python -m signal_noise collect -f daily       # Fetch by frequency
python -m signal_noise list                   # List collectors with status
python -m signal_noise count                  # Show total count
python -m signal_noise serve                  # Start scheduler + REST API
python -m signal_noise analyze spectrum       # SVD spectral analysis
python -m signal_noise analyze quality        # Signal health scoring
python -m signal_noise coverage               # Domain/category coverage matrix
python -m signal_noise rebuild-manifest       # Rebuild collector discovery cache
```

## REST API

```bash
# Health check
curl http://localhost:8000/health

# List all signals
curl http://localhost:8000/signals

# Signal metadata
curl http://localhost:8000/signals/fear_greed

# Time series data (with optional date filter)
curl http://localhost:8000/signals/fear_greed/data?since=2025-01-01

# Latest value
curl http://localhost:8000/signals/fear_greed/latest

# Anomaly detection
curl http://localhost:8000/signals/fear_greed/anomalies

# Batch query
curl -X POST http://localhost:8000/signals/batch \
  -H "Content-Type: application/json" \
  -d '{"names": ["fear_greed", "btc_ohlcv"], "since": "2025-01-01"}'
```

## Architecture

```
Scheduler ──→ Collector ──→ SQLite Store ──→ REST API ──→ Consumer
(per-collector   (API        (WAL mode)      (FastAPI)
 frequency)       fetch)
```

signal-noise is a **data collection service**. It collects and delivers raw time series — consumers handle transforms, evaluation, and prediction.

## Domain Coverage

| Domain | Description | Example categories |
|--------|-------------|-------------------|
| financial | Equities, FX, bonds, crypto | equity, crypto, forex, rates, commodity |
| macro | GDP, employment, inflation | economic, labor, inflation, trade, fiscal |
| sentiment | Sentiment indices, social media | sentiment, attention |
| earth | Weather, climate, ocean | weather, climate, marine, air_quality |
| geophysical | Solar, seismic, celestial | space_weather, seismic, celestial |
| infrastructure | Logistics, aviation, internet | logistics, aviation, internet |
| real_estate | Housing, property prices | real_estate |
| developer | GitHub, npm, StackOverflow | developer |
| health | Epidemiology, disease surveillance | epidemiology, public_health |
| computed | Calculated features | temporal |

## Development

```bash
pip install -e ".[dev]"
pytest tests/ -v
ruff check src/ tests/
```

## Documentation

- **[DESIGN.md](DESIGN.md)** — Design philosophy, Collection Spectrum (L1–L7), 80+ use cases, architectural decisions
- **[AGENTS.md](AGENTS.md)** — Terminology, module reference, domain taxonomy, how to add new collectors
