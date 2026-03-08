# signal-noise

> "even noise is worth collecting -- the signal hides within"

Collect worldwide time series and deliver via REST API + WebSocket.

**2,006 collectors** across **6 domains** and **61 categories** — from stock prices and GDP to earthquake magnitudes, solar wind speed, and real-time orderbook microstructure.

## Features

- **Broad coverage** — markets, economy, environment, technology, sentiment, society
- **Collection Spectrum** — L1 (free APIs) through L6 (physical sensors). See [DESIGN.md](DESIGN.md) for the full spectrum
- **REST API** — FastAPI with signal discovery, time series data, anomaly detection, and batch queries
- **Ops board** — mobile-friendly read-only status board at `/ops` for health, failures, stale signals, and suppressed collectors
- **WebSocket** — Real-time signal event streaming (`/ws/signals`)
- **Streaming collectors** — Binance WebSocket for orderbook depth, trade flow, VPIN, liquidations, funding rate
- **EventBus** — In-process pub/sub for signal update propagation
- **Smart scheduling** — priority queue dispatcher + worker pool with per-collector circuit breakers
- **Dual storage** — `signals` (daily) + `signals_realtime` (1-min, 30-day retention with daily rollup)
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

Then open `http://localhost:8000/ops` in a browser for the internal ops board.

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
python -m signal_noise rollup-realtime        # Rollup 1-min data to daily + purge old data
```

## REST API

```bash
# Health check
curl http://localhost:8000/health

# Per-signal health details (stale, failing, never_seen, suppressed)
curl http://localhost:8000/health/signals

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

# Realtime data (1-min microstructure signals)
curl http://localhost:8000/signals/vpin_btc/realtime?since=2026-03-01

# Batch query
curl -X POST http://localhost:8000/signals/batch \
  -H "Content-Type: application/json" \
  -d '{"names": ["fear_greed", "btc_ohlcv"], "since": "2025-01-01"}'

# WebSocket (real-time signal events)
wscat -c "ws://localhost:8000/ws/signals?names=vpin_btc,book_imbalance_btc"
```

Open `http://localhost:8000/ops` in a browser for the read-only ops board.

## Architecture

```
Polling Collectors ──→ SQLite Store ──→ REST API ──→ Consumer
(per-collector          (WAL mode)      (FastAPI)
 frequency)                  ↑
                             │
Streaming Collectors ──→ signals_realtime ──→ EventBus ──→ WebSocket
(Binance WS)           (1-min, 30d retain)   (pub/sub)   (/ws/signals)
```

signal-noise is a **data collection service**. It collects and delivers raw time series — consumers handle transforms, evaluation, and prediction.

The built-in `/ops` page is intentionally narrow in scope: it is a read-only operational view over `/health` and `/health/signals`, not a general signal exploration UI.

## Domain Coverage

| Domain | Description | Example categories |
|--------|-------------|-------------------|
| markets | Equities, FX, bonds, crypto, derivatives | equity, crypto, forex, rates, commodity, microstructure |
| economy | GDP, employment, inflation, housing, energy, inequality | economic, labor, inflation, energy, manufacturing, inequality |
| environment | Weather, climate, ocean, geology, biodiversity, noise | weather, climate, hydrology, cryosphere, noise, satellite |
| technology | Software ecosystems, internet, logistics, aviation, R&D | developer, internet, aviation, logistics, academic |
| sentiment | Opinions, attention, prediction markets, gaming | sentiment, attention, prediction_market, gaming |
| society | Health, mortality, conflict, demographics, governance | public_health, crime, governance, legislation, food_security |

## Development

```bash
pip install -e ".[dev]"
pytest tests/ -q   # 924 passed as of 2026-03-08
ruff check src/ tests/
```

## Documentation

- **[DESIGN.md](DESIGN.md)** — Design philosophy, Collection Spectrum (L1–L7), 80+ use cases, architectural decisions
- **[AGENTS.md](AGENTS.md)** — Terminology, module reference, domain taxonomy, how to add new collectors
