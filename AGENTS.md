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
| **Domain** | Stable top-level grouping (17 types) | financial, earth, macro |
| **Category** | Concrete classification (~48 types) | equity, weather, labor |

### Domain List

| Domain | Description | Categories |
|--------|-----------|------------|
| financial | Equities, FX, bonds, commodities, crypto | equity, crypto, crypto_derivatives, forex, rates, commodity, microstructure |
| macro | GDP, employment, inflation, trade, fiscal | economic, labor, inflation, trade, fiscal |
| sentiment | Sentiment indices, social media, attention | sentiment, attention |
| earth | Weather, climate, ocean, air quality, hydrology | weather, climate, marine, air_quality, hydrology, satellite |
| geophysical | Solar, geomagnetic, seismic, lunar | space_weather, seismic, celestial, space |
| infrastructure | Logistics, aviation, internet | logistics, aviation, internet, transportation |
| real_estate | Housing, property prices | real_estate |
| developer | GitHub, npm, StackOverflow, package registries | developer, academic, patents |
| health | Epidemiology, public health, disease surveillance | epidemiology, public_health, excess_deaths |
| computed | Calculated features (temporal, etc.) | temporal |
| creativity | Creative industries, media | — |
| food | Food prices, agriculture | food_price, agriculture |
| conflict | Armed conflict, displacement | armed_conflict, displacement |
| urban | City statistics, transportation | city_stats |
| animal | Biodiversity, wildlife | biodiversity, wildlife |
| mortality | Cause of death, excess mortality | cause_of_death |
| prediction | Prediction markets | prediction_market |

### Categories (48 types)

### Scale

- **Domains**: 17
- **Categories**: 48
- **Collectors**: ~1,476 time series

## Architecture

```
Polling Collectors ──→ SQLite Store ──→ REST API ──→ Consumer
(per-collector          (WAL mode)      (FastAPI)
 frequency)                  ↑
                             │
Streaming Collectors ──→ signals_realtime ──→ EventBus ──→ WebSocket
(Binance WS)           (1-min, 30d retain)   (pub/sub)   (/ws/signals)
```

### Modules

| Module | Role |
|--------|------|
| `collector/base.py` | `BaseCollector` ABC, `CollectorMeta`, taxonomy constants |
| `collector/__init__.py` | Collector registry (`COLLECTORS` dict), `collect_all()` |
| `collector/streaming.py` | `StreamingCollector` ABC for WebSocket-based collectors |
| `collector/binance_ws.py` | Binance WS collectors (orderbook, trade flow, liquidation, funding rate) |
| `collector/*.py` | Individual polling collector implementations |
| `store/sqlite_store.py` | `SignalStore` — `signals` + `signals_realtime` tables |
| `store/events.py` | `EventBus` — in-process pub/sub for signal updates |
| `store/migration.py` | Parquet → SQLite migration |
| `scheduler/loop.py` | asyncio scheduler + streaming task runner + daily rollup |
| `api/app.py` | FastAPI REST API + WebSocket endpoint |
| `cli.py` | CLI entrypoint |
| `config.py` | Paths, `CollectorConfig` |
| `client.py` | `SignalClient` — REST + WebSocket subscribe for consumers |

## CLI

```bash
python -m signal_noise collect                # Fetch all collectors (SQLite)
python -m signal_noise collect -s fear_greed  # Fetch specific collector
python -m signal_noise collect -f daily       # Filter by frequency
python -m signal_noise collect --force        # Ignore cache
python -m signal_noise list                   # List collectors with status
python -m signal_noise count                  # Show collector count
python -m signal_noise serve                  # Start scheduler + REST API
python -m signal_noise serve --no-scheduler   # API only
python -m signal_noise analyze spectrum       # SVD spectral analysis
python -m signal_noise analyze quality        # Signal health scoring
python -m signal_noise coverage               # Domain/category coverage matrix
python -m signal_noise rebuild-manifest       # Rebuild collector discovery cache
python -m signal_noise backfill               # Fetch extended historical data
```

## REST API

```
GET /health                           # Service health check
GET /health/signals                   # List stale signals
GET /signals                          # List all signals (name, domain, category, ...)
GET /signals/{name}                   # Signal metadata
GET /signals/{name}/data?since=...    # Time series data (auto-fallback to realtime for microstructure)
GET /signals/{name}/latest            # Latest value + timestamp
GET /signals/{name}/realtime?since=.. # Realtime (1-min) data for microstructure signals
GET /signals/{name}/anomalies         # Anomaly detection (z-score)
POST /signals/batch                   # Batch query multiple signals
GET /audit                            # Audit log of data changes
WS /ws/signals?names=...             # WebSocket real-time signal events
```

## API Keys (L2 collectors)

L2 collectors require API keys stored in `~/.secrets/`:

| Provider | Env var | Secret file | Collectors |
|----------|---------|-------------|:----------:|
| FRED | `FRED_API_KEY` | `~/.secrets/fred` | 81 |
| EIA | `EIA_API_KEY` | `~/.secrets/eia` | 42 |
| BEA | `BEA_API_KEY` | `~/.secrets/bea` | 12 |
| Finnhub | `FINNHUB_API_KEY` | `~/.secrets/finnhub` | 42 |

Secret file format: `export KEY_NAME=value`

## Adding a New Collector

### Polling collector (BaseCollector)
1. Create `src/signal_noise/collector/<name>.py`
2. Subclass `BaseCollector`, define `meta: CollectorMeta` with correct `domain` and `category`
3. Implement `fetch()` → return `DataFrame` with `[timestamp|date, value]` columns
4. Auto-discovered via `get_<name>_collectors()` factory or direct `BaseCollector` subclass
5. Add tests in `tests/`

### Streaming collector (StreamingCollector)
1. Subclass `StreamingCollector` in `collector/binance_ws.py` (or new file)
2. Set `use_realtime_store = True` for 1-min data → `signals_realtime` table
3. Implement `async stream()` → yield `DataFrame` per aggregation window
4. Multi-signal: include `name` column in DataFrame for per-signal routing

## Development

```bash
pip install -e ".[dev]"
pytest tests/ -v                      # 703 tests
ruff check src/ tests/
python -m signal_noise count          # Show collector count
```
