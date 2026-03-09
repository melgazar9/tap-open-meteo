# CLAUDE.md - AI Agent Development Guide for tap-open-mateo

## Coding Standards (MUST FOLLOW)

- **Documentation-driven**: All decisions must follow official API documentation. Do NOT guess. Verify against live docs (Open-Meteo, Meltano SDK). If uncertain, say "I don't know" rather than produce something wrong.
- **Minimal loops**: Prefer list/dict comprehensions and vectorized operations over `for`/`while` loops. Only use explicit loops when necessary.
- **DRY / OOP / SOLID**: Eliminate code duplication aggressively. Use base class methods, shared schemas, and helper functions. Check the whole codebase for duplication.
- **Naming conventions**: Names must be extremely clear and self-documenting. You should know exactly what a function/method/class does just by reading its name. No vague names like "temp_stream" or "process_data".
- **Keep tap.py, meltano.yml, and .env.example in sync**: When adding/removing config properties, update all three files together.
- **Reference taps**: Follow patterns from `../tap-fred`, `../tap-fmp`, `../tap-massive`, `../tap-yfinance` for architecture decisions.

## Project Overview

- **Project Type**: Singer Tap (Meltano SDK v0.53.5)
- **Source**: Open-Meteo (all 13 API endpoints)
- **Authentication**: API key in query params (paid tier) or none (free tier)
- **No pagination**: All endpoints return full time series in one response (use `SinglePagePaginator`)

## Architecture

### Key Components

1. **Tap Class** (`tap_open_mateo/tap.py`): Config schema (~44 properties), location presets, stream discovery gated by `enabled_endpoints`
2. **Base Client** (`tap_open_mateo/client.py`): `OpenMateoStream(RESTStream, ABC)` — rate limiting, backoff, param builders, response extraction, post_process
3. **Helpers** (`tap_open_mateo/helpers.py`): Columnar-to-row pivoting, ensemble member normalization, previous runs normalization, surrogate key generation
4. **Streams** (`tap_open_mateo/streams/`): 19 stream classes across 11 files

### Stream Files

```
streams/
├── __init__.py                    # Re-exports all 19 stream classes
├── forecast_streams.py            # ForecastHourly, ForecastDaily, PreviousRunsHourly
├── historical_streams.py          # HistoricalHourly, HistoricalDaily (ERA5)
├── historical_forecast_streams.py # HistoricalForecastHourly, HistoricalForecastDaily
├── ensemble_streams.py            # EnsembleHourly
├── seasonal_streams.py            # SeasonalSixHourly, SeasonalDaily
├── climate_streams.py             # ClimateDaily (CMIP6)
├── marine_streams.py              # MarineHourly, MarineDaily
├── air_quality_streams.py         # AirQualityHourly
├── satellite_streams.py           # SatelliteRadiationHourly, SatelliteRadiationDaily
├── flood_streams.py               # FloodDaily
└── utility_streams.py             # Geocoding, Elevation
```

### Open-Meteo API Endpoints (all 13)

| Endpoint | Base URL | Streams |
|----------|----------|---------|
| Forecast | `api.open-meteo.com/v1/forecast` | forecast_hourly, forecast_daily |
| Historical (ERA5) | `archive-api.open-meteo.com/v1/archive` | historical_hourly, historical_daily |
| Historical Forecast | `historical-forecast-api.open-meteo.com/v1/forecast` | historical_forecast_hourly, historical_forecast_daily |
| Previous Runs | `previous-runs-api.open-meteo.com/v1/forecast` | previous_runs_hourly |
| Ensemble | `ensemble-api.open-meteo.com/v1/ensemble` | ensemble_hourly |
| Seasonal | `seasonal-api.open-meteo.com/v1/seasonal` | seasonal_six_hourly, seasonal_daily |
| Climate (CMIP6) | `climate-api.open-meteo.com/v1/climate` | climate_daily |
| Marine | `marine-api.open-meteo.com/v1/marine` | marine_hourly, marine_daily |
| Air Quality | `air-quality-api.open-meteo.com/v1/air-quality` | air_quality_hourly |
| Satellite Radiation | `satellite-api.open-meteo.com/v1/archive` | satellite_radiation_hourly, satellite_radiation_daily |
| Flood | `flood-api.open-meteo.com/v1/flood` | flood_daily |
| Geocoding | `geocoding-api.open-meteo.com/v1/search` | geocoding |
| Elevation | `api.open-meteo.com/v1/elevation` | elevation |

**Paid tier URL pattern**: Replace `{subdomain}-api.open-meteo.com` with `customer-{subdomain}-api.open-meteo.com`

### Key Design Patterns

- **Columnar-to-row pivoting**: Open-Meteo returns `{"time": [...], "var1": [...], "var2": [...]}` — must pivot to one record per timestamp
- **Ensemble member normalization**: `_memberNN` suffix columns → `member` field via `pivot_ensemble_to_rows`
- **Previous runs normalization**: `_previous_dayN` suffix columns → `model_run_offset_days` field via `pivot_previous_runs_to_rows`
- **Multi-location batching**: Comma-separated lat/lon in API requests, configurable `max_locations_per_request`
- **Date chunking**: Historical/satellite/climate streams partition large date ranges into chunks
- **Thread-safe rate limiting**: Shared sliding window via `deque` + `threading.Lock`
- **Surrogate keys**: UUID5 from sorted primary key values

### Streams with custom `_build_base_params`

Most streams use the default (temperature_unit, wind_speed_unit, precipitation_unit, timezone, timeformat). These override it:
- **Marine**: uses `length_unit` instead of weather units
- **Air Quality**: no weather units, adds `domains` param
- **Satellite**: no weather units, adds `tilt`/`azimuth` params
- **Flood**: no weather units, minimal params
- **Geocoding/Elevation**: fully custom (non-time-series)

### Seasonal API Notes

- API parameter name is `hourly` (NOT `six_hourly`), response key is also `hourly`
- Timestamps are 6 hours apart; records labeled `granularity="six_hourly"`
- Response includes 51 ensemble members (control + _member01 through _member50)

## Testing

```bash
uv sync              # Install dependencies
uv run pytest        # Run all tests
uv run pytest -v -k test_name  # Run specific test
```

## File Structure

```
tap-open-mateo/
├── tap_open_mateo/
│   ├── __init__.py
│   ├── tap.py            # Main tap class, config, location presets
│   ├── client.py         # OpenMateoStream base class
│   ├── helpers.py         # Pivot functions, surrogate keys
│   └── streams/          # 11 stream files, 19 stream classes
├── tests/
│   ├── __init__.py
│   └── test_core.py
├── pyproject.toml
├── meltano.yml
└── README.md
```
