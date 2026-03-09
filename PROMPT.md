# Prompt: Build `tap-open-mateo` — A Singer Tap for Open-Meteo Weather APIs

## Goal
Build a production-grade Singer tap (Meltano SDK) that extracts weather data from the Open-Meteo suite of REST APIs. Open-Meteo provides pre-parsed JSON access to GFS, GEFS, ECMWF, ERA5, and dozens of other weather models — no GRIB2 parsing required. This tap serves two purposes:

1. **Fast-lane weather data** — Get weather features into ML pipelines immediately without complex binary parsing
2. **Production complement** — Point-based weather queries for specific locations (population centers, trading hubs) alongside the full-grid GRIB2 taps

This tap covers 5 Open-Meteo API endpoints:
- **Forecast** — Current GFS/ECMWF/ICON forecasts (free tier)
- **Historical** — ERA5 reanalysis back to 1940 (paid tier)
- **Ensemble** — GEFS/ECMWF ensemble member data (paid tier)
- **Previous Runs** — Point-in-time forecast archives, last 7 days of model runs (paid tier)
- **Climate** — CMIP6 climate projections 1950-2050 (paid tier)

This is an extractor ONLY — no loader logic. Output goes to JSONL via Meltano's `target-jsonl` or any Singer target.

---

## Reference Codebases (MUST study these first)

Before writing any code, read and understand these existing taps to match their patterns:

1. **tap-fred** at `~/code/github/personal/tap-fred/` — Most similar architecture. Study:
   - `tap_fred/tap.py` — Main Tap class, config schema, thread-safe caching, wildcard "*" discovery
   - `tap_fred/client.py` — Base stream class hierarchy, `_make_request()` with backoff, `_throttle()` rate limiter, `_safe_partition_extraction()`, `post_process()` pipeline
   - `tap_fred/helpers.py` — `clean_json_keys()`, `generate_surrogate_key()`
   - `tap_fred/streams/` — How streams are organized by endpoint category

2. **tap-fmp** at `~/code/github/personal/tap-fmp/` — Study:
   - `tap_fmp/client.py` — How `_fetch_with_retry()` works, pagination patterns
   - `tap_fmp/mixins.py` — Configuration mixins for different resource types

3. **tap-massive** at `~/code/github/personal/tap-massive/` — Study:
   - `tap_massive/client.py` — `_check_missing_fields()` schema validation
   - How `post_process()` converts camelCase → snake_case

**Match these patterns exactly.** Use the same libraries (`singer-sdk~=0.53.5`, `requests~=2.32.3`, `backoff>=2.2.1,<3.0.0`), same base class hierarchy style, same error handling approach.

---

## Open-Meteo API Documentation

### API Endpoints Overview

| Endpoint | Base URL | Auth | Free Tier |
|----------|----------|------|-----------|
| Forecast | `https://api.open-meteo.com/v1/forecast` | None (free) / API key (paid) | Yes |
| Historical | `https://archive-api.open-meteo.com/v1/archive` | API key required | No |
| Ensemble | `https://ensemble-api.open-meteo.com/v1/ensemble` | API key required | No |
| Previous Runs | `https://previous-runs-api.open-meteo.com/v1/forecast` | API key required | No |
| Climate | `https://climate-api.open-meteo.com/v1/climate` | API key required | No |

**Paid tier base URL:** Replace `api.open-meteo.com` with `customer-api.open-meteo.com` (and similarly for archive, ensemble, previous-runs, climate subdomains). Pass `&apikey=YOUR_KEY` as query parameter.

### Authentication
- **Free tier**: No authentication. No API key needed. Limited to Forecast API only.
- **Paid tier**: API key passed as query parameter `&apikey=YOUR_KEY`. Required for Historical, Ensemble, Previous Runs, and Climate APIs.
- Paid plans start at ~$20/month for 1M calls.

### Rate Limits
- **Free tier**: 600 calls/minute, 5,000 calls/hour, 10,000 calls/day
- **Paid tier**: Unlimited per-minute/hour/day within monthly quota (1M+ calls/month)
- Implement: max 500 requests/minute with 0.15s minimum between requests (configurable)

### Response Format (ALL endpoints)
All endpoints return the same JSON structure. **No pagination** — full time series returned in a single response.

**Single-location response:**
```json
{
  "latitude": 40.71,
  "longitude": -74.01,
  "generationtime_ms": 0.036,
  "utc_offset_seconds": 0,
  "timezone": "GMT",
  "timezone_abbreviation": "GMT",
  "elevation": 10.0,
  "hourly_units": {
    "time": "iso8601",
    "temperature_2m": "°C",
    "relative_humidity_2m": "%"
  },
  "hourly": {
    "time": ["2026-03-09T00:00", "2026-03-09T01:00", ...],
    "temperature_2m": [6.7, 6.2, 5.8, ...],
    "relative_humidity_2m": [78, 80, 82, ...]
  },
  "daily_units": {
    "time": "iso8601",
    "temperature_2m_max": "°C"
  },
  "daily": {
    "time": ["2026-03-09", "2026-03-10", ...],
    "temperature_2m_max": [12.3, 14.1, ...]
  }
}
```

**Multi-location response:** When comma-separated lat/lon are passed, returns a JSON **array** of the above objects.

**Key structural notes:**
- `hourly` and `daily` are **columnar** — keys are variable names, values are arrays aligned with the `time` array
- You MUST pivot this to row-based records (one record per timestamp × location)
- Units are in a separate `*_units` object, NOT embedded in the data

### Common Query Parameters (all endpoints)

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `latitude` | float | Yes | — | WGS84. Comma-separated for multi-location |
| `longitude` | float | Yes | — | WGS84. Comma-separated for multi-location |
| `hourly` | string | No | — | Comma-separated hourly variables to request |
| `daily` | string | No | — | Comma-separated daily variables to request |
| `temperature_unit` | string | No | `celsius` | `celsius` or `fahrenheit` |
| `wind_speed_unit` | string | No | `kmh` | `kmh`, `ms`, `mph`, `kn` |
| `precipitation_unit` | string | No | `mm` | `mm` or `inch` |
| `timeformat` | string | No | `iso8601` | `iso8601` or `unixtime` |
| `timezone` | string | No | `GMT` | IANA timezone or `auto` |
| `cell_selection` | string | No | `land` | `land`, `sea`, `nearest` |
| `apikey` | string | No | — | For paid tier |

---

## Endpoint-Specific Details

### 1. Forecast API

**Base URL:** `https://api.open-meteo.com/v1/forecast`
**Paid URL:** `https://customer-api.open-meteo.com/v1/forecast`

**Additional parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `forecast_days` | int (0-16) | 7 | Forecast horizon in days |
| `past_days` | int (0-92) | 0 | Include historical days before today |
| `models` | string | `auto` | Comma-separated model names (see model list) |
| `current` | string | — | Comma-separated variables for current conditions |

**Available hourly variables (60+):**
`temperature_2m`, `relative_humidity_2m`, `dew_point_2m`, `apparent_temperature`, `pressure_msl`, `surface_pressure`, `cloud_cover`, `cloud_cover_low`, `cloud_cover_mid`, `cloud_cover_high`, `visibility`, `wind_speed_10m`, `wind_speed_80m`, `wind_speed_120m`, `wind_speed_180m`, `wind_direction_10m`, `wind_direction_80m`, `wind_direction_120m`, `wind_direction_180m`, `wind_gusts_10m`, `precipitation`, `rain`, `showers`, `snowfall`, `precipitation_probability`, `snow_depth`, `weather_code`, `is_day`, `shortwave_radiation`, `direct_radiation`, `direct_normal_irradiance`, `diffuse_radiation`, `global_tilted_irradiance`, `soil_temperature_0cm`, `soil_temperature_6cm`, `soil_temperature_18cm`, `soil_temperature_54cm`, `soil_moisture_0_to_1cm`, `soil_moisture_1_to_3cm`, `soil_moisture_3_to_9cm`, `soil_moisture_9_to_27cm`, `soil_moisture_27_to_81cm`, `evapotranspiration`, `et0_fao_evapotranspiration`, `vapour_pressure_deficit`, `cape`, `lifted_index`, `convective_inhibition`, `freezing_level_height`, `boundary_layer_height`, `uv_index`, `uv_index_clear_sky`, `sunshine_duration`, `wet_bulb_temperature`

**Pressure level variables** at 19 levels (1000, 975, 950, 925, 900, 850, 800, 700, 600, 500, 400, 300, 250, 200, 150, 100, 70, 50, 30 hPa):
`temperature_{level}hPa`, `relative_humidity_{level}hPa`, `dew_point_{level}hPa`, `cloud_cover_{level}hPa`, `wind_speed_{level}hPa`, `wind_direction_{level}hPa`, `geopotential_height_{level}hPa`

**Available daily variables (50+):**
`temperature_2m_max`, `temperature_2m_min`, `temperature_2m_mean`, `apparent_temperature_max`, `apparent_temperature_min`, `apparent_temperature_mean`, `precipitation_sum`, `rain_sum`, `showers_sum`, `snowfall_sum`, `precipitation_hours`, `precipitation_probability_max`, `precipitation_probability_mean`, `precipitation_probability_min`, `weather_code`, `sunrise`, `sunset`, `daylight_duration`, `sunshine_duration`, `wind_speed_10m_max`, `wind_gusts_10m_max`, `wind_direction_10m_dominant`, `shortwave_radiation_sum`, `et0_fao_evapotranspiration`, `uv_index_max`, `uv_index_clear_sky_max`

**Available models (46+):**
`best_match` (auto), `ecmwf_ifs025`, `ecmwf_aifs025`, `gfs_seamless`, `gfs_global`, `gfs_hrrr`, `icon_seamless`, `icon_global`, `icon_eu`, `icon_d2`, `gem_seamless`, `gem_global`, `gem_regional`, `gem_hrdps`, `jma_seamless`, `jma_gsm`, `jma_msm`, `meteofrance_seamless`, `meteofrance_arpege_world`, `meteofrance_arpege_europe`, `meteofrance_arome_france`, `knmi_seamless`, `knmi_harmonie_arome_europe`, `dmi_seamless`, `dmi_harmonie_arome_europe`, `ukmo_seamless`, `ukmo_global_deterministic_10km`, `bom_access_global`, `cma_grapes_global`, `arpae_cosmo_seamless`, `arpae_cosmo_2i`, `arpae_cosmo_5m`, `metno_seamless`, `metno_nordic`

### 2. Historical API (ERA5)

**Base URL:** `https://archive-api.open-meteo.com/v1/archive`
**Paid URL:** `https://customer-archive-api.open-meteo.com/v1/archive`

**Key differences from Forecast:**
- `start_date` and `end_date` are **REQUIRED** (format: `YYYY-MM-DD`)
- No `forecast_days`, `past_days`, or `current` parameters
- Data available back to **1940** (ERA5) and **1950** (ERA5-Land)

**Available models:**
| Model | Resolution | Coverage | Date Range |
|-------|-----------|----------|------------|
| `best_match` (default) | varies | Global | varies |
| `ecmwf_ifs` | 9 km | Global | 2017-present |
| `era5` | ~25 km | Global | **1940-present** |
| `era5_land` | ~11 km | Global | 1950-present |
| `era5_ensemble` | ~55 km | Global | 1940-present |
| `cerra` | 5 km | Europe only | 1985-June 2021 |

**Hourly variables (~29):**
`temperature_2m`, `relative_humidity_2m`, `dew_point_2m`, `apparent_temperature`, `vapour_pressure_deficit`, `pressure_msl`, `surface_pressure`, `precipitation`, `rain`, `snowfall`, `cloud_cover`, `cloud_cover_low`, `cloud_cover_mid`, `cloud_cover_high`, `weather_code`, `shortwave_radiation`, `direct_radiation`, `diffuse_radiation`, `direct_normal_irradiance`, `global_tilted_irradiance`, `sunshine_duration`, `wind_speed_10m`, `wind_speed_100m`, `wind_direction_10m`, `wind_direction_100m`, `wind_gusts_10m`, `soil_temperature_0_to_7cm`, `soil_temperature_7_to_28cm`, `soil_temperature_28_to_100cm`, `soil_temperature_100_to_255cm`, `soil_moisture_0_to_7cm`, `soil_moisture_7_to_28cm`, `soil_moisture_28_to_100cm`, `soil_moisture_100_to_255cm`, `et0_fao_evapotranspiration`, `snow_depth`

**Practical note for backfilling:** ERA5 has 86 years of hourly data. For 50 locations × 86 years = 4,300 location-years. At one API call per location per month chunk = ~51,600 calls. Well within paid tier limits. Partition by (location, month) to keep response sizes manageable.

### 3. Ensemble API (GEFS/ECMWF Ensembles)

**Base URL:** `https://ensemble-api.open-meteo.com/v1/ensemble`
**Paid URL:** `https://customer-ensemble-api.open-meteo.com/v1/ensemble`

**Critical: `models` parameter is REQUIRED** (no default).

**Response structure for ensemble members:**
Variable names in response have member suffixes:
```json
{
  "hourly": {
    "time": ["2026-03-09T00:00", ...],
    "temperature_2m": [6.7, ...],
    "temperature_2m_member01": [6.5, ...],
    "temperature_2m_member02": [7.1, ...],
    "temperature_2m_member30": [6.3, ...]
  }
}
```
- Base variable (e.g., `temperature_2m`) is the control/mean run
- Members: `{variable}_member01` through `{variable}_memberNN` (zero-padded, starting at 01)
- Member count depends on model (GFS=31, ECMWF=51, ICON=40)

**Key models for energy trading:**
| Model | Resolution | Members | Horizon | Update |
|-------|-----------|---------|---------|--------|
| `gfs025` | 25 km | 31 | 10 days | 6-hourly |
| `gfs05` | 50 km | 31 | 35 days | 6-hourly |
| `ecmwf_ifs025` | 25 km | 51 | 15 days | 6-hourly |
| `ecmwf_aifs025` | 25 km | 51 | 15 days | 6-hourly |
| `icon_seamless` | 26 km | 40 | 7.5 days | 12-hourly |
| `gem_global` | 25 km | 21 | 16-39 days | 12-hourly |

**Hourly variables (subset of forecast):**
`temperature_2m`, `relative_humidity_2m`, `dew_point_2m`, `apparent_temperature`, `wind_speed_10m`, `wind_speed_80m`, `wind_speed_120m`, `wind_direction_10m`, `wind_direction_80m`, `wind_direction_120m`, `wind_gusts_10m`, `precipitation`, `rain`, `snowfall`, `snow_depth`, `pressure_msl`, `surface_pressure`, `cloud_cover`, `visibility`, `cape`, `sunshine_duration`, `shortwave_radiation`, `direct_radiation`, `diffuse_radiation`, `weather_code`, `soil_temperature_0_to_7cm`, `soil_moisture_0_to_7cm`

### 4. Previous Runs API (Point-in-Time Forecast Archives)

**Base URL:** `https://previous-runs-api.open-meteo.com/v1/forecast`
**Paid URL:** `https://customer-previous-runs-api.open-meteo.com/v1/forecast`

**THIS IS THE MOST CRITICAL ENDPOINT FOR TRADING.** It provides what forecasts looked like on previous days — essential for computing forecast revision signals.

**Mechanism:** Same endpoint structure as Forecast API, but append `_previous_day1` through `_previous_day7` to variable names.

**Response structure:**
```json
{
  "hourly": {
    "time": ["2026-03-09T00:00", ...],
    "temperature_2m": [6.7, ...],
    "temperature_2m_previous_day1": [6.5, ...],
    "temperature_2m_previous_day2": [7.1, ...],
    "temperature_2m_previous_day3": [6.9, ...],
    "temperature_2m_previous_day7": [7.5, ...]
  },
  "hourly_units": {
    "temperature_2m": "°C",
    "temperature_2m_previous_day1": "°C",
    "temperature_2m_previous_day2": "°C"
  }
}
```

**Previous run depth:** Day 0 (current/latest) through Day 7 (7 days ago).

**Data availability:** Generally from January 2024 onward. Some models (GFS temperature) from March 2021. JMA from 2018.

**How to request:** Include both the base variable AND the `_previous_dayN` suffixed variables in the `hourly` parameter:
```
?hourly=temperature_2m,temperature_2m_previous_day1,temperature_2m_previous_day2,...,temperature_2m_previous_day7
```

**All standard forecast hourly/daily variables** support the `_previous_dayN` suffix.

### 5. Climate API (CMIP6 Projections)

**Base URL:** `https://climate-api.open-meteo.com/v1/climate`
**Paid URL:** `https://customer-climate-api.open-meteo.com/v1/climate`

**Key differences:**
- `start_date`, `end_date`, `models`, and `daily` are ALL **required**
- Date range: **1950-01-01 to 2050-12-31**
- **Daily resolution ONLY** — no hourly data
- Models downscaled to 10 km via ERA5-Land bias correction
- Has `disable_bias_correction` parameter (boolean, default false)

**Available models (7 CMIP6 HighResMIP):**
`CMCC_CM2_VHR4`, `FGOALS_f3_H`, `HiRAM_SIT_HR`, `MRI_AGCM3_2_S`, `EC_Earth3P_HR`, `MPI_ESM1_2_XR`, `NICAM16_8S`

**Daily variables:**
`temperature_2m_max`, `temperature_2m_min`, `temperature_2m_mean`, `relative_humidity_2m_max`, `relative_humidity_2m_min`, `relative_humidity_2m_mean`, `wind_speed_10m_mean`, `wind_speed_10m_max`, `precipitation_sum`, `rain_sum`, `snowfall_sum`, `cloud_cover_mean`, `pressure_msl_mean`, `shortwave_radiation_sum`, `soil_moisture_0_to_10cm_mean`

---

## Directory Structure

```
tap-open-mateo/
├── tap_open_mateo/
│   ├── __init__.py
│   ├── __main__.py              # TapOpenMateo.cli()
│   ├── tap.py                   # Main Tap class, config, stream registration
│   ├── client.py                # OpenMateoStream base class, request handling, rate limiting
│   ├── helpers.py               # Columnar-to-row pivot, surrogate key generation
│   └── streams/
│       ├── __init__.py          # Re-export all stream classes
│       ├── forecast_streams.py  # Forecast + Previous Runs streams
│       ├── historical_streams.py # ERA5 historical data streams
│       ├── ensemble_streams.py  # Ensemble member data streams
│       └── climate_streams.py   # CMIP6 climate projection streams
├── tests/
│   ├── __init__.py
│   └── test_core.py
├── pyproject.toml
├── meltano.yml
└── README.md
```

---

## Implementation Requirements

### 1. Tap Class (`tap.py`)

```python
class TapOpenMateo(Tap):
    name = "tap-open-mateo"
```

**Config Properties (singer_sdk.typing):**
- `api_key` (StringType, optional, secret) — Open-Meteo API key for paid tier. If omitted, only Forecast API available (free tier).
- `locations` (ArrayType(ObjectType), required) — List of locations to query. Each object: `{"name": "New York", "latitude": 40.71, "longitude": -74.01}`. Name is for labeling records.
- `location_presets` (StringType, optional) — Shortcut: `"us_population_centers"` auto-generates top 50 US metro lat/lon. `"energy_hubs"` generates Henry Hub, Cushing OK, NYMEX delivery points, etc. If set, overrides `locations`.
- `hourly_variables` (ArrayType(StringType), default `["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m", "precipitation", "rain", "snowfall", "snow_depth", "pressure_msl", "cloud_cover", "soil_temperature_0cm", "soil_moisture_0_to_1cm"]`) — Which hourly variables to request
- `daily_variables` (ArrayType(StringType), default `["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean", "precipitation_sum", "wind_speed_10m_max", "wind_gusts_10m_max"]`) — Which daily variables to request
- `models` (ArrayType(StringType), default `["best_match"]`) — Which weather models to query
- `ensemble_models` (ArrayType(StringType), default `["gfs025"]`) — Which ensemble models to query
- `climate_models` (ArrayType(StringType), default `["EC_Earth3P_HR", "MRI_AGCM3_2_S"]`) — Which CMIP6 models
- `temperature_unit` (StringType, default `"fahrenheit"`) — `celsius` or `fahrenheit`
- `wind_speed_unit` (StringType, default `"mph"`) — `kmh`, `ms`, `mph`, `kn`
- `precipitation_unit` (StringType, default `"inch"`) — `mm` or `inch`
- `timezone` (StringType, default `"America/Chicago"`) — IANA timezone for daily aggregation
- `forecast_days` (IntegerType, default 16) — Forecast horizon (max 16)
- `past_days` (IntegerType, default 7) — How many past days to include in forecast requests
- `previous_run_days` (ArrayType(IntegerType), default `[1, 2, 3, 5, 7]`) — Which `_previous_dayN` suffixes to request
- `historical_start_date` (DateType, optional) — Start date for Historical API. Example: `"1940-01-01"`
- `historical_end_date` (DateType, optional) — End date for Historical API. Default: yesterday.
- `historical_chunk_days` (IntegerType, default 365) — Days per Historical API request (keep responses manageable)
- `climate_start_date` (DateType, default `"1950-01-01"`) — Climate API start
- `climate_end_date` (DateType, default `"2050-12-31"`) — Climate API end
- `enabled_endpoints` (ArrayType(StringType), default `["forecast", "historical", "ensemble", "previous_runs"]`) — Which endpoints to extract. Climate excluded by default (rarely needed for short-term trading).
- `max_requests_per_minute` (IntegerType, default 500)
- `min_throttle_seconds` (NumberType, default 0.15)
- `max_locations_per_request` (IntegerType, default 10) — Batch locations into multi-location requests (Open-Meteo supports comma-separated lat/lon)
- `strict_mode` (BooleanType, default False)

**Location Presets (built-in):**
```python
US_POPULATION_CENTERS = [
    {"name": "New York", "latitude": 40.71, "longitude": -74.01},
    {"name": "Los Angeles", "latitude": 34.05, "longitude": -118.24},
    {"name": "Chicago", "latitude": 41.88, "longitude": -87.63},
    {"name": "Houston", "latitude": 29.76, "longitude": -95.37},
    {"name": "Phoenix", "latitude": 33.45, "longitude": -112.07},
    {"name": "Philadelphia", "latitude": 39.95, "longitude": -75.17},
    {"name": "San Antonio", "latitude": 29.42, "longitude": -98.49},
    {"name": "San Diego", "latitude": 32.72, "longitude": -117.16},
    {"name": "Dallas", "latitude": 32.78, "longitude": -96.80},
    {"name": "Jacksonville", "latitude": 30.33, "longitude": -81.66},
    # ... top 50 US metros by population
]

ENERGY_HUBS = [
    {"name": "Henry Hub", "latitude": 30.05, "longitude": -91.14},      # Erath, LA
    {"name": "Cushing OK", "latitude": 35.98, "longitude": -96.77},     # WTI delivery
    {"name": "Houston Ship Channel", "latitude": 29.73, "longitude": -95.25},
    {"name": "Midland TX", "latitude": 31.99, "longitude": -102.08},    # Permian Basin
    {"name": "Appalachia", "latitude": 39.63, "longitude": -79.96},     # Marcellus Shale
    # ... other key energy infrastructure points
]
```

**Thread-Safe Caching:**
- `get_resolved_locations()` — Resolve `location_presets` or `locations` config into final list. Cache with lock.

### 2. Base Stream Class (`client.py`)

**`OpenMateoStream(RESTStream, ABC)`** — Base for all Open-Meteo streams:

```python
class OpenMateoStream(RESTStream, ABC):
    """Base class for Open-Meteo API streams."""
```

**Dynamic `url_base`:** Must resolve based on config:
```python
@property
def url_base(self) -> str:
    if self.config.get("api_key"):
        return self._paid_url_base
    return self._free_url_base
```

**Must implement:**
- `_throttle()` — Sliding window rate limiter (copy pattern from tap-fred's `client.py`)
- `_make_request(url, params)` — Centralized request with:
  - `@backoff.on_exception` decorator (exponential, max_tries=5, max_time=120)
  - Retry on 429, 500-504, network errors
  - Give up on 400, 401, 403
  - Include `apikey` in params if configured
  - Log URL (with apikey redacted)
- `_pivot_columnar_to_rows(response_data, location_name, latitude, longitude)` — THE KEY TRANSFORMATION:
  ```python
  def _pivot_columnar_to_rows(self, data: dict, location_name: str,
                                latitude: float, longitude: float,
                                granularity: str) -> list[dict]:
      """Convert Open-Meteo columnar response to row-based records.

      Open-Meteo returns:
        {"hourly": {"time": [...], "temperature_2m": [...], "wind_speed_10m": [...]}}

      We emit one record per timestamp:
        {"time": "2026-03-09T00:00", "temperature_2m": 6.7, "wind_speed_10m": 12.3, ...}
      """
      section = data.get(granularity, {})  # "hourly" or "daily"
      units = data.get(f"{granularity}_units", {})
      times = section.get("time", [])

      records = []
      variables = [k for k in section if k != "time"]
      for i, time_val in enumerate(times):
          record = {
              "location_name": location_name,
              "latitude": latitude,
              "longitude": longitude,
              "time": time_val,
              "granularity": granularity,
              "elevation": data.get("elevation"),
              "model": data.get("model", "best_match"),
          }
          for var in variables:
              values = section[var]
              record[var] = values[i] if i < len(values) else None
          records.append(record)
      return records
  ```
- `post_process(record, context)` — Add surrogate key, any final transforms

**Multi-location batching:**
Open-Meteo supports comma-separated lat/lon for up to ~50 locations per request. Batch locations to minimize API calls:
```python
def _batch_locations(self, locations: list[dict], batch_size: int) -> list[list[dict]]:
    """Split locations into batches for multi-location requests."""
    return [locations[i:i + batch_size] for i in range(0, len(locations), batch_size)]
```

### 3. Streams

**Stream 1: `ForecastHourlyStream`** — Current forecast, hourly resolution
- **Partitioned by location batch** (groups of `max_locations_per_request`)
- Path: `/v1/forecast`
- Query params: `latitude={lat1},{lat2},...&longitude={lon1},{lon2},...&hourly={vars}&forecast_days={n}&past_days={n}&models={model}&temperature_unit={unit}&wind_speed_unit={unit}&precipitation_unit={unit}&timezone={tz}`
- Pivots columnar response to one record per (location, timestamp)
- Schema:
  ```python
  schema = th.PropertiesList(
      # Identity
      th.Property("location_name", th.StringType, required=True),
      th.Property("latitude", th.NumberType, required=True),
      th.Property("longitude", th.NumberType, required=True),
      th.Property("elevation", th.NumberType),
      th.Property("model", th.StringType),
      th.Property("time", th.DateTimeType, required=True),
      th.Property("granularity", th.StringType, required=True),  # "hourly"

      # Weather variables (all optional — depends on config)
      th.Property("temperature_2m", th.NumberType),
      th.Property("relative_humidity_2m", th.NumberType),
      th.Property("dew_point_2m", th.NumberType),
      th.Property("apparent_temperature", th.NumberType),
      th.Property("wind_speed_10m", th.NumberType),
      th.Property("wind_direction_10m", th.NumberType),
      th.Property("wind_gusts_10m", th.NumberType),
      th.Property("precipitation", th.NumberType),
      th.Property("rain", th.NumberType),
      th.Property("snowfall", th.NumberType),
      th.Property("snow_depth", th.NumberType),
      th.Property("pressure_msl", th.NumberType),
      th.Property("surface_pressure", th.NumberType),
      th.Property("cloud_cover", th.NumberType),
      th.Property("cloud_cover_low", th.NumberType),
      th.Property("cloud_cover_mid", th.NumberType),
      th.Property("cloud_cover_high", th.NumberType),
      th.Property("visibility", th.NumberType),
      th.Property("weather_code", th.IntegerType),
      th.Property("soil_temperature_0cm", th.NumberType),
      th.Property("soil_temperature_6cm", th.NumberType),
      th.Property("soil_temperature_18cm", th.NumberType),
      th.Property("soil_temperature_54cm", th.NumberType),
      th.Property("soil_moisture_0_to_1cm", th.NumberType),
      th.Property("soil_moisture_1_to_3cm", th.NumberType),
      th.Property("soil_moisture_3_to_9cm", th.NumberType),
      th.Property("soil_moisture_9_to_27cm", th.NumberType),
      th.Property("soil_moisture_27_to_81cm", th.NumberType),
      th.Property("cape", th.NumberType),
      th.Property("evapotranspiration", th.NumberType),
      th.Property("et0_fao_evapotranspiration", th.NumberType),
      th.Property("vapour_pressure_deficit", th.NumberType),
      th.Property("shortwave_radiation", th.NumberType),
      th.Property("direct_radiation", th.NumberType),
      th.Property("diffuse_radiation", th.NumberType),
      th.Property("direct_normal_irradiance", th.NumberType),
      th.Property("sunshine_duration", th.NumberType),
      th.Property("wet_bulb_temperature", th.NumberType),
      th.Property("uv_index", th.NumberType),
      th.Property("freezing_level_height", th.NumberType),
      th.Property("is_day", th.IntegerType),

      # Surrogate key
      th.Property("surrogate_key", th.StringType),
  ).to_dict()
  ```
- Primary keys: `["location_name", "model", "time", "granularity"]`
- **No replication key** — forecast data is ephemeral (changes every model run). Always full extract.

**Stream 2: `ForecastDailyStream`** — Current forecast, daily resolution
- Same pattern as ForecastHourlyStream but queries `daily` variables
- Schema uses daily aggregation variables (`temperature_2m_max`, `temperature_2m_min`, etc.)
- Primary keys: `["location_name", "model", "time", "granularity"]`

**Stream 3: `HistoricalHourlyStream`** — ERA5 historical data, hourly
- **Partitioned by (location_batch, date_chunk)** — split date range into `historical_chunk_days`-sized chunks
- Path: `/v1/archive`
- Query params: `latitude=...&longitude=...&hourly={vars}&start_date={start}&end_date={end}&models={model}&temperature_unit=...`
- Same columnar-to-row pivot
- Schema: same as ForecastHourlyStream (subset of variables available)
- Primary keys: `["location_name", "model", "time", "granularity"]`
- Replication key: `"time"` — for incremental sync (don't re-download completed months)

**Partition generation for Historical:**
```python
@property
def partitions(self):
    locations = self.get_resolved_locations()
    batches = self._batch_locations(locations, self.config["max_locations_per_request"])
    start = self.config["historical_start_date"]
    end = self.config.get("historical_end_date", yesterday())
    chunk_days = self.config["historical_chunk_days"]

    partitions = []
    for batch_idx, batch in enumerate(batches):
        current = start
        while current <= end:
            chunk_end = min(current + timedelta(days=chunk_days - 1), end)
            partitions.append({
                "batch_idx": batch_idx,
                "locations": batch,
                "start_date": current.isoformat(),
                "end_date": chunk_end.isoformat(),
            })
            current = chunk_end + timedelta(days=1)
    return partitions
```

**Stream 4: `HistoricalDailyStream`** — ERA5 historical, daily resolution
- Same partitioning as HistoricalHourlyStream
- Queries `daily` variables
- Replication key: `"time"`

**Stream 5: `EnsembleHourlyStream`** — Ensemble forecast members
- **Partitioned by (location_batch, model)**
- Path: `/v1/ensemble`
- Query params: `latitude=...&longitude=...&hourly={vars}&models={ensemble_model}`
- **Critical transformation:** Ensemble responses have `{variable}_member01`, `{variable}_member02`, etc. Pivot into normalized records:
  ```python
  # Instead of one wide record with 31 member columns,
  # emit one record per (location, time, member):
  {
      "location_name": "Chicago",
      "time": "2026-03-09T00:00",
      "model": "gfs025",
      "member": 0,          # 0 = control, 1-30 = perturbed
      "temperature_2m": 6.7,
      "wind_speed_10m": 12.3,
      ...
  }
  ```
- This normalization is essential — you don't want 31 separate columns per variable
- Schema:
  ```python
  schema = th.PropertiesList(
      th.Property("location_name", th.StringType, required=True),
      th.Property("latitude", th.NumberType, required=True),
      th.Property("longitude", th.NumberType, required=True),
      th.Property("elevation", th.NumberType),
      th.Property("model", th.StringType, required=True),
      th.Property("member", th.IntegerType, required=True),  # 0=control, 1-N=perturbed
      th.Property("time", th.DateTimeType, required=True),
      th.Property("granularity", th.StringType, required=True),

      # Weather variables (same as forecast, subset available for ensemble)
      th.Property("temperature_2m", th.NumberType),
      th.Property("relative_humidity_2m", th.NumberType),
      th.Property("dew_point_2m", th.NumberType),
      th.Property("apparent_temperature", th.NumberType),
      th.Property("wind_speed_10m", th.NumberType),
      th.Property("wind_direction_10m", th.NumberType),
      th.Property("wind_gusts_10m", th.NumberType),
      th.Property("precipitation", th.NumberType),
      th.Property("rain", th.NumberType),
      th.Property("snowfall", th.NumberType),
      th.Property("snow_depth", th.NumberType),
      th.Property("pressure_msl", th.NumberType),
      th.Property("surface_pressure", th.NumberType),
      th.Property("cloud_cover", th.NumberType),
      th.Property("visibility", th.NumberType),
      th.Property("cape", th.NumberType),
      th.Property("weather_code", th.IntegerType),
      th.Property("shortwave_radiation", th.NumberType),
      th.Property("sunshine_duration", th.NumberType),

      th.Property("surrogate_key", th.StringType),
  ).to_dict()
  ```
- Primary keys: `["location_name", "model", "member", "time", "granularity"]`

**Stream 6: `PreviousRunsHourlyStream`** — Point-in-time forecast archives
- **Partitioned by location batch**
- Path: `/v1/forecast` (same as forecast, but on `previous-runs-api` subdomain)
- Query params build the `hourly` list dynamically:
  ```python
  def _build_previous_runs_variables(self, base_vars: list[str], previous_days: list[int]) -> str:
      """Build variable list including _previous_dayN suffixes."""
      all_vars = list(base_vars)
      for var in base_vars:
          for day in previous_days:
              all_vars.append(f"{var}_previous_day{day}")
      return ",".join(all_vars)
  ```
- **Critical transformation:** Flatten `_previous_dayN` suffixed columns into normalized records:
  ```python
  # Response has: temperature_2m, temperature_2m_previous_day1, temperature_2m_previous_day2, ...
  # Emit separate records per previous_day:
  {
      "location_name": "Chicago",
      "time": "2026-03-09T00:00",
      "model_run_offset_days": 0,    # 0 = current, 1 = yesterday's forecast, etc.
      "temperature_2m": 6.7,
      "wind_speed_10m": 12.3,
      ...
  }
  {
      "location_name": "Chicago",
      "time": "2026-03-09T00:00",
      "model_run_offset_days": 1,    # Yesterday's forecast for same time
      "temperature_2m": 6.5,
      "wind_speed_10m": 11.8,
      ...
  }
  ```
- This is the **forecast revision signal** — compare `model_run_offset_days=0` vs `model_run_offset_days=1` to see how the forecast changed
- Schema:
  ```python
  schema = th.PropertiesList(
      th.Property("location_name", th.StringType, required=True),
      th.Property("latitude", th.NumberType, required=True),
      th.Property("longitude", th.NumberType, required=True),
      th.Property("elevation", th.NumberType),
      th.Property("model", th.StringType),
      th.Property("model_run_offset_days", th.IntegerType, required=True),  # 0=current, 1-7=previous
      th.Property("time", th.DateTimeType, required=True),
      th.Property("granularity", th.StringType, required=True),

      # Same weather variables as ForecastHourlyStream
      th.Property("temperature_2m", th.NumberType),
      th.Property("relative_humidity_2m", th.NumberType),
      th.Property("dew_point_2m", th.NumberType),
      th.Property("apparent_temperature", th.NumberType),
      th.Property("wind_speed_10m", th.NumberType),
      th.Property("wind_direction_10m", th.NumberType),
      th.Property("wind_gusts_10m", th.NumberType),
      th.Property("precipitation", th.NumberType),
      th.Property("rain", th.NumberType),
      th.Property("snowfall", th.NumberType),
      th.Property("snow_depth", th.NumberType),
      th.Property("pressure_msl", th.NumberType),
      th.Property("cloud_cover", th.NumberType),
      th.Property("weather_code", th.IntegerType),

      th.Property("surrogate_key", th.StringType),
  ).to_dict()
  ```
- Primary keys: `["location_name", "model", "model_run_offset_days", "time", "granularity"]`

**Stream 7: `ClimateDailyStream`** — CMIP6 climate projections (optional)
- **Partitioned by (location_batch, model, date_chunk)**
- Path: `/v1/climate`
- Query params: `latitude=...&longitude=...&daily={vars}&models={model}&start_date=...&end_date=...`
- Daily resolution ONLY
- Schema:
  ```python
  schema = th.PropertiesList(
      th.Property("location_name", th.StringType, required=True),
      th.Property("latitude", th.NumberType, required=True),
      th.Property("longitude", th.NumberType, required=True),
      th.Property("model", th.StringType, required=True),
      th.Property("time", th.DateTimeType, required=True),
      th.Property("granularity", th.StringType, required=True),

      th.Property("temperature_2m_max", th.NumberType),
      th.Property("temperature_2m_min", th.NumberType),
      th.Property("temperature_2m_mean", th.NumberType),
      th.Property("precipitation_sum", th.NumberType),
      th.Property("rain_sum", th.NumberType),
      th.Property("snowfall_sum", th.NumberType),
      th.Property("wind_speed_10m_mean", th.NumberType),
      th.Property("wind_speed_10m_max", th.NumberType),
      th.Property("cloud_cover_mean", th.NumberType),
      th.Property("pressure_msl_mean", th.NumberType),
      th.Property("relative_humidity_2m_max", th.NumberType),
      th.Property("relative_humidity_2m_min", th.NumberType),
      th.Property("relative_humidity_2m_mean", th.NumberType),
      th.Property("shortwave_radiation_sum", th.NumberType),
      th.Property("soil_moisture_0_to_10cm_mean", th.NumberType),

      th.Property("surrogate_key", th.StringType),
  ).to_dict()
  ```
- Primary keys: `["location_name", "model", "time", "granularity"]`
- Replication key: `"time"`

### 4. Helpers (`helpers.py`)

```python
def pivot_columnar_to_rows(
    section_data: dict,
    times: list[str],
    location_name: str,
    latitude: float,
    longitude: float,
    elevation: float | None,
    model: str,
    granularity: str,
) -> list[dict]:
    """Convert Open-Meteo columnar format to row-based records.

    Input:  {"time": [t1, t2], "temp": [v1, v2], "wind": [w1, w2]}
    Output: [{"time": t1, "temp": v1, "wind": w1}, {"time": t2, "temp": v2, "wind": w2}]
    """

def pivot_ensemble_to_rows(
    section_data: dict,
    times: list[str],
    base_variables: list[str],
    location_name: str,
    latitude: float,
    longitude: float,
    elevation: float | None,
    model: str,
) -> list[dict]:
    """Convert ensemble columnar response to normalized member rows.

    Detects _memberNN suffixes, extracts member number, emits one row per (time, member).
    """

def pivot_previous_runs_to_rows(
    section_data: dict,
    times: list[str],
    base_variables: list[str],
    location_name: str,
    latitude: float,
    longitude: float,
    elevation: float | None,
    model: str,
) -> list[dict]:
    """Convert previous runs columnar response to normalized rows.

    Detects _previous_dayN suffixes, extracts offset, emits one row per (time, offset).
    """

def generate_surrogate_key(data: dict) -> str:
    """UUID5 from sorted field values."""

def clean_variable_name(name: str) -> str:
    """Normalize Open-Meteo variable names to snake_case if needed."""
```

### 5. Error Handling

- **Never silently skip data.** If an API call fails after retries, either raise (strict_mode=True) or log ERROR with full context (endpoint, locations, params) and continue to next partition.
- **Track skipped partitions** — Maintain a `_skipped_partitions` list like tap-fred. Log summary at end.
- **Validate response structure** — Check that `hourly` or `daily` key exists. Check that `time` array length matches data arrays. Log WARNING if lengths mismatch.
- **Handle None/null values** — Open-Meteo returns `null` for missing data points (e.g., no snowfall in summer). Keep as None, don't skip the record.
- **API error responses** — Open-Meteo returns `{"error": true, "reason": "..."}` on errors. Parse and log the reason.

### 6. Rate Limiting

Implement the sliding-window rate limiter from tap-fred (`_throttle()` in client.py):
- Max 500 requests per 60-second window (configurable, free tier is 600/min)
- Minimum 0.15 seconds between consecutive requests (configurable)
- Add random jitter: `random.uniform(0.05, 0.15)` seconds
- Thread-safe with lock

### 7. Testing

- `test_core.py` — Use `singer_sdk.testing.get_tap_test_class(TapOpenMateo)` for standard Singer tests
- Verify columnar-to-row pivot produces correct record count (len(time_array) records)
- Verify ensemble pivot normalizes member columns correctly
- Verify previous runs pivot normalizes `_previous_dayN` columns correctly
- Verify multi-location batching splits correctly
- Verify location presets resolve to valid lat/lon

### 8. pyproject.toml

The existing pyproject.toml is already correct for dependencies. Update the `[project.scripts]` entry and ensure `backoff` is in dependencies:

```toml
[project]
name = "tap-open-mateo"
version = "0.0.1"
description = "Singer tap for Open-Meteo weather forecast, historical, and ensemble APIs"
requires-python = ">=3.10,<4.0"

dependencies = [
    "singer-sdk~=0.53.5",
    "requests~=2.32.3",
    "backoff>=2.2.1,<3.0.0",
]

[project.scripts]
tap-open-mateo = "tap_open_mateo.tap:TapOpenMateo.cli"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

### 9. meltano.yml

```yaml
version: 1
send_anonymous_usage_stats: false
project_id: "tap-open-mateo"

plugins:
  extractors:
  - name: tap-open-mateo
    namespace: tap_open_mateo
    pip_url: -e .
    capabilities:
      - state
      - catalog
      - discover
      - about
      - stream-maps
    settings:
      - name: api_key
        kind: password
        sensitive: true
      - name: locations
        kind: array
      - name: location_presets
        kind: string
      - name: hourly_variables
        kind: array
      - name: daily_variables
        kind: array
      - name: models
        kind: array
      - name: ensemble_models
        kind: array
      - name: climate_models
        kind: array
      - name: temperature_unit
        kind: string
      - name: wind_speed_unit
        kind: string
      - name: precipitation_unit
        kind: string
      - name: timezone
        kind: string
      - name: forecast_days
        kind: integer
      - name: past_days
        kind: integer
      - name: previous_run_days
        kind: array
      - name: historical_start_date
        kind: date_iso8601
      - name: historical_end_date
        kind: date_iso8601
      - name: historical_chunk_days
        kind: integer
      - name: climate_start_date
        kind: date_iso8601
      - name: climate_end_date
        kind: date_iso8601
      - name: enabled_endpoints
        kind: array
      - name: max_requests_per_minute
        kind: integer
      - name: max_locations_per_request
        kind: integer
      - name: strict_mode
        kind: boolean
    select:
      - forecast_hourly.*
      - forecast_daily.*
      - historical_hourly.*
      - historical_daily.*
      - ensemble_hourly.*
      - previous_runs_hourly.*
      - climate_daily.*
    config:
      api_key: ${OPEN_METEO_API_KEY}
      location_presets: "us_population_centers"
      temperature_unit: "fahrenheit"
      wind_speed_unit: "mph"
      precipitation_unit: "inch"
      timezone: "America/Chicago"
      forecast_days: 16
      past_days: 7
      previous_run_days: [1, 2, 3, 5, 7]
      historical_start_date: "2020-01-01"
      historical_chunk_days: 365
      enabled_endpoints: ["forecast", "historical", "ensemble", "previous_runs"]
      ensemble_models: ["gfs025"]
      max_locations_per_request: 10
      max_requests_per_minute: 500

  loaders:
  - name: target-jsonl
    variant: andyh1203
    pip_url: target-jsonl
```

---

## Key Design Decisions

### Why Separate Streams per Endpoint (not one mega-stream)
Each Open-Meteo endpoint has different:
- Available variables (ensemble has fewer than forecast)
- Response structure (ensemble has member suffixes, previous runs has day suffixes)
- Partitioning strategy (historical needs date chunking, forecast doesn't)
- Replication behavior (historical is incremental, forecast is full extract)
- Rate limits / pricing (different subdomains)

### Why Pivot Columnar to Rows
Open-Meteo returns columnar data (`{"time": [...], "temp": [...], "wind": [...]}`). Singer/SQL targets expect row-based records. The pivot happens in the stream's `get_records()` method, NOT in `post_process()`, because it's a structural transformation, not a field-level cleanup.

### Why Normalize Ensemble Members and Previous Runs
Without normalization, you'd have 31 columns per variable (ensemble) or 8 columns per variable (previous runs). This makes downstream SQL/pandas extremely painful. Instead, add a `member` or `model_run_offset_days` column and emit separate rows. This is the standard "unpivot" / "melt" pattern for analytical data.

### Why Multi-Location Batching
Open-Meteo supports comma-separated lat/lon for multiple locations in a single request. Batching 10 locations per request reduces API calls by 10x. The response is a JSON array — iterate and emit records for each location.

---

## Data Volume Estimates

| Stream | Locations | Records/Location/Day | Total/Day |
|--------|-----------|---------------------|-----------|
| Forecast Hourly | 50 | 384 (16 days × 24h) | 19,200 |
| Forecast Daily | 50 | 16 | 800 |
| Historical Hourly | 50 | 24 (per day of backfill) | varies |
| Historical Daily | 50 | 1 (per day of backfill) | varies |
| Ensemble Hourly | 50 | 7,440 (240h × 31 members) | 372,000 |
| Previous Runs | 50 | 2,304 (384 × 6 offsets) | 115,200 |

**Historical backfill (one-time):** 50 locations × 86 years × 365 days × 24 hours = ~37.7M records. At 10 locations per API call and 365-day chunks = ~430 API calls. Trivial for paid tier.

**Daily production run:** ~507,200 records/day across all streams. ~200 API calls/day. Well within all rate limits.

---

## Critical Rules

1. **ALL imports at top of file.** No inline imports ever.
2. **No silent failures.** Every error must be logged with context or raised.
3. **No backwards compatibility code.** This is a new tap.
4. **DRY.** Single `_make_request()` for all API calls. Single pivot helper used by all streams.
5. **Pivot columnar to rows.** Open-Meteo returns columnar — Singer needs rows. This is the core transformation.
6. **Normalize ensemble members.** Don't emit 31 columns per variable. Emit separate rows with `member` field.
7. **Normalize previous runs.** Don't emit 8 columns per variable. Emit separate rows with `model_run_offset_days` field.
8. **Batch locations.** Use multi-location requests to minimize API calls.
9. **Chunk historical date ranges.** Don't request 86 years in one call. Chunk by `historical_chunk_days`.
10. **Use UV** for all Python execution: `uv run`, `uv sync`, etc.
11. **Match existing tap patterns** — Don't invent new patterns. Use the same class hierarchy, same error handling, same config parsing as tap-fred/tap-fmp/tap-massive.
12. **Test it.** Run `uv run tap-open-mateo --config config.json --discover` and verify schema. Run a small extraction and verify JSONL output has correct records.
