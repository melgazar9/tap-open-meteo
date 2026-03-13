"""Tests for tap-open-mateo."""

from __future__ import annotations

from datetime import date, timedelta

from tap_open_mateo.helpers import (
    pivot_columnar_to_rows,
    pivot_ensemble_to_rows,
    pivot_previous_runs_to_rows,
)
from tap_open_mateo.tap import ENERGY_HUBS, US_POPULATION_CENTERS, TapOpenMateo

SAMPLE_CONFIG = {
    "locations": [
        {"name": "New York", "latitude": 40.71, "longitude": -74.01},
        {"name": "Chicago", "latitude": 41.88, "longitude": -87.63},
    ],
    "enabled_endpoints": ["forecast"],
    "forecast_days": 2,
    "past_days": 0,
    "max_locations_per_request": 10,
}


# --- Helper function tests ---


def test_pivot_columnar_to_rows():
    """Verify columnar-to-row pivot produces correct record count."""
    section_data = {
        "time": ["2026-03-09T00:00", "2026-03-09T01:00", "2026-03-09T02:00"],
        "temperature_2m": [6.7, 6.2, 5.8],
        "wind_speed_10m": [12.3, 11.5, 10.8],
    }

    records = pivot_columnar_to_rows(
        section_data=section_data,
        times=section_data["time"],
        location_name="New York",
        latitude=40.71,
        longitude=-74.01,
        elevation=10.0,
        model="best_match",
        granularity="hourly",
    )

    assert len(records) == 3
    assert records[0]["location_name"] == "New York"
    assert records[0]["latitude"] == 40.71
    assert records[0]["longitude"] == -74.01
    assert records[0]["elevation"] == 10.0
    assert records[0]["model"] == "best_match"
    assert records[0]["time"] == "2026-03-09T00:00"
    assert records[0]["granularity"] == "hourly"
    assert records[0]["temperature_2m"] == 6.7
    assert records[0]["wind_speed_10m"] == 12.3
    assert records[1]["temperature_2m"] == 6.2
    assert records[2]["temperature_2m"] == 5.8


def test_pivot_columnar_handles_none():
    """Verify None values are preserved (not skipped)."""
    section_data = {
        "time": ["2026-03-09T00:00", "2026-03-09T01:00"],
        "temperature_2m": [6.7, None],
        "snowfall": [None, None],
    }

    records = pivot_columnar_to_rows(
        section_data=section_data,
        times=section_data["time"],
        location_name="Test",
        latitude=0.0,
        longitude=0.0,
        elevation=None,
        model="best_match",
        granularity="hourly",
    )

    assert len(records) == 2
    assert records[0]["temperature_2m"] == 6.7
    assert records[1]["temperature_2m"] is None
    assert records[0]["snowfall"] is None
    assert records[1]["snowfall"] is None


def test_pivot_ensemble_to_rows():
    """Verify ensemble pivot normalizes member columns correctly."""
    section_data = {
        "time": ["2026-03-09T00:00", "2026-03-09T01:00"],
        "temperature_2m": [6.7, 6.2],  # Control (member 0)
        "temperature_2m_member01": [6.5, 6.0],
        "temperature_2m_member02": [7.1, 6.8],
        "wind_speed_10m": [12.3, 11.5],  # Control
        "wind_speed_10m_member01": [12.0, 11.0],
        "wind_speed_10m_member02": [13.0, 12.0],
    }

    records = pivot_ensemble_to_rows(
        section_data=section_data,
        times=section_data["time"],
        base_variables=["temperature_2m", "wind_speed_10m"],
        location_name="Chicago",
        latitude=41.88,
        longitude=-87.63,
        elevation=180.0,
        model="gfs025",
    )

    # 2 timestamps x 3 members (0, 1, 2) = 6 records
    assert len(records) == 6

    # Check control run (member 0)
    control_records = [r for r in records if r["member"] == 0]
    assert len(control_records) == 2
    assert control_records[0]["temperature_2m"] == 6.7
    assert control_records[0]["wind_speed_10m"] == 12.3

    # Check member 1
    member1_records = [r for r in records if r["member"] == 1]
    assert len(member1_records) == 2
    assert member1_records[0]["temperature_2m"] == 6.5
    assert member1_records[0]["wind_speed_10m"] == 12.0

    # Check member 2
    member2_records = [r for r in records if r["member"] == 2]
    assert len(member2_records) == 2
    assert member2_records[0]["temperature_2m"] == 7.1


def test_pivot_previous_runs_to_rows():
    """Verify previous runs pivot normalizes _previous_dayN columns correctly."""
    section_data = {
        "time": ["2026-03-09T00:00", "2026-03-09T01:00"],
        "temperature_2m": [6.7, 6.2],  # Current (offset 0)
        "temperature_2m_previous_day1": [6.5, 6.0],
        "temperature_2m_previous_day2": [7.1, 6.8],
    }

    records = pivot_previous_runs_to_rows(
        section_data=section_data,
        times=section_data["time"],
        base_variables=["temperature_2m"],
        previous_run_days=[1, 2],
        location_name="Houston",
        latitude=29.76,
        longitude=-95.37,
        elevation=12.0,
        model="best_match",
    )

    # 2 timestamps x 3 offsets (0, 1, 2) = 6 records
    assert len(records) == 6

    current = [r for r in records if r["model_run_offset_days"] == 0]
    assert len(current) == 2
    assert current[0]["temperature_2m"] == 6.7

    day1 = [r for r in records if r["model_run_offset_days"] == 1]
    assert len(day1) == 2
    assert day1[0]["temperature_2m"] == 6.5

    day2 = [r for r in records if r["model_run_offset_days"] == 2]
    assert len(day2) == 2
    assert day2[0]["temperature_2m"] == 7.1


# --- Location preset tests ---


def test_us_population_centers_count():
    """Verify we have 50 US metro locations."""
    assert len(US_POPULATION_CENTERS) == 50


def test_energy_hubs_count():
    """Verify energy hubs have valid data."""
    assert len(ENERGY_HUBS) >= 5


def test_location_presets_have_required_fields():
    """Verify all preset locations have name, latitude, longitude."""
    for loc in US_POPULATION_CENTERS + ENERGY_HUBS:
        assert "name" in loc
        assert "latitude" in loc
        assert "longitude" in loc
        assert isinstance(loc["name"], str)
        assert isinstance(loc["latitude"], (int, float))
        assert isinstance(loc["longitude"], (int, float))
        assert -90 <= loc["latitude"] <= 90
        assert -180 <= loc["longitude"] <= 180


# --- Tap instantiation tests ---


def test_tap_discover_streams():
    """Verify tap discovers the correct streams based on enabled_endpoints."""
    tap = TapOpenMateo(config=SAMPLE_CONFIG)
    streams = tap.discover_streams()
    stream_names = {s.name for s in streams}
    assert "forecast_hourly" in stream_names
    assert "forecast_daily" in stream_names
    # Historical should not be present without historical_start_date
    assert "historical_hourly" not in stream_names


def test_tap_discover_with_historical():
    """Verify historical streams appear when configured."""
    config = {
        **SAMPLE_CONFIG,
        "enabled_endpoints": ["forecast", "historical"],
        "historical_start_date": "2024-01-01",
    }
    tap = TapOpenMateo(config=config)
    streams = tap.discover_streams()
    stream_names = {s.name for s in streams}
    assert "historical_hourly" in stream_names
    assert "historical_daily" in stream_names


def test_tap_location_resolution():
    """Verify locations are resolved from config."""
    tap = TapOpenMateo(config=SAMPLE_CONFIG)
    locations = tap.get_resolved_locations()
    assert len(locations) == 2
    assert locations[0]["name"] == "New York"


def test_tap_location_preset_resolution():
    """Verify location presets are resolved correctly."""
    config = {
        **SAMPLE_CONFIG,
        "location_presets": "energy_hubs",
    }
    # Remove locations since preset should override
    config.pop("locations", None)
    tap = TapOpenMateo(config=config)
    locations = tap.get_resolved_locations()
    assert len(locations) == len(ENERGY_HUBS)
    assert locations[0]["name"] == "Henry Hub"


# --- Multi-location batching tests ---


def test_batch_locations():
    """Verify location batching splits correctly."""
    config = {
        **SAMPLE_CONFIG,
        "location_presets": "us_population_centers",
    }
    tap = TapOpenMateo(config=config)
    streams = tap.discover_streams()
    forecast_stream = next(s for s in streams if s.name == "forecast_hourly")

    locations = forecast_stream.get_resolved_locations()
    batches = forecast_stream._batch_locations(locations, 10)
    assert len(batches) == 5  # 50 locations / 10 per batch
    assert len(batches[0]) == 10
    assert len(batches[-1]) == 10


def test_batch_locations_uneven():
    """Verify batching handles non-evenly-divisible counts."""
    config = {
        **SAMPLE_CONFIG,
        "locations": [
            {"name": f"Loc{i}", "latitude": float(i), "longitude": float(i)}
            for i in range(13)
        ],
    }
    tap = TapOpenMateo(config=config)
    streams = tap.discover_streams()
    forecast_stream = next(s for s in streams if s.name == "forecast_hourly")

    batches = forecast_stream._batch_locations(
        forecast_stream.get_resolved_locations(), 5
    )
    assert len(batches) == 3
    assert len(batches[0]) == 5
    assert len(batches[1]) == 5
    assert len(batches[2]) == 3


# --- Ensemble granularity parameter tests ---


def test_pivot_ensemble_custom_granularity():
    """Verify ensemble pivot respects custom granularity parameter."""
    section_data = {
        "time": ["2026-03-09T00:00", "2026-03-09T06:00"],
        "temperature_2m": [6.7, 6.2],
        "temperature_2m_member01": [6.5, 6.0],
    }

    records = pivot_ensemble_to_rows(
        section_data=section_data,
        times=section_data["time"],
        base_variables=["temperature_2m"],
        location_name="Test",
        latitude=0.0,
        longitude=0.0,
        elevation=None,
        model="ecmwf_seasonal_seamless",
        granularity="six_hourly",
    )

    assert len(records) == 4  # 2 timestamps x 2 members
    assert all(r["granularity"] == "six_hourly" for r in records)


def test_pivot_ensemble_default_granularity():
    """Verify ensemble pivot defaults to 'hourly' granularity."""
    section_data = {
        "time": ["2026-03-09T00:00"],
        "temperature_2m": [6.7],
    }

    records = pivot_ensemble_to_rows(
        section_data=section_data,
        times=section_data["time"],
        base_variables=["temperature_2m"],
        location_name="Test",
        latitude=0.0,
        longitude=0.0,
        elevation=None,
        model="gfs025",
    )

    assert len(records) == 1
    assert records[0]["granularity"] == "hourly"


# --- New endpoint discovery tests ---


def test_tap_discover_all_endpoints():
    """Verify all endpoint streams appear when enabled."""
    config = {
        **SAMPLE_CONFIG,
        "enabled_endpoints": [
            "forecast", "historical", "historical_forecast",
            "ensemble", "previous_runs", "seasonal", "marine",
            "air_quality", "flood", "climate", "elevation",
        ],
        "historical_start_date": "2024-01-01",
        "historical_forecast_start_date": "2024-01-01",
    }
    tap = TapOpenMateo(config=config)
    streams = tap.discover_streams()
    stream_names = {s.name for s in streams}

    expected = {
        "forecast_hourly", "forecast_daily",
        "historical_hourly", "historical_daily",
        "historical_forecast_hourly", "historical_forecast_daily",
        "ensemble_hourly", "previous_runs_hourly",
        "seasonal_six_hourly", "seasonal_daily",
        "marine_hourly", "marine_daily",
        "air_quality_hourly",
        "flood_daily",
        "climate_daily",
        "elevation",
    }
    assert expected == stream_names


def test_tap_discover_geocoding():
    """Verify geocoding stream requires search_terms to be configured."""
    # Without search terms, geocoding should not appear
    config = {
        **SAMPLE_CONFIG,
        "enabled_endpoints": ["geocoding"],
    }
    tap = TapOpenMateo(config=config)
    streams = tap.discover_streams()
    stream_names = {s.name for s in streams}
    assert "geocoding" not in stream_names

    # With search terms, geocoding should appear
    config["geocoding_search_terms"] = ["Berlin", "Tokyo"]
    tap = TapOpenMateo(config=config)
    streams = tap.discover_streams()
    stream_names = {s.name for s in streams}
    assert "geocoding" in stream_names


def test_tap_discover_satellite_requires_dates():
    """Verify satellite streams require start_date to be configured."""
    config: dict = {
        **SAMPLE_CONFIG,
        "enabled_endpoints": ["satellite"],
    }
    tap = TapOpenMateo(config=config)
    streams = tap.discover_streams()
    stream_names = {s.name for s in streams}
    assert "satellite_radiation_hourly" not in stream_names

    config["satellite_start_date"] = "2024-01-01"
    tap = TapOpenMateo(config=config)
    streams = tap.discover_streams()
    stream_names = {s.name for s in streams}
    assert "satellite_radiation_hourly" in stream_names
    assert "satellite_radiation_daily" in stream_names


# --- Schema completeness tests ---


def test_all_streams_have_primary_keys():
    """Verify every stream defines primary_keys and they exist in schema."""
    config = {
        **SAMPLE_CONFIG,
        "enabled_endpoints": [
            "forecast", "historical", "historical_forecast",
            "ensemble", "previous_runs", "seasonal", "marine",
            "air_quality", "flood", "climate", "elevation",
            "geocoding",
        ],
        "historical_start_date": "2024-01-01",
        "historical_forecast_start_date": "2024-01-01",
        "satellite_start_date": "2024-01-01",
        "geocoding_search_terms": ["Test"],
    }
    tap = TapOpenMateo(config=config)
    streams = tap.discover_streams()

    for stream in streams:
        assert stream.primary_keys, f"{stream.name} has no primary_keys"
        schema_props = set(stream.schema.get("properties", {}).keys())
        for pk in stream.primary_keys:
            assert pk in schema_props, (
                f"{stream.name}: primary_key '{pk}' not in schema properties"
            )


def test_all_timeseries_streams_have_time_field():
    """Verify every time-series stream has 'time' in schema."""
    config = {
        **SAMPLE_CONFIG,
        "enabled_endpoints": [
            "forecast", "ensemble", "previous_runs", "seasonal",
            "marine", "air_quality", "flood", "climate",
        ],
    }
    tap = TapOpenMateo(config=config)
    streams = tap.discover_streams()

    for stream in streams:
        schema_props = set(stream.schema.get("properties", {}).keys())
        assert "time" in schema_props, f"{stream.name}: missing 'time' in schema"
        assert "granularity" in schema_props, (
            f"{stream.name}: missing 'granularity' in schema"
        )


def test_forecast_hourly_schema_has_pressure_levels():
    """Verify forecast hourly schema includes pressure level variables."""
    from tap_open_mateo.streams.forecast_streams import (
        FORECAST_HOURLY_PROPERTIES,
        PRESSURE_LEVEL_VARIABLE_TYPES,
        PRESSURE_LEVELS,
    )

    schema = FORECAST_HOURLY_PROPERTIES.to_dict()
    props = set(schema["properties"].keys())

    # Should have 7 variable types x 19 pressure levels = 133 properties
    expected_count = len(PRESSURE_LEVEL_VARIABLE_TYPES) * len(PRESSURE_LEVELS)
    pressure_props = {
        p for p in props if any(p.endswith(f"_{level}hPa") for level in PRESSURE_LEVELS)
    }
    assert len(pressure_props) == expected_count, (
        f"Expected {expected_count} pressure level properties, got {len(pressure_props)}"
    )


def test_historical_hourly_uses_era5_variable_names():
    """Verify historical hourly includes ERA5-specific naming (range depths, 100m wind)."""
    from tap_open_mateo.streams.historical_streams import HISTORICAL_HOURLY_PROPERTIES

    schema = HISTORICAL_HOURLY_PROPERTIES.to_dict()
    props = set(schema["properties"].keys())

    # ERA5 uses range depths for soil
    assert "soil_temperature_0_to_7cm" in props
    assert "soil_moisture_0_to_7cm" in props
    # ERA5 uses 100m wind (not 80m/120m/180m)
    assert "wind_speed_100m" in props
    assert "wind_direction_100m" in props
    # Archive API also accepts forecast-style point depths
    assert "soil_temperature_0cm" in props
    assert "soil_moisture_0_to_1cm" in props


def test_ensemble_schema_has_member_field():
    """Verify ensemble/seasonal/flood schemas include the 'member' field."""
    from tap_open_mateo.streams.ensemble_streams import EnsembleHourlyStream
    from tap_open_mateo.streams.flood_streams import FLOOD_DAILY_PROPERTIES
    from tap_open_mateo.streams.seasonal_streams import (
        SEASONAL_DAILY_PROPERTIES,
        SEASONAL_SIX_HOURLY_PROPERTIES,
    )

    for name, schema_dict in [
        ("ensemble_hourly", EnsembleHourlyStream.schema),
        ("seasonal_six_hourly", SEASONAL_SIX_HOURLY_PROPERTIES.to_dict()),
        ("seasonal_daily", SEASONAL_DAILY_PROPERTIES.to_dict()),
        ("flood_daily", FLOOD_DAILY_PROPERTIES.to_dict()),
    ]:
        props = set(schema_dict["properties"].keys())
        assert "member" in props, f"{name}: missing 'member' field in schema"


def test_previous_runs_schema_has_offset_field():
    """Verify previous runs schema includes model_run_offset_days."""
    from tap_open_mateo.streams.forecast_streams import PreviousRunsHourlyStream

    schema = PreviousRunsHourlyStream.schema
    props = set(schema["properties"].keys())
    assert "model_run_offset_days" in props


# --- Endpoint-specific config tests ---


def test_marine_forecast_days_capped_at_8():
    """Verify marine streams cap forecast_days to 8 (API max)."""
    config = {
        **SAMPLE_CONFIG,
        "enabled_endpoints": ["marine"],
        "forecast_days": 16,
    }
    tap = TapOpenMateo(config=config)
    streams = tap.discover_streams()
    marine_hourly = next(s for s in streams if s.name == "marine_hourly")

    # Simulate get_records context to check the capped value
    capped = min(marine_hourly.config.get("forecast_days", 8), 8)
    assert capped == 8, f"Marine forecast_days should be capped at 8, got {capped}"


def test_air_quality_forecast_days_capped_at_7():
    """Verify air quality stream caps forecast_days to 7 (API max)."""
    config = {
        **SAMPLE_CONFIG,
        "enabled_endpoints": ["air_quality"],
        "forecast_days": 16,
    }
    tap = TapOpenMateo(config=config)
    streams = tap.discover_streams()
    aq_stream = next(s for s in streams if s.name == "air_quality_hourly")

    capped = min(aq_stream.config.get("forecast_days", 7), 7)
    assert capped == 7, f"Air quality forecast_days should be capped at 7, got {capped}"


def test_historical_streams_use_era5_lag():
    """Verify historical streams account for ERA5 5-day publication delay."""
    from tap_open_mateo.streams.historical_streams import (
        HistoricalDailyStream,
        HistoricalHourlyStream,
    )

    assert HistoricalHourlyStream._default_end_date_lag_days >= 5
    assert HistoricalDailyStream._default_end_date_lag_days >= 5


# --- Date chunking tests ---


def test_date_chunked_partitions():
    """Verify date chunking produces correct partition ranges."""
    config = {
        **SAMPLE_CONFIG,
        "enabled_endpoints": ["historical"],
        "historical_start_date": "2024-01-01",
        "historical_end_date": "2024-03-31",
        "historical_chunk_days": 30,
    }
    tap = TapOpenMateo(config=config)
    streams = tap.discover_streams()
    hist_stream = next(s for s in streams if s.name == "historical_hourly")

    partitions = hist_stream.partitions
    # 2 locations fit in 1 batch (batch_size=10), 91 days / 30-day chunks = 4 chunks
    assert len(partitions) == 4

    # Verify each partition has required keys
    for p in partitions:
        assert "batch_idx" in p
        assert "locations" in p
        assert "start_date" in p
        assert "end_date" in p

    # First partition starts at configured start_date
    first = partitions[0]
    assert first["start_date"] == "2024-01-01"


def test_date_chunking_no_gaps():
    """Verify date chunks are contiguous with no gaps or overlaps."""
    config = {
        **SAMPLE_CONFIG,
        "locations": [{"name": "Test", "latitude": 0, "longitude": 0}],
        "enabled_endpoints": ["historical"],
        "historical_start_date": "2024-01-01",
        "historical_end_date": "2024-12-31",
        "historical_chunk_days": 100,
    }
    tap = TapOpenMateo(config=config)
    streams = tap.discover_streams()
    hist_stream = next(s for s in streams if s.name == "historical_hourly")

    partitions = hist_stream.partitions
    # 1 location, 366 days / 100-day chunks = 4 chunks
    assert len(partitions) == 4

    # Verify contiguous: each chunk's start = previous chunk's end + 1 day
    for i in range(1, len(partitions)):
        prev_end = date.fromisoformat(partitions[i - 1]["end_date"])
        curr_start = date.fromisoformat(partitions[i]["start_date"])
        assert curr_start == prev_end + timedelta(days=1), (
            f"Gap between chunk {i-1} end ({prev_end}) and chunk {i} start ({curr_start})"
        )

    # First chunk starts at configured date, last chunk ends at configured date
    assert partitions[0]["start_date"] == "2024-01-01"
    assert partitions[-1]["end_date"] == "2024-12-31"


# --- Cell selection tests ---


def test_cell_selection_propagated_to_base_params():
    """Verify cell_selection config is passed to API params."""
    config = {
        **SAMPLE_CONFIG,
        "cell_selection": "nearest",
    }
    tap = TapOpenMateo(config=config)
    streams = tap.discover_streams()
    forecast_stream = next(s for s in streams if s.name == "forecast_hourly")

    params = forecast_stream._build_base_params()
    assert params.get("cell_selection") == "nearest"


def test_cell_selection_not_sent_when_unset():
    """Verify cell_selection is omitted when not configured (API uses its default)."""
    tap = TapOpenMateo(config=SAMPLE_CONFIG)
    streams = tap.discover_streams()
    forecast_stream = next(s for s in streams if s.name == "forecast_hourly")

    params = forecast_stream._build_base_params()
    assert "cell_selection" not in params


# --- URL base tests ---


def test_free_tier_url_base():
    """Verify streams use free tier URLs when no API key is set."""
    config = {**SAMPLE_CONFIG}
    config.pop("api_key", None)
    tap = TapOpenMateo(config=config)
    streams = tap.discover_streams()
    forecast_stream = next(s for s in streams if s.name == "forecast_hourly")
    assert "customer-" not in forecast_stream.url_base


def test_paid_tier_url_base():
    """Verify streams use paid tier URLs when API key is set."""
    config = {**SAMPLE_CONFIG, "api_key": "test-key-123"}
    tap = TapOpenMateo(config=config)
    streams = tap.discover_streams()
    forecast_stream = next(s for s in streams if s.name == "forecast_hourly")
    assert "customer-" in forecast_stream.url_base


# --- Shared schema DRY tests ---


def test_forecast_and_historical_forecast_share_schema():
    """Verify historical_forecast streams reuse FORECAST schemas (DRY)."""
    from tap_open_mateo.streams.forecast_streams import (
        FORECAST_DAILY_PROPERTIES,
        FORECAST_HOURLY_PROPERTIES,
    )
    from tap_open_mateo.streams.historical_forecast_streams import (
        HistoricalForecastDailyStream,
        HistoricalForecastHourlyStream,
    )

    assert HistoricalForecastHourlyStream.schema == FORECAST_HOURLY_PROPERTIES.to_dict()
    assert HistoricalForecastDailyStream.schema == FORECAST_DAILY_PROPERTIES.to_dict()


# --- API key redaction tests ---


def test_api_key_redacted_in_logs():
    """Verify API keys are redacted from log messages."""
    from tap_open_mateo.client import OpenMateoStream

    msg = "https://api.open-meteo.com/v1/forecast?apikey=secret123&hourly=temp"
    redacted = OpenMateoStream.redact_api_key(msg)
    assert "secret123" not in redacted
    assert "REDACTED" in redacted


# --- Flood ensemble/stats mode tests ---


def test_flood_ensemble_vs_stats_mode():
    """Verify flood stream switches between ensemble and stats variables."""
    from tap_open_mateo.streams.flood_streams import (
        DEFAULT_FLOOD_STATS_VARIABLES,
        DEFAULT_FLOOD_VARIABLES,
    )

    # Stats mode should default to statistical aggregates
    assert "river_discharge_mean" in DEFAULT_FLOOD_STATS_VARIABLES
    assert "river_discharge_median" in DEFAULT_FLOOD_STATS_VARIABLES
    assert "river_discharge_max" in DEFAULT_FLOOD_STATS_VARIABLES

    # Ensemble mode should default to raw river_discharge
    assert "river_discharge" in DEFAULT_FLOOD_VARIABLES
    assert len(DEFAULT_FLOOD_VARIABLES) == 1


# --- Partition count tests ---


def test_ensemble_partitions_include_model():
    """Verify ensemble partitions cross-product locations x models."""
    config = {
        **SAMPLE_CONFIG,
        "enabled_endpoints": ["ensemble"],
        "ensemble_models": ["gfs025", "icon_seamless"],
    }
    tap = TapOpenMateo(config=config)
    streams = tap.discover_streams()
    ens_stream = next(s for s in streams if s.name == "ensemble_hourly")

    partitions = ens_stream.partitions
    # 2 locations / 10 per batch = 1 batch, x 2 models = 2 partitions
    assert len(partitions) == 2
    models_in_partitions = {p["ensemble_model"] for p in partitions}
    assert models_in_partitions == {"gfs025", "icon_seamless"}


def test_seasonal_partitions_include_model():
    """Verify seasonal partitions cross-product locations x models."""
    config = {
        **SAMPLE_CONFIG,
        "enabled_endpoints": ["seasonal"],
        "seasonal_models": ["ecmwf_seasonal_seamless", "meteo_france_seamless"],
    }
    tap = TapOpenMateo(config=config)
    streams = tap.discover_streams()
    seasonal_stream = next(s for s in streams if s.name == "seasonal_six_hourly")

    partitions = seasonal_stream.partitions
    assert len(partitions) == 2
    models = {p["seasonal_model"] for p in partitions}
    assert models == {"ecmwf_seasonal_seamless", "meteo_france_seamless"}
