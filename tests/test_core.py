"""Tests for tap-open-mateo."""

from __future__ import annotations

from tap_open_mateo.helpers import (
    generate_surrogate_key,
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


def test_generate_surrogate_key():
    """Verify surrogate key is deterministic."""
    data = {"location_name": "NYC", "time": "2026-03-09T00:00", "model": "gfs"}
    key1 = generate_surrogate_key(data)
    key2 = generate_surrogate_key(data)
    assert key1 == key2
    assert len(key1) == 36  # UUID format


def test_generate_surrogate_key_different_data():
    """Verify different data produces different keys."""
    data1 = {"location_name": "NYC", "time": "2026-03-09T00:00"}
    data2 = {"location_name": "LA", "time": "2026-03-09T00:00"}
    assert generate_surrogate_key(data1) != generate_surrogate_key(data2)


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
    config = {
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
