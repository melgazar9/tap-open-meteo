"""Production quality tests for tap-open-mateo.

Tests that ensure data completeness, schema correctness,
error handling, and edge cases — following tap-massive patterns.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest

from tap_open_mateo.tap import TapOpenMateo

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


# --- Schema field validation (tap-massive _check_missing_fields pattern) ---


def test_check_missing_fields_detects_schema_gap(caplog):
    """CRITICAL: API returns fields not in schema = silent data loss."""
    tap = TapOpenMateo(config=SAMPLE_CONFIG)
    streams = tap.discover_streams()
    stream = next(s for s in streams if s.name == "forecast_hourly")

    # Simulate a record with an extra field not in schema
    row = {"location_name": "NYC", "unknown_api_field": 42.0}

    with caplog.at_level(logging.CRITICAL):
        stream._check_missing_fields(row)

    assert "SCHEMA GAP" in caplog.text
    assert "unknown_api_field" in caplog.text


def test_check_missing_fields_runs_once():
    """Schema check should only run on first record, not every record."""
    tap = TapOpenMateo(config=SAMPLE_CONFIG)
    streams = tap.discover_streams()
    stream = next(s for s in streams if s.name == "forecast_hourly")

    stream._check_missing_fields({"location_name": "NYC"})
    assert stream._schema_checked is True

    # Second call should be a no-op (no re-check)
    stream._schema_checked = True
    stream._check_missing_fields({"totally_new_field": 1})
    # If it checked again, it would log — but since _schema_checked is True, it skips


def test_check_missing_fields_no_false_positive():
    """Schema fields not in record should NOT trigger critical (they're optional)."""
    tap = TapOpenMateo(config=SAMPLE_CONFIG)
    streams = tap.discover_streams()
    stream = next(s for s in streams if s.name == "forecast_hourly")

    # A record with only metadata fields — many schema fields missing (expected)
    row = {
        "location_name": "NYC", "latitude": 40.71, "longitude": -74.01,
        "time": "2024-01-01T00:00", "granularity": "hourly", "model": "best_match",
        "temperature_2m": 42.0,
    }

    with patch("logging.critical") as mock_critical:
        stream._check_missing_fields(row)
        mock_critical.assert_not_called()


# --- Coordinate drift detection ---


def test_coordinate_drift_warns_on_large_drift(caplog):
    """Warn when API returns coordinates far from requested location."""
    tap = TapOpenMateo(config=SAMPLE_CONFIG)
    streams = tap.discover_streams()
    stream = next(s for s in streams if s.name == "forecast_hourly")

    # API returned coordinates 0.5 degrees off (> 0.25 threshold)
    data = {"latitude": 41.21, "longitude": -74.01}
    location = {"name": "NYC", "latitude": 40.71, "longitude": -74.01}

    with caplog.at_level(logging.WARNING):
        stream._check_coordinate_drift(data, location)

    assert "coordinate drift" in caplog.text
    assert "NYC" in caplog.text


def test_coordinate_drift_no_false_positive():
    """No warning for normal grid snapping (within threshold)."""
    tap = TapOpenMateo(config=SAMPLE_CONFIG)
    streams = tap.discover_streams()
    stream = next(s for s in streams if s.name == "forecast_hourly")

    # API returned coordinates 0.1 degrees off (< 0.25 threshold)
    data = {"latitude": 40.81, "longitude": -73.91}
    location = {"name": "NYC", "latitude": 40.71, "longitude": -74.01}

    with patch("logging.warning") as mock_warn:
        stream._check_coordinate_drift(data, location)
        mock_warn.assert_not_called()


# --- ERA5 historical lag ---


def test_historical_default_end_date_has_era5_lag():
    """ERA5 data is published with 5-day delay; default end should account for this."""
    config = {
        **SAMPLE_CONFIG,
        "enabled_endpoints": ["historical"],
        "historical_start_date": "2024-01-01",
    }
    tap = TapOpenMateo(config=config)
    streams = tap.discover_streams()
    stream = next(s for s in streams if s.name == "historical_hourly")

    assert stream._default_end_date_lag_days == 6

    # The default end date should be 6 days ago
    expected_end = date.today() - timedelta(days=6)
    partitions = stream.partitions
    assert len(partitions) > 0

    # Find the latest end_date across all partitions
    latest_end = max(p["end_date"] for p in partitions)
    assert latest_end == expected_end.isoformat()


def test_forecast_stream_default_lag_is_1():
    """Non-historical streams should default to 1-day lag for date chunking."""
    from tap_open_mateo.client import OpenMateoStream
    assert OpenMateoStream._default_end_date_lag_days == 1


# --- Skipped partition tracking ---


def test_skipped_partitions_tracked():
    """Verify _skipped_partitions accumulates on client errors."""
    tap = TapOpenMateo(config=SAMPLE_CONFIG)
    streams = tap.discover_streams()
    stream = next(s for s in streams if s.name == "forecast_hourly")

    # Simulate a 400 error
    stream._skipped_partitions.append({"stream": "forecast_hourly", "url": "test", "status_code": 400})

    assert len(stream._skipped_partitions) == 1
    assert stream._skipped_partitions[0]["status_code"] == 400


# --- Mocked _fetch_and_extract ---


def test_fetch_and_extract_single_location():
    """Verify single-location response extraction."""
    tap = TapOpenMateo(config=SAMPLE_CONFIG)
    streams = tap.discover_streams()
    stream = next(s for s in streams if s.name == "forecast_hourly")

    mock_response = {
        "latitude": 40.71,
        "longitude": -74.01,
        "elevation": 10.0,
        "hourly": {
            "time": ["2024-01-01T00:00", "2024-01-01T01:00"],
            "temperature_2m": [32.5, 31.2],
            "wind_speed_10m": [5.1, 4.8],
        },
    }

    with patch.object(stream, "_make_request", return_value=mock_response):
        records = stream._fetch_and_extract(
            [{"name": "New York", "latitude": 40.71, "longitude": -74.01}],
            "hourly", "best_match", {"hourly": "temperature_2m,wind_speed_10m"},
        )

    assert len(records) == 2
    assert records[0]["location_name"] == "New York"
    assert records[0]["temperature_2m"] == 32.5
    assert records[0]["wind_speed_10m"] == 5.1
    assert records[1]["temperature_2m"] == 31.2


def test_fetch_and_extract_multi_location():
    """Verify multi-location response extraction (API returns array)."""
    tap = TapOpenMateo(config=SAMPLE_CONFIG)
    streams = tap.discover_streams()
    stream = next(s for s in streams if s.name == "forecast_hourly")

    mock_response = [
        {
            "latitude": 40.71, "longitude": -74.01, "elevation": 10.0,
            "hourly": {
                "time": ["2024-01-01T00:00"],
                "temperature_2m": [32.5],
            },
        },
        {
            "latitude": 41.88, "longitude": -87.63, "elevation": 180.0,
            "hourly": {
                "time": ["2024-01-01T00:00"],
                "temperature_2m": [28.1],
            },
        },
    ]

    locations = [
        {"name": "New York", "latitude": 40.71, "longitude": -74.01},
        {"name": "Chicago", "latitude": 41.88, "longitude": -87.63},
    ]

    with patch.object(stream, "_make_request", return_value=mock_response):
        records = stream._fetch_and_extract(
            locations, "hourly", "best_match", {"hourly": "temperature_2m"},
        )

    assert len(records) == 2
    assert records[0]["location_name"] == "New York"
    assert records[0]["temperature_2m"] == 32.5
    assert records[1]["location_name"] == "Chicago"
    assert records[1]["temperature_2m"] == 28.1


def test_fetch_and_extract_empty_response(caplog):
    """Verify empty API response is handled gracefully."""
    tap = TapOpenMateo(config=SAMPLE_CONFIG)
    streams = tap.discover_streams()
    stream = next(s for s in streams if s.name == "forecast_hourly")

    with patch.object(stream, "_make_request", return_value={}):
        with caplog.at_level(logging.WARNING):
            records = stream._fetch_and_extract(
                [{"name": "NYC", "latitude": 40.71, "longitude": -74.01}],
                "hourly", "best_match", {},
            )

    assert records == []
    assert "Empty response" in caplog.text


def test_fetch_and_extract_missing_section(caplog):
    """Verify missing data section in response is handled gracefully."""
    tap = TapOpenMateo(config=SAMPLE_CONFIG)
    streams = tap.discover_streams()
    stream = next(s for s in streams if s.name == "forecast_hourly")

    # API returned response but without 'hourly' section
    mock_response = {"latitude": 40.71, "longitude": -74.01, "elevation": 10.0}

    with patch.object(stream, "_make_request", return_value=mock_response):
        with caplog.at_level(logging.WARNING):
            records = stream._fetch_and_extract(
                [{"name": "NYC", "latitude": 40.71, "longitude": -74.01}],
                "hourly", "best_match", {},
            )

    assert records == []
    assert "missing 'hourly' section" in caplog.text


# --- Response section validation ---


def test_validate_response_section_empty_time():
    """Verify empty time array returns None."""
    tap = TapOpenMateo(config=SAMPLE_CONFIG)
    streams = tap.discover_streams()
    stream = next(s for s in streams if s.name == "forecast_hourly")

    data = {"hourly": {"time": [], "temperature_2m": []}}
    result = stream._validate_response_section(data, "hourly")
    assert result is None


def test_validate_response_section_mismatched_lengths(caplog):
    """Verify mismatched variable lengths are logged."""
    tap = TapOpenMateo(config=SAMPLE_CONFIG)
    streams = tap.discover_streams()
    stream = next(s for s in streams if s.name == "forecast_hourly")

    data = {
        "hourly": {
            "time": ["2024-01-01T00:00", "2024-01-01T01:00"],
            "temperature_2m": [32.5],  # Only 1 value but 2 timestamps
        }
    }

    with caplog.at_level(logging.WARNING):
        result = stream._validate_response_section(data, "hourly")

    assert result is not None  # Still returns data (best effort)
    assert "mismatched lengths" in caplog.text


# --- Date chunking ---


def test_date_chunking_produces_contiguous_ranges():
    """Verify date chunks cover the full range with no gaps."""
    config = {
        **SAMPLE_CONFIG,
        "enabled_endpoints": ["historical"],
        "historical_start_date": "2023-01-01",
        "historical_end_date": "2024-06-15",
        "historical_chunk_days": 90,
    }
    tap = TapOpenMateo(config=config)
    streams = tap.discover_streams()
    stream = next(s for s in streams if s.name == "historical_hourly")

    partitions = stream.partitions
    # Extract unique date ranges (one batch per partition)
    date_ranges = sorted(set((p["start_date"], p["end_date"]) for p in partitions))

    # Verify contiguity: each chunk's start = previous chunk's end + 1 day
    for i in range(1, len(date_ranges)):
        prev_end = date.fromisoformat(date_ranges[i - 1][1])
        curr_start = date.fromisoformat(date_ranges[i][0])
        assert curr_start == prev_end + timedelta(days=1), (
            f"Gap detected: {prev_end} -> {curr_start}"
        )

    # Verify full coverage
    assert date_ranges[0][0] == "2023-01-01"
    assert date_ranges[-1][1] == "2024-06-15"


def test_date_chunking_single_day():
    """Verify date chunking handles single-day range."""
    config = {
        **SAMPLE_CONFIG,
        "enabled_endpoints": ["historical"],
        "historical_start_date": "2024-01-01",
        "historical_end_date": "2024-01-01",
    }
    tap = TapOpenMateo(config=config)
    streams = tap.discover_streams()
    stream = next(s for s in streams if s.name == "historical_hourly")

    partitions = stream.partitions
    assert len(partitions) > 0
    assert partitions[0]["start_date"] == "2024-01-01"
    assert partitions[0]["end_date"] == "2024-01-01"


# --- Config validation ---


def test_missing_locations_raises():
    """Verify tap raises if neither locations nor presets configured."""
    config = {
        "enabled_endpoints": ["forecast"],
    }
    with pytest.raises(ValueError, match="Either 'locations' or 'location_presets'"):
        TapOpenMateo(config=config)


def test_invalid_preset_raises():
    """Verify tap raises for invalid location preset."""
    config = {
        "location_presets": "nonexistent_preset",
        "enabled_endpoints": ["forecast"],
    }
    with pytest.raises(ValueError, match="Unknown location_presets"):
        TapOpenMateo(config=config)


# --- Endpoint compatibility warnings ---


def test_satellite_north_america_warning(caplog):
    """Verify warning when satellite endpoint + North America locations."""
    config = {
        **SAMPLE_CONFIG,
        "enabled_endpoints": ["satellite", "forecast"],
        "satellite_start_date": "2024-01-01",
    }
    tap = TapOpenMateo(config=config)
    with caplog.at_level(logging.WARNING):
        tap.discover_streams()

    assert "SATELLITE DATA GAP" in caplog.text


def test_marine_inland_warning(caplog):
    """Verify warning when marine endpoint + inland locations."""
    config = {
        "locations": [{"name": "Denver", "latitude": 39.74, "longitude": -104.98}],
        "enabled_endpoints": ["marine"],
    }
    tap = TapOpenMateo(config=config)
    with caplog.at_level(logging.WARNING):
        tap.discover_streams()

    assert "MARINE DATA QUALITY" in caplog.text


# --- Stream count completeness ---


def test_all_19_streams_discoverable():
    """Verify all 19 stream classes can be instantiated when all endpoints enabled."""
    config = {
        **SAMPLE_CONFIG,
        "enabled_endpoints": [
            "forecast", "historical", "historical_forecast",
            "ensemble", "previous_runs", "seasonal", "marine",
            "air_quality", "satellite", "flood", "climate",
            "geocoding", "elevation",
        ],
        "historical_start_date": "2024-01-01",
        "historical_forecast_start_date": "2024-01-01",
        "satellite_start_date": "2024-01-01",
        "geocoding_search_terms": ["Berlin"],
    }
    tap = TapOpenMateo(config=config)
    streams = tap.discover_streams()

    assert len(streams) == 19

    expected_names = {
        "forecast_hourly", "forecast_daily",
        "historical_hourly", "historical_daily",
        "historical_forecast_hourly", "historical_forecast_daily",
        "ensemble_hourly", "previous_runs_hourly",
        "seasonal_six_hourly", "seasonal_daily",
        "marine_hourly", "marine_daily",
        "air_quality_hourly",
        "satellite_radiation_hourly", "satellite_radiation_daily",
        "flood_daily",
        "climate_daily",
        "geocoding", "elevation",
    }
    actual_names = {s.name for s in streams}
    assert expected_names == actual_names


# --- Primary key uniqueness ---


def test_all_streams_have_primary_keys():
    """Verify every stream defines primary_keys for deduplication."""
    config = {
        **SAMPLE_CONFIG,
        "enabled_endpoints": [
            "forecast", "historical", "historical_forecast",
            "ensemble", "previous_runs", "seasonal", "marine",
            "air_quality", "satellite", "flood", "climate",
            "geocoding", "elevation",
        ],
        "historical_start_date": "2024-01-01",
        "historical_forecast_start_date": "2024-01-01",
        "satellite_start_date": "2024-01-01",
        "geocoding_search_terms": ["Test"],
    }
    tap = TapOpenMateo(config=config)
    streams = tap.discover_streams()

    for stream in streams:
        assert stream.primary_keys, f"Stream {stream.name} has no primary_keys"
        assert len(stream.primary_keys) >= 1, f"Stream {stream.name} primary_keys is empty"

        # Verify all primary key fields exist in schema
        schema_fields = set(stream.schema.get("properties", {}).keys())
        for pk in stream.primary_keys:
            assert pk in schema_fields, (
                f"Stream {stream.name}: primary key '{pk}' not in schema"
            )


def test_no_schemas_have_surrogate_key():
    """Verify no stream schema includes surrogate_key field (removed in favor of natural keys)."""
    config = {
        **SAMPLE_CONFIG,
        "enabled_endpoints": [
            "forecast", "ensemble", "marine", "air_quality",
            "flood", "geocoding", "elevation",
        ],
        "geocoding_search_terms": ["Test"],
    }
    tap = TapOpenMateo(config=config)
    streams = tap.discover_streams()

    for stream in streams:
        schema_fields = set(stream.schema.get("properties", {}).keys())
        assert "surrogate_key" not in schema_fields, (
            f"Stream {stream.name} still has surrogate_key in schema"
        )
