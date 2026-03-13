#!/usr/bin/env python3
"""Validate tap-open-mateo schemas against live Open-Meteo API responses.

For each endpoint, requests ALL weather variables defined in the schema,
then compares:
  - Variables in our schema but rejected/missing from API response
  - Variables returned by API but NOT in our schema

Usage:
    uv run python tests/validate_schemas_against_api.py
"""

from __future__ import annotations

import json
import sys
import time

import requests

# Metadata fields that are added by the tap, not returned by the API
METADATA_FIELDS = {
    "location_name", "latitude", "longitude", "elevation",
    "model", "time", "granularity",
    "member", "model_run_offset_days",
}

# Test locations
NYC = {"lat": 40.71, "lon": -74.01}
MIAMI_OCEAN = {"lat": 25.0, "lon": -79.0}  # offshore for marine


def extract_weather_vars(properties_list) -> list[str]:
    """Extract weather variable names from a PropertiesList schema, excluding metadata."""
    schema = properties_list.to_dict() if hasattr(properties_list, "to_dict") else properties_list
    all_props = schema.get("properties", {})
    return sorted(k for k in all_props if k not in METADATA_FIELDS)


def test_endpoint(
    name: str,
    url: str,
    section_key: str,
    variables: list[str],
    extra_params: dict | None = None,
    lat: float = NYC["lat"],
    lon: float = NYC["lon"],
) -> dict:
    """Test an endpoint by requesting all schema variables and analyzing the response."""
    params = {
        "latitude": str(lat),
        "longitude": str(lon),
        "timezone": "America/Chicago",
        "timeformat": "iso8601",
        **(extra_params or {}),
    }

    # Set hourly or daily param
    if section_key in ("hourly", "six_hourly"):
        params["hourly"] = ",".join(variables)
    elif section_key == "daily":
        params["daily"] = ",".join(variables)

    print(f"\n{'='*70}")
    print(f"Testing: {name}")
    print(f"  URL: {url}")
    print(f"  Section: {section_key}")
    print(f"  Variables requested: {len(variables)}")

    try:
        time.sleep(0.5)  # rate limiting
        resp = requests.get(url, params=params, timeout=30)

        if resp.status_code != 200:
            error_data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
            reason = error_data.get("reason", resp.text[:300])
            print(f"  ERROR {resp.status_code}: {reason}")
            return {
                "name": name,
                "status": "error",
                "status_code": resp.status_code,
                "reason": reason,
                "schema_vars": variables,
            }

        data = resp.json()

        # Get the response section (API uses "hourly" even for seasonal 6-hourly)
        api_section_key = "hourly" if section_key == "six_hourly" else section_key
        section = data.get(api_section_key, {})

        if not section:
            print(f"  WARNING: No '{api_section_key}' section in response")
            return {
                "name": name,
                "status": "no_section",
                "schema_vars": variables,
            }

        # API response columns (excluding "time")
        api_columns = sorted(k for k in section if k != "time")

        # For ensemble/seasonal APIs, strip _memberNN suffixes to get base variable names
        base_api_vars = set()
        for col in api_columns:
            # Strip _member01 through _member50 (and _member00 for control)
            import re
            base = re.sub(r"_member\d+$", "", col)
            # Strip _previous_dayN
            base = re.sub(r"_previous_day\d+$", "", base)
            base_api_vars.add(base)

        schema_set = set(variables)

        in_schema_not_api = sorted(schema_set - base_api_vars)
        in_api_not_schema = sorted(base_api_vars - schema_set)

        n_times = len(section.get("time", []))
        print(f"  Response: {len(api_columns)} columns, {n_times} timestamps")
        print(f"  Base API variables (after stripping suffixes): {len(base_api_vars)}")

        if in_schema_not_api:
            print(f"  SCHEMA-ONLY ({len(in_schema_not_api)}): {in_schema_not_api}")
        if in_api_not_schema:
            print(f"  API-ONLY ({len(in_api_not_schema)}): {in_api_not_schema}")
        if not in_schema_not_api and not in_api_not_schema:
            print("  PERFECT MATCH")

        return {
            "name": name,
            "status": "ok",
            "n_schema_vars": len(schema_set),
            "n_api_base_vars": len(base_api_vars),
            "in_schema_not_api": in_schema_not_api,
            "in_api_not_schema": in_api_not_schema,
        }

    except Exception as e:
        print(f"  EXCEPTION: {e}")
        return {"name": name, "status": "exception", "error": str(e)}


def main():
    from tap_open_mateo.streams.air_quality_streams import AIR_QUALITY_HOURLY_PROPERTIES
    from tap_open_mateo.streams.climate_streams import ClimateDailyStream
    from tap_open_mateo.streams.ensemble_streams import EnsembleHourlyStream
    from tap_open_mateo.streams.flood_streams import FLOOD_DAILY_PROPERTIES
    from tap_open_mateo.streams.forecast_streams import (
        FORECAST_DAILY_PROPERTIES,
        FORECAST_HOURLY_PROPERTIES,
    )
    from tap_open_mateo.streams.historical_streams import (
        HISTORICAL_DAILY_PROPERTIES,
        HISTORICAL_HOURLY_PROPERTIES,
    )
    from tap_open_mateo.streams.marine_streams import (
        MARINE_DAILY_PROPERTIES,
        MARINE_HOURLY_PROPERTIES,
    )
    from tap_open_mateo.streams.satellite_streams import (
        SATELLITE_DAILY_PROPERTIES,
        SATELLITE_HOURLY_PROPERTIES,
    )
    from tap_open_mateo.streams.seasonal_streams import (
        SEASONAL_DAILY_PROPERTIES,
        SEASONAL_SIX_HOURLY_PROPERTIES,
    )

    results = []

    # --- Forecast ---
    forecast_hourly_vars = extract_weather_vars(FORECAST_HOURLY_PROPERTIES)
    # Split pressure level vars from regular vars (URL length limit)
    pressure_vars = [v for v in forecast_hourly_vars if "hPa" in v]
    regular_hourly_vars = [v for v in forecast_hourly_vars if "hPa" not in v]

    results.append(test_endpoint(
        "Forecast Hourly (regular vars)",
        "https://api.open-meteo.com/v1/forecast",
        "hourly", regular_hourly_vars,
        {"forecast_days": "1"},
    ))

    if pressure_vars:
        # Test a sample of pressure level vars (first 20)
        sample_pressure = pressure_vars[:20]
        results.append(test_endpoint(
            "Forecast Hourly (pressure level sample)",
            "https://api.open-meteo.com/v1/forecast",
            "hourly", sample_pressure,
            {"forecast_days": "1"},
        ))

    results.append(test_endpoint(
        "Forecast Daily",
        "https://api.open-meteo.com/v1/forecast",
        "daily", extract_weather_vars(FORECAST_DAILY_PROPERTIES),
        {"forecast_days": "1"},
    ))

    # --- Historical (ERA5) ---
    results.append(test_endpoint(
        "Historical Hourly (ERA5)",
        "https://archive-api.open-meteo.com/v1/archive",
        "hourly", extract_weather_vars(HISTORICAL_HOURLY_PROPERTIES),
        {"start_date": "2024-01-01", "end_date": "2024-01-02"},
    ))

    results.append(test_endpoint(
        "Historical Daily (ERA5)",
        "https://archive-api.open-meteo.com/v1/archive",
        "daily", extract_weather_vars(HISTORICAL_DAILY_PROPERTIES),
        {"start_date": "2024-01-01", "end_date": "2024-01-02"},
    ))

    # --- Historical Forecast ---
    results.append(test_endpoint(
        "Historical Forecast Hourly",
        "https://historical-forecast-api.open-meteo.com/v1/forecast",
        "hourly", regular_hourly_vars,
        {"start_date": "2024-01-01", "end_date": "2024-01-02"},
    ))

    results.append(test_endpoint(
        "Historical Forecast Daily",
        "https://historical-forecast-api.open-meteo.com/v1/forecast",
        "daily", extract_weather_vars(FORECAST_DAILY_PROPERTIES),
        {"start_date": "2024-01-01", "end_date": "2024-01-02"},
    ))

    # --- Previous Runs ---
    results.append(test_endpoint(
        "Previous Runs Hourly",
        "https://previous-runs-api.open-meteo.com/v1/forecast",
        "hourly", regular_hourly_vars,
        {"forecast_days": "1", "past_days": "1"},
    ))

    # --- Ensemble ---
    ensemble_schema = EnsembleHourlyStream.schema
    ensemble_vars = sorted(
        k for k in ensemble_schema.get("properties", {}) if k not in METADATA_FIELDS
    )
    results.append(test_endpoint(
        "Ensemble Hourly",
        "https://ensemble-api.open-meteo.com/v1/ensemble",
        "hourly", ensemble_vars,
        {"models": "gfs025", "forecast_days": "1"},
    ))

    # --- Marine ---
    results.append(test_endpoint(
        "Marine Hourly",
        "https://marine-api.open-meteo.com/v1/marine",
        "hourly", extract_weather_vars(MARINE_HOURLY_PROPERTIES),
        {"forecast_days": "1", "length_unit": "imperial"},
        lat=MIAMI_OCEAN["lat"], lon=MIAMI_OCEAN["lon"],
    ))

    results.append(test_endpoint(
        "Marine Daily",
        "https://marine-api.open-meteo.com/v1/marine",
        "daily", extract_weather_vars(MARINE_DAILY_PROPERTIES),
        {"forecast_days": "1", "length_unit": "imperial"},
        lat=MIAMI_OCEAN["lat"], lon=MIAMI_OCEAN["lon"],
    ))

    # --- Air Quality ---
    results.append(test_endpoint(
        "Air Quality Hourly",
        "https://air-quality-api.open-meteo.com/v1/air-quality",
        "hourly", extract_weather_vars(AIR_QUALITY_HOURLY_PROPERTIES),
        {"forecast_days": "1"},
    ))

    # --- Satellite ---
    results.append(test_endpoint(
        "Satellite Hourly",
        "https://satellite-api.open-meteo.com/v1/archive",
        "hourly", extract_weather_vars(SATELLITE_HOURLY_PROPERTIES),
        {"start_date": "2024-06-01", "end_date": "2024-06-02"},
    ))

    results.append(test_endpoint(
        "Satellite Daily",
        "https://satellite-api.open-meteo.com/v1/archive",
        "daily", extract_weather_vars(SATELLITE_DAILY_PROPERTIES),
        {"start_date": "2024-06-01", "end_date": "2024-06-02"},
    ))

    # --- Flood ---
    flood_vars = extract_weather_vars(FLOOD_DAILY_PROPERTIES)
    # Test stats mode (non-ensemble)
    flood_stats_vars = [v for v in flood_vars if v != "river_discharge"]
    results.append(test_endpoint(
        "Flood Daily (stats)",
        "https://flood-api.open-meteo.com/v1/flood",
        "daily", flood_stats_vars,
        {"forecast_days": "5"},
    ))

    # --- Climate ---
    climate_schema = ClimateDailyStream.schema
    climate_vars = sorted(
        k for k in climate_schema.get("properties", {}) if k not in METADATA_FIELDS
    )
    results.append(test_endpoint(
        "Climate Daily",
        "https://climate-api.open-meteo.com/v1/climate",
        "daily", climate_vars,
        {"models": "EC_Earth3P_HR", "start_date": "2024-01-01", "end_date": "2024-12-31"},
    ))

    # --- Seasonal ---
    results.append(test_endpoint(
        "Seasonal Six-Hourly",
        "https://seasonal-api.open-meteo.com/v1/seasonal",
        "six_hourly", extract_weather_vars(SEASONAL_SIX_HOURLY_PROPERTIES),
        {"models": "ecmwf_seasonal_seamless"},
    ))

    results.append(test_endpoint(
        "Seasonal Daily",
        "https://seasonal-api.open-meteo.com/v1/seasonal",
        "daily", extract_weather_vars(SEASONAL_DAILY_PROPERTIES),
        {"models": "ecmwf_seasonal_seamless"},
    ))

    # --- Summary ---
    print(f"\n{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}")

    errors = []
    mismatches = []
    perfect = []

    for r in results:
        if r["status"] in ("error", "exception", "no_section"):
            errors.append(r)
        elif r.get("in_schema_not_api") or r.get("in_api_not_schema"):
            mismatches.append(r)
        else:
            perfect.append(r)

    if perfect:
        print(f"\nPERFECT MATCHES ({len(perfect)}):")
        for r in perfect:
            print(f"  {r['name']}")

    if mismatches:
        print(f"\nMISMATCHES ({len(mismatches)}):")
        for r in mismatches:
            print(f"\n  {r['name']}:")
            if r.get("in_schema_not_api"):
                print(f"    Schema has but API doesn't: {r['in_schema_not_api']}")
            if r.get("in_api_not_schema"):
                print(f"    API has but schema doesn't: {r['in_api_not_schema']}")

    if errors:
        print(f"\nERRORS ({len(errors)}):")
        for r in errors:
            reason = r.get("reason", r.get("error", "unknown"))
            print(f"  {r['name']}: {r['status']} - {reason[:200]}")

    # Write detailed results to file
    output_path = "tests/schema_validation_results.json"
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nDetailed results written to {output_path}")

    return 1 if (errors or mismatches) else 0


if __name__ == "__main__":
    sys.exit(main())
