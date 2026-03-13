#!/usr/bin/env python3
"""Find specific invalid variables for endpoints that failed bulk validation.

Tests each variable individually against the API to identify which ones
are rejected vs accepted.
"""

from __future__ import annotations

import sys
import time

import requests

NYC = {"lat": 40.71, "lon": -74.01}
MIAMI_OCEAN = {"lat": 25.0, "lon": -79.0}

METADATA_FIELDS = {
    "location_name", "latitude", "longitude", "elevation",
    "model", "time", "granularity",
    "member", "model_run_offset_days",
}


def extract_weather_vars(properties_list) -> list[str]:
    schema = properties_list.to_dict() if hasattr(properties_list, "to_dict") else properties_list
    return sorted(k for k in schema.get("properties", {}) if k not in METADATA_FIELDS)


def test_var(url: str, section: str, var: str, extra_params: dict, lat: float, lon: float) -> bool:
    """Test if a single variable is accepted by the API."""
    params = {
        "latitude": str(lat),
        "longitude": str(lon),
        "timezone": "America/Chicago",
        "timeformat": "iso8601",
        **extra_params,
    }
    api_section = "hourly" if section == "six_hourly" else section
    params[api_section] = var

    time.sleep(0.3)
    try:
        resp = requests.get(url, params=params, timeout=15)
        return resp.status_code == 200
    except Exception:
        return False


def find_invalid(name: str, url: str, section: str, variables: list[str],
                 extra_params: dict, lat: float = NYC["lat"], lon: float = NYC["lon"]):
    """Find which variables are invalid for an endpoint."""
    print(f"\n{'='*60}")
    print(f"Testing {name}: {len(variables)} variables individually")

    accepted = []
    rejected = []

    for i, var in enumerate(variables):
        ok = test_var(url, section, var, extra_params, lat, lon)
        status = "OK" if ok else "REJECTED"
        if not ok:
            rejected.append(var)
            print(f"  [{i+1}/{len(variables)}] {var}: {status}")
        else:
            accepted.append(var)

    print(f"\n  Accepted: {len(accepted)}/{len(variables)}")
    if rejected:
        print(f"  REJECTED ({len(rejected)}): {rejected}")
    return accepted, rejected


def main():
    from tap_open_mateo.streams.air_quality_streams import AIR_QUALITY_HOURLY_PROPERTIES
    from tap_open_mateo.streams.ensemble_streams import EnsembleHourlyStream
    from tap_open_mateo.streams.historical_streams import HISTORICAL_HOURLY_PROPERTIES
    from tap_open_mateo.streams.marine_streams import MARINE_HOURLY_PROPERTIES
    from tap_open_mateo.streams.seasonal_streams import SEASONAL_DAILY_PROPERTIES

    all_rejected = {}

    # 1. Historical Hourly
    _, rejected = find_invalid(
        "Historical Hourly (ERA5)",
        "https://archive-api.open-meteo.com/v1/archive",
        "hourly",
        extract_weather_vars(HISTORICAL_HOURLY_PROPERTIES),
        {"start_date": "2024-01-01", "end_date": "2024-01-02"},
    )
    all_rejected["historical_hourly"] = rejected

    # 2. Ensemble Hourly
    ensemble_vars = sorted(
        k for k in EnsembleHourlyStream.schema.get("properties", {})
        if k not in METADATA_FIELDS
    )
    _, rejected = find_invalid(
        "Ensemble Hourly",
        "https://ensemble-api.open-meteo.com/v1/ensemble",
        "hourly",
        ensemble_vars,
        {"models": "gfs025", "forecast_days": "1"},
    )
    all_rejected["ensemble_hourly"] = rejected

    # 3. Marine Hourly
    _, rejected = find_invalid(
        "Marine Hourly",
        "https://marine-api.open-meteo.com/v1/marine",
        "hourly",
        extract_weather_vars(MARINE_HOURLY_PROPERTIES),
        {"forecast_days": "1", "length_unit": "imperial"},
        lat=MIAMI_OCEAN["lat"], lon=MIAMI_OCEAN["lon"],
    )
    all_rejected["marine_hourly"] = rejected

    # 4. Air Quality Hourly
    _, rejected = find_invalid(
        "Air Quality Hourly",
        "https://air-quality-api.open-meteo.com/v1/air-quality",
        "hourly",
        extract_weather_vars(AIR_QUALITY_HOURLY_PROPERTIES),
        {"forecast_days": "1"},
    )
    all_rejected["air_quality_hourly"] = rejected

    # 5. Seasonal Daily
    _, rejected = find_invalid(
        "Seasonal Daily",
        "https://seasonal-api.open-meteo.com/v1/seasonal",
        "daily",
        extract_weather_vars(SEASONAL_DAILY_PROPERTIES),
        {"models": "ecmwf_seasonal_seamless"},
    )
    all_rejected["seasonal_daily"] = rejected

    # Summary
    print(f"\n{'='*60}")
    print("FINAL SUMMARY OF INVALID VARIABLES")
    print(f"{'='*60}")
    for endpoint, vars in all_rejected.items():
        if vars:
            print(f"\n{endpoint}:")
            for v in vars:
                print(f"  - {v}")
        else:
            print(f"\n{endpoint}: All variables valid (bulk error was transient)")

    return 1 if any(all_rejected.values()) else 0


if __name__ == "__main__":
    sys.exit(main())
