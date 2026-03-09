"""Open-Meteo tap class."""

from __future__ import annotations

import sys
import threading
import typing as t
from collections import deque

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_open_mateo.streams import (
    AirQualityHourlyStream,
    ClimateDailyStream,
    ElevationStream,
    EnsembleHourlyStream,
    FloodDailyStream,
    ForecastDailyStream,
    ForecastHourlyStream,
    GeocodingStream,
    HistoricalDailyStream,
    HistoricalForecastDailyStream,
    HistoricalForecastHourlyStream,
    HistoricalHourlyStream,
    MarineDailyStream,
    MarineHourlyStream,
    PreviousRunsHourlyStream,
    SatelliteRadiationDailyStream,
    SatelliteRadiationHourlyStream,
    SeasonalDailyStream,
    SeasonalSixHourlyStream,
)

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

# --- Location Presets ---

US_POPULATION_CENTERS: list[dict] = [
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
    {"name": "Austin", "latitude": 30.27, "longitude": -97.74},
    {"name": "Fort Worth", "latitude": 32.75, "longitude": -97.33},
    {"name": "San Jose", "latitude": 37.34, "longitude": -121.89},
    {"name": "Columbus", "latitude": 39.96, "longitude": -82.99},
    {"name": "Charlotte", "latitude": 35.23, "longitude": -80.84},
    {"name": "Indianapolis", "latitude": 39.77, "longitude": -86.16},
    {"name": "San Francisco", "latitude": 37.78, "longitude": -122.42},
    {"name": "Seattle", "latitude": 47.61, "longitude": -122.33},
    {"name": "Denver", "latitude": 39.74, "longitude": -104.98},
    {"name": "Nashville", "latitude": 36.16, "longitude": -86.78},
    {"name": "Washington DC", "latitude": 38.91, "longitude": -77.04},
    {"name": "Oklahoma City", "latitude": 35.47, "longitude": -97.52},
    {"name": "El Paso", "latitude": 31.76, "longitude": -106.44},
    {"name": "Boston", "latitude": 42.36, "longitude": -71.06},
    {"name": "Portland", "latitude": 45.52, "longitude": -122.68},
    {"name": "Las Vegas", "latitude": 36.17, "longitude": -115.14},
    {"name": "Memphis", "latitude": 35.15, "longitude": -90.05},
    {"name": "Louisville", "latitude": 38.25, "longitude": -85.76},
    {"name": "Baltimore", "latitude": 39.29, "longitude": -76.61},
    {"name": "Milwaukee", "latitude": 43.04, "longitude": -87.91},
    {"name": "Albuquerque", "latitude": 35.08, "longitude": -106.65},
    {"name": "Tucson", "latitude": 32.22, "longitude": -110.97},
    {"name": "Fresno", "latitude": 36.74, "longitude": -119.77},
    {"name": "Sacramento", "latitude": 38.58, "longitude": -121.49},
    {"name": "Mesa", "latitude": 33.42, "longitude": -111.83},
    {"name": "Kansas City", "latitude": 39.10, "longitude": -94.58},
    {"name": "Atlanta", "latitude": 33.75, "longitude": -84.39},
    {"name": "Omaha", "latitude": 41.26, "longitude": -95.94},
    {"name": "Colorado Springs", "latitude": 38.83, "longitude": -104.82},
    {"name": "Raleigh", "latitude": 35.78, "longitude": -78.64},
    {"name": "Long Beach", "latitude": 33.77, "longitude": -118.19},
    {"name": "Virginia Beach", "latitude": 36.85, "longitude": -75.98},
    {"name": "Miami", "latitude": 25.76, "longitude": -80.19},
    {"name": "Oakland", "latitude": 37.80, "longitude": -122.27},
    {"name": "Minneapolis", "latitude": 44.98, "longitude": -93.27},
    {"name": "Tampa", "latitude": 27.95, "longitude": -82.46},
    {"name": "Tulsa", "latitude": 36.15, "longitude": -95.99},
    {"name": "Arlington TX", "latitude": 32.74, "longitude": -97.11},
    {"name": "New Orleans", "latitude": 29.95, "longitude": -90.07},
    {"name": "Wichita", "latitude": 37.69, "longitude": -97.34},
]

ENERGY_HUBS: list[dict] = [
    {"name": "Henry Hub", "latitude": 30.05, "longitude": -91.14},
    {"name": "Cushing OK", "latitude": 35.98, "longitude": -96.77},
    {"name": "Houston Ship Channel", "latitude": 29.73, "longitude": -95.25},
    {"name": "Midland TX", "latitude": 31.99, "longitude": -102.08},
    {"name": "Appalachia", "latitude": 39.63, "longitude": -79.96},
    {"name": "Chicago Citygate", "latitude": 41.85, "longitude": -87.65},
    {"name": "SoCal Citygate", "latitude": 34.00, "longitude": -118.17},
    {"name": "PG&E Citygate", "latitude": 37.77, "longitude": -122.42},
    {"name": "Dominion South", "latitude": 38.35, "longitude": -81.63},
    {"name": "TETCO M3", "latitude": 40.44, "longitude": -79.95},
]

LOCATION_PRESETS: dict[str, list[dict]] = {
    "us_population_centers": US_POPULATION_CENTERS,
    "energy_hubs": ENERGY_HUBS,
}


class TapOpenMateo(Tap):
    """Singer tap for Open-Meteo weather APIs."""

    name = "tap-open-mateo"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            secret=True,
            description=(
                "Open-Meteo API key for paid tier. "
                "If omitted, only Forecast API is available (free tier)."
            ),
        ),
        th.Property(
            "locations",
            th.ArrayType(
                th.ObjectType(
                    th.Property("name", th.StringType, required=True),
                    th.Property("latitude", th.NumberType, required=True),
                    th.Property("longitude", th.NumberType, required=True),
                )
            ),
            description=(
                "List of locations to query. Each object requires name, latitude, longitude."
            ),
        ),
        th.Property(
            "location_presets",
            th.StringType,
            description=(
                "Shortcut for locations: 'us_population_centers' or 'energy_hubs'. "
                "If set, overrides 'locations'."
            ),
        ),
        th.Property(
            "hourly_variables",
            th.ArrayType(th.StringType),
            default=[
                "temperature_2m",
                "relative_humidity_2m",
                "dew_point_2m",
                "apparent_temperature",
                "wind_speed_10m",
                "wind_direction_10m",
                "wind_gusts_10m",
                "precipitation",
                "rain",
                "snowfall",
                "snow_depth",
                "pressure_msl",
                "cloud_cover",
                "soil_temperature_0cm",
                "soil_moisture_0_to_1cm",
            ],
            description="Hourly variables to request from the API.",
        ),
        th.Property(
            "daily_variables",
            th.ArrayType(th.StringType),
            default=[
                "temperature_2m_max",
                "temperature_2m_min",
                "temperature_2m_mean",
                "precipitation_sum",
                "wind_speed_10m_max",
                "wind_gusts_10m_max",
            ],
            description="Daily variables to request from the API.",
        ),
        th.Property(
            "models",
            th.ArrayType(th.StringType),
            default=["best_match"],
            description="Weather models to query for forecast/historical.",
        ),
        th.Property(
            "ensemble_models",
            th.ArrayType(th.StringType),
            default=["gfs025"],
            description="Ensemble models to query.",
        ),
        th.Property(
            "climate_models",
            th.ArrayType(th.StringType),
            default=["EC_Earth3P_HR", "MRI_AGCM3_2_S"],
            description="CMIP6 climate models to query.",
        ),
        th.Property(
            "temperature_unit",
            th.StringType,
            default="fahrenheit",
            description="Temperature unit: 'celsius' or 'fahrenheit'.",
        ),
        th.Property(
            "wind_speed_unit",
            th.StringType,
            default="mph",
            description="Wind speed unit: 'kmh', 'ms', 'mph', 'kn'.",
        ),
        th.Property(
            "precipitation_unit",
            th.StringType,
            default="inch",
            description="Precipitation unit: 'mm' or 'inch'.",
        ),
        th.Property(
            "timezone",
            th.StringType,
            default="America/Chicago",
            description="IANA timezone for daily aggregation.",
        ),
        th.Property(
            "forecast_days",
            th.IntegerType,
            default=16,
            description="Forecast horizon in days (max 16).",
        ),
        th.Property(
            "past_days",
            th.IntegerType,
            default=7,
            description="How many past days to include in forecast requests.",
        ),
        th.Property(
            "previous_run_days",
            th.ArrayType(th.IntegerType),
            default=[1, 2, 3, 5, 7],
            description="Which _previous_dayN suffixes to request.",
        ),
        th.Property(
            "historical_start_date",
            th.DateType,
            description="Start date for Historical API (e.g. '2020-01-01').",
        ),
        th.Property(
            "historical_end_date",
            th.DateType,
            description="End date for Historical API. Defaults to yesterday.",
        ),
        th.Property(
            "historical_chunk_days",
            th.IntegerType,
            default=365,
            description="Days per Historical API request chunk.",
        ),
        th.Property(
            "climate_start_date",
            th.DateType,
            default="1950-01-01",
            description="Climate API start date.",
        ),
        th.Property(
            "climate_end_date",
            th.DateType,
            default="2050-01-01",
            description="Climate API end date (data available up to 2050-01-01).",
        ),
        # --- Historical Forecast ---
        th.Property(
            "historical_forecast_start_date",
            th.DateType,
            description="Start date for Historical Forecast API (data from 2022+).",
        ),
        th.Property(
            "historical_forecast_end_date",
            th.DateType,
            description="End date for Historical Forecast API. Defaults to yesterday.",
        ),
        # --- Seasonal Forecast ---
        th.Property(
            "seasonal_models",
            th.ArrayType(th.StringType),
            default=["ecmwf_seasonal_seamless"],
            description="Seasonal forecast models to query.",
        ),
        th.Property(
            "seasonal_six_hourly_variables",
            th.ArrayType(th.StringType),
            description="6-hourly variables for seasonal forecasts.",
        ),
        th.Property(
            "seasonal_daily_variables",
            th.ArrayType(th.StringType),
            description="Daily variables for seasonal forecasts.",
        ),
        # --- Marine ---
        th.Property(
            "marine_hourly_variables",
            th.ArrayType(th.StringType),
            description="Hourly variables for marine forecasts.",
        ),
        th.Property(
            "marine_daily_variables",
            th.ArrayType(th.StringType),
            description="Daily variables for marine forecasts.",
        ),
        th.Property(
            "length_unit",
            th.StringType,
            default="imperial",
            description="Length unit for marine API: 'metric' or 'imperial'.",
        ),
        # --- Air Quality ---
        th.Property(
            "air_quality_variables",
            th.ArrayType(th.StringType),
            description="Hourly variables for air quality forecasts.",
        ),
        th.Property(
            "air_quality_domains",
            th.StringType,
            default="auto",
            description="Air quality model domain: 'auto', 'cams_europe', 'cams_global'.",
        ),
        # --- Satellite Radiation ---
        th.Property(
            "satellite_start_date",
            th.DateType,
            description="Start date for satellite radiation archive.",
        ),
        th.Property(
            "satellite_end_date",
            th.DateType,
            description="End date for satellite radiation archive. Defaults to yesterday.",
        ),
        th.Property(
            "satellite_hourly_variables",
            th.ArrayType(th.StringType),
            description="Hourly variables for satellite radiation.",
        ),
        th.Property(
            "satellite_daily_variables",
            th.ArrayType(th.StringType),
            description="Daily variables for satellite radiation.",
        ),
        th.Property(
            "solar_panel_tilt",
            th.NumberType,
            description="Panel tilt angle (0-90 degrees) for GTI calculations.",
        ),
        th.Property(
            "solar_panel_azimuth",
            th.NumberType,
            description="Panel azimuth angle (-90 to 90 degrees) for GTI calculations.",
        ),
        # --- Flood ---
        th.Property(
            "flood_variables",
            th.ArrayType(th.StringType),
            description="Daily variables for flood forecasts.",
        ),
        th.Property(
            "flood_ensemble",
            th.BooleanType,
            default=False,
            description="If true, request all 50 ensemble members for flood data.",
        ),
        th.Property(
            "flood_forecast_days",
            th.IntegerType,
            default=92,
            description="Flood forecast horizon in days (max 210).",
        ),
        # --- Geocoding ---
        th.Property(
            "geocoding_search_terms",
            th.ArrayType(th.StringType),
            description="Location names to search via Geocoding API.",
        ),
        th.Property(
            "geocoding_count",
            th.IntegerType,
            default=10,
            description="Max results per geocoding search (1-100).",
        ),
        th.Property(
            "geocoding_language",
            th.StringType,
            default="en",
            description="Language for geocoding results.",
        ),
        # --- Endpoint Selection ---
        th.Property(
            "enabled_endpoints",
            th.ArrayType(th.StringType),
            default=["forecast", "historical", "ensemble", "previous_runs"],
            description=(
                "Which endpoints to extract. Options: forecast, historical, "
                "historical_forecast, ensemble, previous_runs, seasonal, marine, "
                "air_quality, satellite, flood, climate, geocoding, elevation."
            ),
        ),
        th.Property(
            "max_requests_per_minute",
            th.IntegerType,
            default=500,
            description="Maximum API requests per minute.",
        ),
        th.Property(
            "min_throttle_seconds",
            th.NumberType,
            default=0.15,
            description="Minimum seconds between consecutive requests.",
        ),
        th.Property(
            "max_locations_per_request",
            th.IntegerType,
            default=10,
            description=(
                "Batch locations into multi-location requests. "
                "Open-Meteo supports comma-separated lat/lon."
            ),
        ),
        th.Property(
            "strict_mode",
            th.BooleanType,
            default=False,
            description="If True, raise on API errors instead of skipping partitions.",
        ),
    ).to_dict()

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        # Initialize shared rate limiter BEFORE super().__init__() so streams can use it
        self._shared_throttle_lock = threading.Lock()
        self._shared_request_timestamps: deque[float] = deque()
        self._locations_lock = threading.Lock()
        self._resolved_locations: list[dict] | None = None
        super().__init__(*args, **kwargs)

    def get_resolved_locations(self) -> list[dict]:
        """Resolve and cache locations from config.

        Checks location_presets first, then falls back to locations config.
        Thread-safe with lock.
        """
        with self._locations_lock:
            if self._resolved_locations is not None:
                return self._resolved_locations

            preset = self.config.get("location_presets")
            if preset:
                locations = LOCATION_PRESETS.get(preset)
                if locations is None:
                    msg = (
                        f"Unknown location_presets value: '{preset}'. "
                        f"Valid options: {list(LOCATION_PRESETS.keys())}"
                    )
                    raise ValueError(msg)
                self._resolved_locations = locations
                self.logger.info(
                    "Using location preset '%s' with %d locations",
                    preset,
                    len(locations),
                )
                return self._resolved_locations

            locations = self.config.get("locations")
            if not locations:
                msg = (
                    "Either 'locations' or 'location_presets' must be configured. "
                    "Set location_presets to 'us_population_centers' or 'energy_hubs', "
                    "or provide a list of {name, latitude, longitude} objects."
                )
                raise ValueError(msg)

            self._resolved_locations = locations
            self.logger.info("Using %d configured locations", len(locations))
            return self._resolved_locations

    @override
    def discover_streams(self) -> list:
        """Return a list of discovered streams based on enabled_endpoints config."""
        enabled = set(self.config.get("enabled_endpoints", []))
        streams: list = []

        if "forecast" in enabled:
            streams.append(ForecastHourlyStream(self))
            streams.append(ForecastDailyStream(self))

        if "historical" in enabled and self.config.get("historical_start_date"):
            streams.append(HistoricalHourlyStream(self))
            streams.append(HistoricalDailyStream(self))

        if "ensemble" in enabled:
            streams.append(EnsembleHourlyStream(self))

        if "previous_runs" in enabled:
            streams.append(PreviousRunsHourlyStream(self))

        if "climate" in enabled:
            streams.append(ClimateDailyStream(self))

        if "historical_forecast" in enabled and self.config.get("historical_forecast_start_date"):
            streams.append(HistoricalForecastHourlyStream(self))
            streams.append(HistoricalForecastDailyStream(self))

        if "seasonal" in enabled:
            streams.append(SeasonalSixHourlyStream(self))
            streams.append(SeasonalDailyStream(self))

        if "marine" in enabled:
            streams.append(MarineHourlyStream(self))
            streams.append(MarineDailyStream(self))

        if "air_quality" in enabled:
            streams.append(AirQualityHourlyStream(self))

        if "satellite" in enabled and self.config.get("satellite_start_date"):
            streams.append(SatelliteRadiationHourlyStream(self))
            streams.append(SatelliteRadiationDailyStream(self))

        if "flood" in enabled:
            streams.append(FloodDailyStream(self))

        if "geocoding" in enabled and self.config.get("geocoding_search_terms"):
            streams.append(GeocodingStream(self))

        if "elevation" in enabled:
            streams.append(ElevationStream(self))

        return streams


if __name__ == "__main__":
    TapOpenMateo.cli()
