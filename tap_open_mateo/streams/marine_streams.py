"""Marine Forecast stream definitions for Open-Meteo.

Provides ocean and wave data including wave height, direction, period,
ocean currents, and sea surface temperature.
"""

from __future__ import annotations

import typing as t

from singer_sdk import typing as th

from tap_open_mateo.client import OpenMateoStream

if t.TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers.types import Context

MARINE_HOURLY_PROPERTIES = th.PropertiesList(
    th.Property("location_name", th.StringType, required=True),
    th.Property("latitude", th.NumberType, required=True),
    th.Property("longitude", th.NumberType, required=True),
    th.Property("elevation", th.NumberType),
    th.Property("model", th.StringType),
    th.Property("time", th.DateTimeType, required=True),
    th.Property("granularity", th.StringType, required=True),
    th.Property("wave_height", th.NumberType),
    th.Property("wave_direction", th.NumberType),
    th.Property("wave_period", th.NumberType),
    th.Property("wave_peak_period", th.NumberType),
    th.Property("wind_wave_height", th.NumberType),
    th.Property("wind_wave_direction", th.NumberType),
    th.Property("wind_wave_period", th.NumberType),
    th.Property("wind_wave_peak_period", th.NumberType),
    th.Property("swell_wave_height", th.NumberType),
    th.Property("swell_wave_direction", th.NumberType),
    th.Property("swell_wave_period", th.NumberType),
    th.Property("swell_wave_peak_period", th.NumberType),
    th.Property("secondary_swell_wave_height", th.NumberType),
    th.Property("secondary_swell_wave_direction", th.NumberType),
    th.Property("secondary_swell_wave_period", th.NumberType),
    th.Property("tertiary_swell_wave_height", th.NumberType),
    th.Property("tertiary_swell_wave_direction", th.NumberType),
    th.Property("tertiary_swell_wave_period", th.NumberType),
    th.Property("ocean_current_velocity", th.NumberType),
    th.Property("ocean_current_direction", th.NumberType),
    th.Property("sea_surface_temperature", th.NumberType),
    th.Property("sea_level_height_msl", th.NumberType),
    th.Property("invert_barometer_height", th.NumberType),
    th.Property("surrogate_key", th.StringType),
)

MARINE_DAILY_PROPERTIES = th.PropertiesList(
    th.Property("location_name", th.StringType, required=True),
    th.Property("latitude", th.NumberType, required=True),
    th.Property("longitude", th.NumberType, required=True),
    th.Property("elevation", th.NumberType),
    th.Property("model", th.StringType),
    th.Property("time", th.DateTimeType, required=True),
    th.Property("granularity", th.StringType, required=True),
    th.Property("wave_height_max", th.NumberType),
    th.Property("wave_direction_dominant", th.NumberType),
    th.Property("wave_period_max", th.NumberType),
    th.Property("wind_wave_height_max", th.NumberType),
    th.Property("wind_wave_direction_dominant", th.NumberType),
    th.Property("wind_wave_period_max", th.NumberType),
    th.Property("wind_wave_peak_period_max", th.NumberType),
    th.Property("swell_wave_height_max", th.NumberType),
    th.Property("swell_wave_direction_dominant", th.NumberType),
    th.Property("swell_wave_period_max", th.NumberType),
    th.Property("surrogate_key", th.StringType),
)

DEFAULT_MARINE_HOURLY_VARIABLES = [
    "wave_height",
    "wave_direction",
    "wave_period",
    "wave_peak_period",
    "wind_wave_height",
    "wind_wave_direction",
    "swell_wave_height",
    "swell_wave_direction",
    "ocean_current_velocity",
    "ocean_current_direction",
    "sea_surface_temperature",
]

DEFAULT_MARINE_DAILY_VARIABLES = [
    "wave_height_max",
    "wave_direction_dominant",
    "wave_period_max",
    "wind_wave_height_max",
    "swell_wave_height_max",
]


class MarineHourlyStream(OpenMateoStream):
    """Marine and ocean forecast data at hourly resolution.

    Uses length_unit instead of weather-specific unit parameters.
    Provides wave, swell, ocean current, and sea surface data.
    """

    name = "marine_hourly"
    path = "/v1/marine"
    _free_url_base = "https://marine-api.open-meteo.com"
    _paid_url_base = "https://customer-marine-api.open-meteo.com"
    primary_keys = ("location_name", "model", "time", "granularity")
    replication_key = None
    schema = MARINE_HOURLY_PROPERTIES.to_dict()

    def _build_base_params(self) -> dict:
        """Marine API uses length_unit, not weather unit params."""
        params: dict[str, str] = {
            "length_unit": self.config.get("length_unit", "imperial"),
            "timezone": self.config.get("timezone", "America/Chicago"),
            "timeformat": "iso8601",
        }
        if api_key := self.config.get("api_key"):
            params["apikey"] = api_key
        return params

    @property
    def partitions(self) -> list[dict]:
        return self._location_batch_partitions()

    def get_records(self, context: Context | None) -> Iterable[dict]:
        if context is None:
            return

        locations = context["locations"]
        extra = {
            "hourly": ",".join(
                self.config.get("marine_hourly_variables", DEFAULT_MARINE_HOURLY_VARIABLES)
            ),
            "forecast_days": str(self.config.get("forecast_days", 16)),
            "past_days": str(self.config.get("past_days", 7)),
        }

        for model in self.config.get("models", ["best_match"]):
            yield from self._fetch_and_extract(locations, "hourly", model, extra)


class MarineDailyStream(OpenMateoStream):
    """Marine and ocean forecast data at daily resolution.

    Daily aggregations of wave and ocean measurements.
    """

    name = "marine_daily"
    path = "/v1/marine"
    _free_url_base = "https://marine-api.open-meteo.com"
    _paid_url_base = "https://customer-marine-api.open-meteo.com"
    primary_keys = ("location_name", "model", "time", "granularity")
    replication_key = None
    schema = MARINE_DAILY_PROPERTIES.to_dict()

    def _build_base_params(self) -> dict:
        """Marine API uses length_unit, not weather unit params."""
        params: dict[str, str] = {
            "length_unit": self.config.get("length_unit", "imperial"),
            "timezone": self.config.get("timezone", "America/Chicago"),
            "timeformat": "iso8601",
        }
        if api_key := self.config.get("api_key"):
            params["apikey"] = api_key
        return params

    @property
    def partitions(self) -> list[dict]:
        return self._location_batch_partitions()

    def get_records(self, context: Context | None) -> Iterable[dict]:
        if context is None:
            return

        locations = context["locations"]
        extra = {
            "daily": ",".join(
                self.config.get("marine_daily_variables", DEFAULT_MARINE_DAILY_VARIABLES)
            ),
            "forecast_days": str(self.config.get("forecast_days", 16)),
            "past_days": str(self.config.get("past_days", 7)),
        }

        for model in self.config.get("models", ["best_match"]):
            yield from self._fetch_and_extract(locations, "daily", model, extra)
