"""Forecast and Previous Runs stream definitions for Open-Meteo."""

from __future__ import annotations

import typing as t

from singer_sdk import typing as th

from tap_open_mateo.client import OpenMateoStream
from tap_open_mateo.helpers import pivot_previous_runs_to_rows

if t.TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers.types import Context

# --- Shared schema properties ---

FORECAST_HOURLY_PROPERTIES = th.PropertiesList(
    th.Property("location_name", th.StringType, required=True),
    th.Property("latitude", th.NumberType, required=True),
    th.Property("longitude", th.NumberType, required=True),
    th.Property("elevation", th.NumberType),
    th.Property("model", th.StringType),
    th.Property("time", th.DateTimeType, required=True),
    th.Property("granularity", th.StringType, required=True),
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
    th.Property("surrogate_key", th.StringType),
)

FORECAST_DAILY_PROPERTIES = th.PropertiesList(
    th.Property("location_name", th.StringType, required=True),
    th.Property("latitude", th.NumberType, required=True),
    th.Property("longitude", th.NumberType, required=True),
    th.Property("elevation", th.NumberType),
    th.Property("model", th.StringType),
    th.Property("time", th.DateTimeType, required=True),
    th.Property("granularity", th.StringType, required=True),
    th.Property("temperature_2m_max", th.NumberType),
    th.Property("temperature_2m_min", th.NumberType),
    th.Property("temperature_2m_mean", th.NumberType),
    th.Property("apparent_temperature_max", th.NumberType),
    th.Property("apparent_temperature_min", th.NumberType),
    th.Property("apparent_temperature_mean", th.NumberType),
    th.Property("precipitation_sum", th.NumberType),
    th.Property("rain_sum", th.NumberType),
    th.Property("showers_sum", th.NumberType),
    th.Property("snowfall_sum", th.NumberType),
    th.Property("precipitation_hours", th.NumberType),
    th.Property("precipitation_probability_max", th.NumberType),
    th.Property("precipitation_probability_mean", th.NumberType),
    th.Property("precipitation_probability_min", th.NumberType),
    th.Property("weather_code", th.IntegerType),
    th.Property("sunrise", th.StringType),
    th.Property("sunset", th.StringType),
    th.Property("daylight_duration", th.NumberType),
    th.Property("sunshine_duration", th.NumberType),
    th.Property("wind_speed_10m_max", th.NumberType),
    th.Property("wind_gusts_10m_max", th.NumberType),
    th.Property("wind_direction_10m_dominant", th.NumberType),
    th.Property("shortwave_radiation_sum", th.NumberType),
    th.Property("et0_fao_evapotranspiration", th.NumberType),
    th.Property("uv_index_max", th.NumberType),
    th.Property("uv_index_clear_sky_max", th.NumberType),
    th.Property("surrogate_key", th.StringType),
)


class ForecastHourlyStream(OpenMateoStream):
    """Current weather forecast at hourly resolution.

    Queries the /v1/forecast endpoint for each location batch and model,
    pivoting the columnar response into one record per (location, timestamp).
    """

    name = "forecast_hourly"
    path = "/v1/forecast"
    primary_keys = ("location_name", "model", "time", "granularity")
    replication_key = None
    schema = FORECAST_HOURLY_PROPERTIES.to_dict()

    @property
    def partitions(self) -> list[dict]:
        return self._location_batch_partitions()

    def get_records(self, context: Context | None) -> Iterable[dict]:
        if context is None:
            return

        locations = context["locations"]
        extra = {
            "hourly": ",".join(self.config.get("hourly_variables", [])),
            "forecast_days": str(self.config.get("forecast_days", 16)),
            "past_days": str(self.config.get("past_days", 7)),
        }

        for model in self.config.get("models", ["best_match"]):
            yield from self._fetch_and_extract(locations, "hourly", model, extra)


class ForecastDailyStream(OpenMateoStream):
    """Current weather forecast at daily resolution.

    Same endpoint as hourly but queries daily-aggregated variables
    (max/min/mean temperatures, precipitation sums, etc.).
    """

    name = "forecast_daily"
    path = "/v1/forecast"
    primary_keys = ("location_name", "model", "time", "granularity")
    replication_key = None
    schema = FORECAST_DAILY_PROPERTIES.to_dict()

    @property
    def partitions(self) -> list[dict]:
        return self._location_batch_partitions()

    def get_records(self, context: Context | None) -> Iterable[dict]:
        if context is None:
            return

        locations = context["locations"]
        extra = {
            "daily": ",".join(self.config.get("daily_variables", [])),
            "forecast_days": str(self.config.get("forecast_days", 16)),
            "past_days": str(self.config.get("past_days", 7)),
        }

        for model in self.config.get("models", ["best_match"]):
            yield from self._fetch_and_extract(locations, "daily", model, extra)


class PreviousRunsHourlyStream(OpenMateoStream):
    """Point-in-time forecast archives from previous model runs.

    Queries the Previous Runs API and normalizes _previous_dayN columns
    into separate rows with a model_run_offset_days field.
    Essential for computing forecast revision signals.
    """

    name = "previous_runs_hourly"
    path = "/v1/forecast"
    _free_url_base = "https://previous-runs-api.open-meteo.com"
    _paid_url_base = "https://customer-previous-runs-api.open-meteo.com"
    primary_keys = ("location_name", "model", "model_run_offset_days", "time", "granularity")
    replication_key = None

    schema = th.PropertiesList(
        th.Property("location_name", th.StringType, required=True),
        th.Property("latitude", th.NumberType, required=True),
        th.Property("longitude", th.NumberType, required=True),
        th.Property("elevation", th.NumberType),
        th.Property("model", th.StringType),
        th.Property("model_run_offset_days", th.IntegerType, required=True),
        th.Property("time", th.DateTimeType, required=True),
        th.Property("granularity", th.StringType, required=True),
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

    @staticmethod
    def _build_previous_runs_hourly_param(base_vars: list[str], previous_days: list[int]) -> str:
        """Build hourly param string with base vars + all _previous_dayN suffixes."""
        suffixed = [f"{var}_previous_day{day}" for var in base_vars for day in previous_days]
        return ",".join([*base_vars, *suffixed])

    @property
    def partitions(self) -> list[dict]:
        return self._location_batch_partitions()

    def get_records(self, context: Context | None) -> Iterable[dict]:
        if context is None:
            return

        locations = context["locations"]
        hourly_vars = self.config.get("hourly_variables", [])
        previous_days = self.config.get("previous_run_days", [1, 2, 3, 5, 7])
        hourly_param = self._build_previous_runs_hourly_param(hourly_vars, previous_days)

        for model in self.config.get("models", ["best_match"]):
            params = {
                **self._build_base_params(),
                **self._build_location_params(locations),
                "hourly": hourly_param,
                "forecast_days": str(self.config.get("forecast_days", 16)),
                "past_days": str(self.config.get("past_days", 7)),
            }
            if model != "best_match":
                params["models"] = model

            data = self._make_request(f"{self.url_base}{self.path}", params)
            if not data:
                continue

            data_items = data if isinstance(data, list) else [data]
            location_list = locations if isinstance(data, list) else [locations[0]]

            for loc_data, loc in zip(data_items, location_list):
                section = self._validate_response_section(loc_data, "hourly")
                if not section:
                    continue

                yield from pivot_previous_runs_to_rows(
                    section_data=section,
                    times=section.get("time", []),
                    base_variables=hourly_vars,
                    previous_run_days=previous_days,
                    location_name=loc["name"],
                    latitude=loc_data.get("latitude", loc["latitude"]),
                    longitude=loc_data.get("longitude", loc["longitude"]),
                    elevation=loc_data.get("elevation"),
                    model=model,
                )
