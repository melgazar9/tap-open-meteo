"""Seasonal Forecast stream definitions for Open-Meteo.

Provides multi-month ensemble forecasts (up to 7 months via SEAS5).
API uses 'hourly' parameter but returns data at 6-hour intervals.
Response includes ensemble member columns (_member01 through _member50).
"""

from __future__ import annotations

import itertools
import typing as t

from singer_sdk import typing as th

from tap_open_mateo.client import OpenMateoStream
from tap_open_mateo.helpers import pivot_ensemble_to_rows

if t.TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers.types import Context

SEASONAL_SIX_HOURLY_PROPERTIES = th.PropertiesList(
    th.Property("location_name", th.StringType, required=True),
    th.Property("latitude", th.NumberType, required=True),
    th.Property("longitude", th.NumberType, required=True),
    th.Property("elevation", th.NumberType),
    th.Property("model", th.StringType, required=True),
    th.Property("member", th.IntegerType, required=True),
    th.Property("time", th.DateTimeType, required=True),
    th.Property("granularity", th.StringType, required=True),
    # Temperature & humidity
    th.Property("temperature_2m", th.NumberType),
    th.Property("temperature_2m_max", th.NumberType),
    th.Property("temperature_2m_min", th.NumberType),
    th.Property("relative_humidity_2m", th.NumberType),
    th.Property("dew_point_2m", th.NumberType),
    th.Property("apparent_temperature", th.NumberType),
    # Pressure
    th.Property("pressure_msl", th.NumberType),
    # Precipitation
    th.Property("precipitation", th.NumberType),
    th.Property("showers", th.NumberType),
    th.Property("snowfall", th.NumberType),
    th.Property("rain", th.NumberType),
    th.Property("weather_code", th.IntegerType),
    th.Property("cloud_cover", th.NumberType),
    # Wind
    th.Property("wind_speed_10m", th.NumberType),
    th.Property("wind_speed_100m", th.NumberType),
    th.Property("wind_speed_200m", th.NumberType),
    th.Property("wind_direction_10m", th.NumberType),
    th.Property("wind_direction_100m", th.NumberType),
    th.Property("wind_direction_200m", th.NumberType),
    th.Property("wind_gusts_10m", th.NumberType),
    # Soil
    th.Property("soil_temperature_0_to_7cm", th.NumberType),
    th.Property("soil_temperature_7_to_28cm", th.NumberType),
    th.Property("soil_temperature_28_to_100cm", th.NumberType),
    th.Property("soil_temperature_100_to_255cm", th.NumberType),
    th.Property("soil_moisture_0_to_7cm", th.NumberType),
    th.Property("soil_moisture_7_to_28cm", th.NumberType),
    th.Property("soil_moisture_28_to_100cm", th.NumberType),
    th.Property("soil_moisture_100_to_255cm", th.NumberType),
    # Solar & atmospheric
    th.Property("sunshine_duration", th.NumberType),
    th.Property("shortwave_radiation", th.NumberType),
    th.Property("vapour_pressure_deficit", th.NumberType),
    th.Property("et0_fao_evapotranspiration", th.NumberType),
    # Marine
    th.Property("wave_height", th.NumberType),
    th.Property("wave_direction", th.NumberType),
    th.Property("wave_period", th.NumberType),
    th.Property("wave_peak_period", th.NumberType),
    th.Property("sea_surface_temperature", th.NumberType),
)

SEASONAL_DAILY_PROPERTIES = th.PropertiesList(
    th.Property("location_name", th.StringType, required=True),
    th.Property("latitude", th.NumberType, required=True),
    th.Property("longitude", th.NumberType, required=True),
    th.Property("elevation", th.NumberType),
    th.Property("model", th.StringType, required=True),
    th.Property("member", th.IntegerType, required=True),
    th.Property("time", th.DateTimeType, required=True),
    th.Property("granularity", th.StringType, required=True),
    # Temperature
    th.Property("temperature_2m_max", th.NumberType),
    th.Property("temperature_2m_min", th.NumberType),
    th.Property("temperature_2m_mean", th.NumberType),
    th.Property("apparent_temperature_max", th.NumberType),
    th.Property("apparent_temperature_min", th.NumberType),
    th.Property("apparent_temperature_mean", th.NumberType),
    th.Property("wet_bulb_temperature_2m_max", th.NumberType),
    th.Property("wet_bulb_temperature_2m_min", th.NumberType),
    th.Property("wet_bulb_temperature_2m_mean", th.NumberType),
    # Humidity
    th.Property("relative_humidity_2m_max", th.NumberType),
    th.Property("relative_humidity_2m_mean", th.NumberType),
    th.Property("relative_humidity_2m_min", th.NumberType),
    th.Property("dew_point_2m_max", th.NumberType),
    th.Property("dew_point_2m_mean", th.NumberType),
    th.Property("dew_point_2m_min", th.NumberType),
    # Precipitation
    th.Property("precipitation_sum", th.NumberType),
    th.Property("rain_sum", th.NumberType),
    th.Property("showers_sum", th.NumberType),
    th.Property("snowfall_sum", th.NumberType),
    th.Property("snowfall_water_equivalent_sum", th.NumberType),
    th.Property("weather_code", th.IntegerType),
    # Pressure
    th.Property("pressure_msl_max", th.NumberType),
    th.Property("pressure_msl_mean", th.NumberType),
    th.Property("pressure_msl_min", th.NumberType),
    th.Property("surface_pressure_max", th.NumberType),
    th.Property("surface_pressure_mean", th.NumberType),
    th.Property("surface_pressure_min", th.NumberType),
    # Sea surface
    th.Property("sea_surface_temperature_max", th.NumberType),
    th.Property("sea_surface_temperature_mean", th.NumberType),
    th.Property("sea_surface_temperature_min", th.NumberType),
    # Cloud
    th.Property("cloud_cover_max", th.NumberType),
    th.Property("cloud_cover_mean", th.NumberType),
    th.Property("cloud_cover_min", th.NumberType),
    # Solar & evapotranspiration
    th.Property("shortwave_radiation_sum", th.NumberType),
    th.Property("et0_fao_evapotranspiration", th.NumberType),
    th.Property("sunshine_duration", th.NumberType),
    th.Property("sunrise", th.StringType),
    th.Property("sunset", th.StringType),
    # Atmospheric
    th.Property("vapour_pressure_deficit_max", th.NumberType),
    # Wind 10m
    th.Property("wind_speed_10m_max", th.NumberType),
    th.Property("wind_speed_10m_mean", th.NumberType),
    th.Property("wind_speed_10m_min", th.NumberType),
    th.Property("wind_gusts_10m_max", th.NumberType),
    th.Property("wind_gusts_10m_mean", th.NumberType),
    th.Property("wind_gusts_10m_min", th.NumberType),
    th.Property("wind_direction_10m_dominant", th.NumberType),
    # Wind 100m
    th.Property("wind_speed_100m_max", th.NumberType),
    th.Property("wind_speed_100m_mean", th.NumberType),
    th.Property("wind_speed_100m_min", th.NumberType),
    th.Property("wind_direction_100m_dominant", th.NumberType),
    # Wind 200m
    th.Property("wind_speed_200m_max", th.NumberType),
    th.Property("wind_speed_200m_mean", th.NumberType),
    th.Property("wind_speed_200m_min", th.NumberType),
    th.Property("wind_direction_200m_dominant", th.NumberType),
    # Soil
    th.Property("soil_temperature_0_to_7cm_mean", th.NumberType),
    th.Property("soil_temperature_7_to_28cm_mean", th.NumberType),
    th.Property("soil_temperature_28_to_100cm_mean", th.NumberType),
    th.Property("soil_temperature_100_to_255cm_mean", th.NumberType),
    th.Property("soil_moisture_0_to_7cm_mean", th.NumberType),
    th.Property("soil_moisture_7_to_28cm_mean", th.NumberType),
    th.Property("soil_moisture_28_to_100cm_mean", th.NumberType),
    th.Property("soil_moisture_100_to_255cm_mean", th.NumberType),
)

DEFAULT_SEASONAL_SIX_HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "precipitation",
    "wind_speed_10m",
    "wind_direction_10m",
    "pressure_msl",
    "cloud_cover",
]

DEFAULT_SEASONAL_DAILY_VARIABLES = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
]


class _SeasonalBaseStream(OpenMateoStream):
    """Shared config for seasonal six-hourly and daily streams."""

    path = "/v1/seasonal"
    _free_url_base = "https://seasonal-api.open-meteo.com"
    _paid_url_base = "https://customer-seasonal-api.open-meteo.com"
    primary_keys = ("location_name", "model", "member", "time", "granularity")
    replication_key = None

    @property
    def partitions(self) -> list[dict]:
        """Generate partitions by (location_batch, seasonal_model)."""
        batches = self._batch_locations(
            self.get_resolved_locations(),
            self.config.get("max_locations_per_request", 10),
        )
        seasonal_models = self.config.get("seasonal_models", ["ecmwf_seasonal_seamless"])

        return [
            {"batch_idx": batch_idx, "locations": batch, "seasonal_model": model}
            for (batch_idx, batch), model
            in itertools.product(enumerate(batches), seasonal_models)
        ]


class SeasonalSixHourlyStream(_SeasonalBaseStream):
    """Seasonal ensemble forecast at 6-hourly resolution.

    The API parameter is 'hourly' but timestamps are 6 hours apart.
    Records are labeled with granularity='six_hourly' to distinguish
    from true hourly data. Ensemble member columns are normalized
    into separate rows via pivot_ensemble_to_rows.
    """

    name = "seasonal_six_hourly"
    schema = SEASONAL_SIX_HOURLY_PROPERTIES.to_dict()

    def get_records(self, context: Context | None) -> Iterable[dict]:
        if context is None:
            return

        locations = context["locations"]
        seasonal_model = context["seasonal_model"]
        hourly_vars = self.config.get(
            "seasonal_six_hourly_variables", DEFAULT_SEASONAL_SIX_HOURLY_VARIABLES
        )

        params = {
            **self._build_base_params(),
            **self._build_location_params(locations),
            "hourly": ",".join(hourly_vars),
            "models": seasonal_model,
        }

        data = self._make_request(f"{self.url_base}{self.path}", params)
        if not data:
            return

        data_items = data if isinstance(data, list) else [data]
        location_list = locations if isinstance(data, list) else [locations[0]]

        for loc_data, loc in zip(data_items, location_list):
            section = self._validate_response_section(loc_data, "hourly")
            if not section:
                continue

            yield from pivot_ensemble_to_rows(
                section_data=section,
                times=section.get("time", []),
                base_variables=hourly_vars,
                location_name=loc["name"],
                latitude=loc_data.get("latitude", loc["latitude"]),
                longitude=loc_data.get("longitude", loc["longitude"]),
                elevation=loc_data.get("elevation"),
                model=seasonal_model,
                granularity="six_hourly",
            )


class SeasonalDailyStream(_SeasonalBaseStream):
    """Seasonal ensemble forecast at daily resolution.

    Daily aggregations of the seasonal ensemble forecast.
    Ensemble member columns are normalized into separate rows.
    """

    name = "seasonal_daily"
    schema = SEASONAL_DAILY_PROPERTIES.to_dict()

    def get_records(self, context: Context | None) -> Iterable[dict]:
        if context is None:
            return

        locations = context["locations"]
        seasonal_model = context["seasonal_model"]
        daily_vars = self.config.get(
            "seasonal_daily_variables", DEFAULT_SEASONAL_DAILY_VARIABLES
        )

        params = {
            **self._build_base_params(),
            **self._build_location_params(locations),
            "daily": ",".join(daily_vars),
            "models": seasonal_model,
        }

        data = self._make_request(f"{self.url_base}{self.path}", params)
        if not data:
            return

        data_items = data if isinstance(data, list) else [data]
        location_list = locations if isinstance(data, list) else [locations[0]]

        for loc_data, loc in zip(data_items, location_list):
            section = self._validate_response_section(loc_data, "daily")
            if not section:
                continue

            yield from pivot_ensemble_to_rows(
                section_data=section,
                times=section.get("time", []),
                base_variables=daily_vars,
                location_name=loc["name"],
                latitude=loc_data.get("latitude", loc["latitude"]),
                longitude=loc_data.get("longitude", loc["longitude"]),
                elevation=loc_data.get("elevation"),
                model=seasonal_model,
                granularity="daily",
            )
