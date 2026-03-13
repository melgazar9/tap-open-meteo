"""Historical (ERA5) stream definitions for Open-Meteo.

Historical Weather API uses different variable naming than Forecast API:
- Soil variables use range depths (0_to_7cm) instead of point depths (0cm)
- Wind has 100m level instead of 80m/120m/180m
- Fewer daily variables (no precipitation_probability, no uv_index)
"""

from __future__ import annotations

import typing as t

from singer_sdk import typing as th

from tap_open_mateo.client import OpenMateoStream

if t.TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers.types import Context

HISTORICAL_HOURLY_PROPERTIES = th.PropertiesList(
    th.Property("location_name", th.StringType, required=True),
    th.Property("latitude", th.NumberType, required=True),
    th.Property("longitude", th.NumberType, required=True),
    th.Property("elevation", th.NumberType),
    th.Property("model", th.StringType),
    th.Property("time", th.DateTimeType, required=True),
    th.Property("granularity", th.StringType, required=True),
    # Temperature & humidity
    th.Property("temperature_2m", th.NumberType),
    th.Property("relative_humidity_2m", th.NumberType),
    th.Property("dew_point_2m", th.NumberType),
    th.Property("apparent_temperature", th.NumberType),
    th.Property("wet_bulb_temperature_2m", th.NumberType),
    # Precipitation
    th.Property("precipitation", th.NumberType),
    th.Property("rain", th.NumberType),
    th.Property("snowfall", th.NumberType),
    th.Property("snow_depth", th.NumberType),
    # Pressure
    th.Property("pressure_msl", th.NumberType),
    th.Property("surface_pressure", th.NumberType),
    # Wind
    th.Property("wind_speed_10m", th.NumberType),
    th.Property("wind_speed_100m", th.NumberType),
    th.Property("wind_direction_10m", th.NumberType),
    th.Property("wind_direction_100m", th.NumberType),
    th.Property("wind_gusts_10m", th.NumberType),
    # Cloud
    th.Property("cloud_cover", th.NumberType),
    th.Property("cloud_cover_low", th.NumberType),
    th.Property("cloud_cover_mid", th.NumberType),
    th.Property("cloud_cover_high", th.NumberType),
    th.Property("weather_code", th.IntegerType),
    th.Property("is_day", th.IntegerType),
    # Solar radiation
    th.Property("shortwave_radiation", th.NumberType),
    th.Property("direct_radiation", th.NumberType),
    th.Property("direct_normal_irradiance", th.NumberType),
    th.Property("diffuse_radiation", th.NumberType),
    th.Property("global_tilted_irradiance", th.NumberType),
    th.Property("terrestrial_radiation", th.NumberType),
    th.Property("shortwave_radiation_instant", th.NumberType),
    th.Property("direct_radiation_instant", th.NumberType),
    th.Property("diffuse_radiation_instant", th.NumberType),
    th.Property("direct_normal_irradiance_instant", th.NumberType),
    th.Property("global_tilted_irradiance_instant", th.NumberType),
    th.Property("terrestrial_radiation_instant", th.NumberType),
    th.Property("sunshine_duration", th.NumberType),
    # Soil (ERA5 range depths)
    th.Property("soil_temperature_0_to_7cm", th.NumberType),
    th.Property("soil_temperature_7_to_28cm", th.NumberType),
    th.Property("soil_temperature_28_to_100cm", th.NumberType),
    th.Property("soil_temperature_100_to_255cm", th.NumberType),
    th.Property("soil_moisture_0_to_7cm", th.NumberType),
    th.Property("soil_moisture_7_to_28cm", th.NumberType),
    th.Property("soil_moisture_28_to_100cm", th.NumberType),
    th.Property("soil_moisture_100_to_255cm", th.NumberType),
    # Soil (forecast-style point depths — also accepted by archive API)
    th.Property("soil_temperature_0cm", th.NumberType),
    th.Property("soil_moisture_0_to_1cm", th.NumberType),
    # Atmospheric
    th.Property("et0_fao_evapotranspiration", th.NumberType),
    th.Property("vapour_pressure_deficit", th.NumberType),
    th.Property("boundary_layer_height", th.NumberType),
    th.Property("total_column_integrated_water_vapour", th.NumberType),
    # ERA5-specific
    th.Property("albedo", th.NumberType),
    th.Property("snow_depth_water_equivalent", th.NumberType),
)

HISTORICAL_DAILY_PROPERTIES = th.PropertiesList(
    th.Property("location_name", th.StringType, required=True),
    th.Property("latitude", th.NumberType, required=True),
    th.Property("longitude", th.NumberType, required=True),
    th.Property("elevation", th.NumberType),
    th.Property("model", th.StringType),
    th.Property("time", th.DateTimeType, required=True),
    th.Property("granularity", th.StringType, required=True),
    th.Property("weather_code", th.IntegerType),
    th.Property("temperature_2m_max", th.NumberType),
    th.Property("temperature_2m_min", th.NumberType),
    th.Property("temperature_2m_mean", th.NumberType),
    th.Property("apparent_temperature_max", th.NumberType),
    th.Property("apparent_temperature_min", th.NumberType),
    th.Property("precipitation_sum", th.NumberType),
    th.Property("rain_sum", th.NumberType),
    th.Property("snowfall_sum", th.NumberType),
    th.Property("precipitation_hours", th.NumberType),
    th.Property("sunrise", th.StringType),
    th.Property("sunset", th.StringType),
    th.Property("sunshine_duration", th.NumberType),
    th.Property("daylight_duration", th.NumberType),
    th.Property("wind_speed_10m_max", th.NumberType),
    th.Property("wind_gusts_10m_max", th.NumberType),
    th.Property("wind_direction_10m_dominant", th.NumberType),
    th.Property("shortwave_radiation_sum", th.NumberType),
    th.Property("et0_fao_evapotranspiration", th.NumberType),
)


class HistoricalHourlyStream(OpenMateoStream):
    """ERA5 historical weather data at hourly resolution.

    Partitioned by (location_batch, date_chunk) for manageable request sizes.
    Supports incremental sync via replication_key='time'.
    ERA5 data is published with a 5-day delay; default end date accounts for this.
    """

    name = "historical_hourly"
    path = "/v1/archive"
    _free_url_base = "https://archive-api.open-meteo.com"
    _paid_url_base = "https://customer-archive-api.open-meteo.com"
    primary_keys = ("location_name", "model", "time", "granularity")
    replication_key = "time"
    _default_end_date_lag_days = 6  # ERA5 published with 5-day delay
    schema = HISTORICAL_HOURLY_PROPERTIES.to_dict()

    @property
    def partitions(self) -> list[dict]:
        return self._date_chunked_partitions("historical_start_date", "historical_end_date")

    def get_records(self, context: Context | None) -> Iterable[dict]:
        if context is None:
            return

        locations = context["locations"]
        extra = {
            "hourly": ",".join(self.config.get("hourly_variables", [])),
            "start_date": context["start_date"],
            "end_date": context["end_date"],
        }

        for model in self.config.get("models", ["best_match"]):
            yield from self._fetch_and_extract(locations, "hourly", model, extra)


class HistoricalDailyStream(OpenMateoStream):
    """ERA5 historical weather data at daily resolution.

    Same partitioning and incremental sync as HistoricalHourlyStream.
    ERA5 data is published with a 5-day delay; default end date accounts for this.
    """

    name = "historical_daily"
    path = "/v1/archive"
    _free_url_base = "https://archive-api.open-meteo.com"
    _paid_url_base = "https://customer-archive-api.open-meteo.com"
    primary_keys = ("location_name", "model", "time", "granularity")
    replication_key = "time"
    _default_end_date_lag_days = 6  # ERA5 published with 5-day delay
    schema = HISTORICAL_DAILY_PROPERTIES.to_dict()

    @property
    def partitions(self) -> list[dict]:
        return self._date_chunked_partitions("historical_start_date", "historical_end_date")

    def get_records(self, context: Context | None) -> Iterable[dict]:
        if context is None:
            return

        locations = context["locations"]
        extra = {
            "daily": ",".join(self.config.get("daily_variables", [])),
            "start_date": context["start_date"],
            "end_date": context["end_date"],
        }

        for model in self.config.get("models", ["best_match"]):
            yield from self._fetch_and_extract(locations, "daily", model, extra)
