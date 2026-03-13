"""Climate (CMIP6) projection stream definitions for Open-Meteo."""

from __future__ import annotations

import itertools
import typing as t

from singer_sdk import typing as th

from tap_open_mateo.client import OpenMateoStream

if t.TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers.types import Context

CLIMATE_DAILY_VARIABLES = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "rain_sum",
    "snowfall_sum",
    "wind_speed_10m_mean",
    "wind_speed_10m_max",
    "cloud_cover_mean",
    "pressure_msl_mean",
    "relative_humidity_2m_max",
    "relative_humidity_2m_min",
    "relative_humidity_2m_mean",
    "shortwave_radiation_sum",
    "soil_moisture_0_to_10cm_mean",
]


class ClimateDailyStream(OpenMateoStream):
    """CMIP6 climate projections at daily resolution.

    Partitioned by (location_batch, climate_model, date_chunk).
    Daily resolution ONLY — no hourly data available for climate projections.
    """

    name = "climate_daily"
    path = "/v1/climate"
    _free_url_base = "https://climate-api.open-meteo.com"
    _paid_url_base = "https://customer-climate-api.open-meteo.com"
    primary_keys = ("location_name", "model", "time", "granularity")
    replication_key = "time"

    schema = th.PropertiesList(
        th.Property("location_name", th.StringType, required=True),
        th.Property("latitude", th.NumberType, required=True),
        th.Property("longitude", th.NumberType, required=True),
        th.Property("elevation", th.NumberType),
        th.Property("model", th.StringType, required=True),
        th.Property("time", th.DateTimeType, required=True),
        th.Property("granularity", th.StringType, required=True),
        th.Property("temperature_2m_max", th.NumberType),
        th.Property("temperature_2m_min", th.NumberType),
        th.Property("temperature_2m_mean", th.NumberType),
        th.Property("precipitation_sum", th.NumberType),
        th.Property("rain_sum", th.NumberType),
        th.Property("snowfall_sum", th.NumberType),
        th.Property("wind_speed_10m_mean", th.NumberType),
        th.Property("wind_speed_10m_max", th.NumberType),
        th.Property("cloud_cover_mean", th.NumberType),
        th.Property("pressure_msl_mean", th.NumberType),
        th.Property("relative_humidity_2m_max", th.NumberType),
        th.Property("relative_humidity_2m_min", th.NumberType),
        th.Property("relative_humidity_2m_mean", th.NumberType),
        th.Property("shortwave_radiation_sum", th.NumberType),
        th.Property("soil_moisture_0_to_10cm_mean", th.NumberType),
    ).to_dict()

    @property
    def partitions(self) -> list[dict]:
        """Generate partitions by (location_batch, climate_model, date_chunk)."""
        base_partitions = self._date_chunked_partitions(
            start_key="climate_start_date",
            end_key="climate_end_date",
            chunk_multiplier=10,  # 10-year chunks for 100-year span
        )
        climate_models = self.config.get("climate_models", ["EC_Earth3P_HR", "MRI_AGCM3_2_S"])

        return [
            {**partition, "climate_model": model}
            for partition, model
            in itertools.product(base_partitions, climate_models)
        ]

    def get_records(self, context: Context | None) -> Iterable[dict]:
        if context is None:
            return

        locations = context["locations"]
        climate_model = context["climate_model"]
        extra = {
            "daily": ",".join(CLIMATE_DAILY_VARIABLES),
            "models": climate_model,
            "start_date": context["start_date"],
            "end_date": context["end_date"],
        }

        yield from self._fetch_and_extract(locations, "daily", climate_model, extra)
