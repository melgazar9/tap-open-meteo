"""Satellite Radiation stream definitions for Open-Meteo.

Provides satellite-derived solar radiation measurements from multiple
data sources (EUMETSAT MSG/MTG, SARAH3, Himawari).
Supports panel tilt/azimuth for Global Tilted Irradiance calculations.
"""

from __future__ import annotations

import typing as t

from singer_sdk import typing as th

from tap_open_mateo.client import OpenMateoStream

if t.TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers.types import Context

SATELLITE_HOURLY_PROPERTIES = th.PropertiesList(
    th.Property("location_name", th.StringType, required=True),
    th.Property("latitude", th.NumberType, required=True),
    th.Property("longitude", th.NumberType, required=True),
    th.Property("elevation", th.NumberType),
    th.Property("model", th.StringType),
    th.Property("time", th.DateTimeType, required=True),
    th.Property("granularity", th.StringType, required=True),
    th.Property("shortwave_radiation", th.NumberType),
    th.Property("direct_radiation", th.NumberType),
    th.Property("diffuse_radiation", th.NumberType),
    th.Property("direct_normal_irradiance", th.NumberType),
    th.Property("global_tilted_irradiance", th.NumberType),
    th.Property("shortwave_radiation_clear_sky", th.NumberType),
    th.Property("terrestrial_radiation", th.NumberType),
    th.Property("shortwave_radiation_instant", th.NumberType),
    th.Property("shortwave_radiation_clear_sky_instant", th.NumberType),
    th.Property("direct_radiation_instant", th.NumberType),
    th.Property("diffuse_radiation_instant", th.NumberType),
    th.Property("direct_normal_irradiance_instant", th.NumberType),
    th.Property("global_tilted_irradiance_instant", th.NumberType),
    th.Property("terrestrial_radiation_instant", th.NumberType),
    th.Property("is_day", th.IntegerType),
    th.Property("sunshine_duration", th.NumberType),
)

SATELLITE_DAILY_PROPERTIES = th.PropertiesList(
    th.Property("location_name", th.StringType, required=True),
    th.Property("latitude", th.NumberType, required=True),
    th.Property("longitude", th.NumberType, required=True),
    th.Property("elevation", th.NumberType),
    th.Property("model", th.StringType),
    th.Property("time", th.DateTimeType, required=True),
    th.Property("granularity", th.StringType, required=True),
    th.Property("sunrise", th.StringType),
    th.Property("sunset", th.StringType),
    th.Property("daylight_duration", th.NumberType),
    th.Property("sunshine_duration", th.NumberType),
    th.Property("shortwave_radiation_sum", th.NumberType),
)

DEFAULT_SATELLITE_HOURLY_VARIABLES = [
    "shortwave_radiation",
    "direct_radiation",
    "diffuse_radiation",
    "direct_normal_irradiance",
    "global_tilted_irradiance",
    "is_day",
    "sunshine_duration",
]

DEFAULT_SATELLITE_DAILY_VARIABLES = [
    "sunrise",
    "sunset",
    "daylight_duration",
    "sunshine_duration",
    "shortwave_radiation_sum",
]


class _SatelliteBaseStream(OpenMateoStream):
    """Shared config for satellite radiation hourly and daily streams."""

    path = "/v1/archive"
    _free_url_base = "https://satellite-api.open-meteo.com"
    _paid_url_base = "https://customer-satellite-api.open-meteo.com"
    primary_keys = ("location_name", "model", "time", "granularity")
    replication_key = "time"

    def _build_base_params(self) -> dict:
        """Satellite API does not accept weather unit params. Supports tilt/azimuth."""
        params: dict[str, str] = {
            "timezone": self.config.get("timezone", "America/Chicago"),
            "timeformat": "iso8601",
        }
        if (tilt := self.config.get("solar_panel_tilt")) is not None:
            params["tilt"] = str(tilt)
        if (azimuth := self.config.get("solar_panel_azimuth")) is not None:
            params["azimuth"] = str(azimuth)
        self._apply_optional_params(params)
        return params


class SatelliteRadiationHourlyStream(_SatelliteBaseStream):
    """Satellite-derived solar radiation data at hourly resolution.

    Date-chunked like historical streams. Supports tilt/azimuth params
    for Global Tilted Irradiance calculations (solar panel output).
    """

    name = "satellite_radiation_hourly"
    schema = SATELLITE_HOURLY_PROPERTIES.to_dict()

    @property
    def partitions(self) -> list[dict]:
        return self._date_chunked_partitions(
            "satellite_start_date", "satellite_end_date"
        )

    def get_records(self, context: Context | None) -> Iterable[dict]:
        if context is None:
            return

        locations = context["locations"]
        extra = {
            "hourly": ",".join(
                self.config.get("satellite_hourly_variables", DEFAULT_SATELLITE_HOURLY_VARIABLES)
            ),
            "start_date": context["start_date"],
            "end_date": context["end_date"],
        }

        yield from self._fetch_and_extract(locations, "hourly", "best_match", extra)


class SatelliteRadiationDailyStream(_SatelliteBaseStream):
    """Satellite-derived solar radiation data at daily resolution.

    Daily aggregations of satellite radiation measurements.
    """

    name = "satellite_radiation_daily"
    schema = SATELLITE_DAILY_PROPERTIES.to_dict()

    @property
    def partitions(self) -> list[dict]:
        return self._date_chunked_partitions(
            "satellite_start_date", "satellite_end_date"
        )

    def get_records(self, context: Context | None) -> Iterable[dict]:
        if context is None:
            return

        locations = context["locations"]
        extra = {
            "daily": ",".join(
                self.config.get("satellite_daily_variables", DEFAULT_SATELLITE_DAILY_VARIABLES)
            ),
            "start_date": context["start_date"],
            "end_date": context["end_date"],
        }

        yield from self._fetch_and_extract(locations, "daily", "best_match", extra)
