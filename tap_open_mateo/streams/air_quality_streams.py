"""Air Quality stream definitions for Open-Meteo.

Provides particulate matter, gas concentrations, pollen data,
and air quality indices (European AQI and US AQI).
Hourly resolution only — no daily aggregation available.
"""

from __future__ import annotations

import typing as t

from singer_sdk import typing as th

from tap_open_mateo.client import OpenMateoStream

if t.TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers.types import Context

AIR_QUALITY_HOURLY_PROPERTIES = th.PropertiesList(
    th.Property("location_name", th.StringType, required=True),
    th.Property("latitude", th.NumberType, required=True),
    th.Property("longitude", th.NumberType, required=True),
    th.Property("elevation", th.NumberType),
    th.Property("model", th.StringType),
    th.Property("time", th.DateTimeType, required=True),
    th.Property("granularity", th.StringType, required=True),
    # Particulate matter
    th.Property("pm10", th.NumberType),
    th.Property("pm2_5", th.NumberType),
    # Gases
    th.Property("carbon_monoxide", th.NumberType),
    th.Property("carbon_dioxide", th.NumberType),
    th.Property("nitrogen_dioxide", th.NumberType),
    th.Property("sulphur_dioxide", th.NumberType),
    th.Property("ozone", th.NumberType),
    th.Property("ammonia", th.NumberType),
    th.Property("methane", th.NumberType),
    th.Property("formaldehyde", th.NumberType),
    th.Property("glyoxal", th.NumberType),
    th.Property("non_methane_volatile_organic_compounds", th.NumberType),
    th.Property("nitrogen_monoxide", th.NumberType),
    th.Property("peroxyacyl_nitrates", th.NumberType),
    # Aerosols and dust
    th.Property("aerosol_optical_depth", th.NumberType),
    th.Property("dust", th.NumberType),
    th.Property("sea_salt_aerosol", th.NumberType),
    th.Property("secondary_inorganic_aerosol", th.NumberType),
    th.Property("residential_elementary_carbon", th.NumberType),
    th.Property("total_elementary_carbon", th.NumberType),
    th.Property("pm2_5_total_organic_matter", th.NumberType),
    # UV
    th.Property("uv_index", th.NumberType),
    th.Property("uv_index_clear_sky", th.NumberType),
    # Pollen
    th.Property("alder_pollen", th.NumberType),
    th.Property("birch_pollen", th.NumberType),
    th.Property("grass_pollen", th.NumberType),
    th.Property("mugwort_pollen", th.NumberType),
    th.Property("olive_pollen", th.NumberType),
    th.Property("ragweed_pollen", th.NumberType),
    # European AQI
    th.Property("european_aqi", th.NumberType),
    th.Property("european_aqi_pm2_5", th.NumberType),
    th.Property("european_aqi_pm10", th.NumberType),
    th.Property("european_aqi_nitrogen_dioxide", th.NumberType),
    th.Property("european_aqi_ozone", th.NumberType),
    th.Property("european_aqi_sulphur_dioxide", th.NumberType),
    # US AQI
    th.Property("us_aqi", th.NumberType),
    th.Property("us_aqi_pm2_5", th.NumberType),
    th.Property("us_aqi_pm10", th.NumberType),
    th.Property("us_aqi_nitrogen_dioxide", th.NumberType),
    th.Property("us_aqi_ozone", th.NumberType),
    th.Property("us_aqi_sulphur_dioxide", th.NumberType),
    th.Property("us_aqi_carbon_monoxide", th.NumberType),
)

DEFAULT_AIR_QUALITY_VARIABLES = [
    "pm10",
    "pm2_5",
    "carbon_monoxide",
    "nitrogen_dioxide",
    "sulphur_dioxide",
    "ozone",
    "aerosol_optical_depth",
    "dust",
    "uv_index",
    "european_aqi",
    "us_aqi",
]


class AirQualityHourlyStream(OpenMateoStream):
    """Air quality forecast and monitoring data at hourly resolution.

    Does not accept weather unit parameters (temperature_unit, etc.).
    Supports CAMS European (11 km) and CAMS Global (45 km) domains.
    """

    name = "air_quality_hourly"
    path = "/v1/air-quality"
    _free_url_base = "https://air-quality-api.open-meteo.com"
    _paid_url_base = "https://customer-air-quality-api.open-meteo.com"
    primary_keys = ("location_name", "model", "time", "granularity")
    replication_key = None
    schema = AIR_QUALITY_HOURLY_PROPERTIES.to_dict()

    def _build_base_params(self) -> dict:
        """Air quality API does not accept weather unit params."""
        params: dict[str, str] = {
            "timezone": self.config.get("timezone", "America/Chicago"),
            "timeformat": "iso8601",
        }
        domains = self.config.get("air_quality_domains", "auto")
        if domains != "auto":
            params["domains"] = domains
        self._apply_optional_params(params)
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
                self.config.get("air_quality_variables", DEFAULT_AIR_QUALITY_VARIABLES)
            ),
            "forecast_days": str(min(self.config.get("forecast_days", 7), 7)),
            "past_days": str(self.config.get("past_days", 7)),
        }

        # Air quality uses "auto" as default model; no models param needed
        yield from self._fetch_and_extract(locations, "hourly", "best_match", extra)
