"""Ensemble forecast stream definitions for Open-Meteo."""

from __future__ import annotations

import itertools
import typing as t

from singer_sdk import typing as th

from tap_open_mateo.client import OpenMateoStream
from tap_open_mateo.helpers import pivot_ensemble_to_rows

if t.TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers.types import Context


class EnsembleHourlyStream(OpenMateoStream):
    """Ensemble forecast member data at hourly resolution.

    Queries the Ensemble API and normalizes _memberNN columns into
    separate rows with a member field (0=control, 1-N=perturbed).
    Partitioned by (location_batch, ensemble_model).
    """

    name = "ensemble_hourly"
    path = "/v1/ensemble"
    _free_url_base = "https://ensemble-api.open-meteo.com"
    _paid_url_base = "https://customer-ensemble-api.open-meteo.com"
    primary_keys = ("location_name", "model", "member", "time", "granularity")
    replication_key = None

    schema = th.PropertiesList(
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
        th.Property("relative_humidity_2m", th.NumberType),
        th.Property("dew_point_2m", th.NumberType),
        th.Property("apparent_temperature", th.NumberType),
        # Precipitation
        th.Property("precipitation", th.NumberType),
        th.Property("rain", th.NumberType),
        th.Property("snowfall", th.NumberType),
        th.Property("snow_depth", th.NumberType),
        # Pressure
        th.Property("pressure_msl", th.NumberType),
        th.Property("surface_pressure", th.NumberType),
        # Wind (10m)
        th.Property("wind_speed_10m", th.NumberType),
        th.Property("wind_direction_10m", th.NumberType),
        th.Property("wind_gusts_10m", th.NumberType),
        # Wind (upper levels)
        th.Property("wind_speed_80m", th.NumberType),
        th.Property("wind_speed_100m", th.NumberType),
        th.Property("wind_speed_120m", th.NumberType),
        th.Property("wind_direction_80m", th.NumberType),
        th.Property("wind_direction_100m", th.NumberType),
        th.Property("wind_direction_120m", th.NumberType),
        # Cloud & visibility
        th.Property("cloud_cover", th.NumberType),
        th.Property("visibility", th.NumberType),
        th.Property("weather_code", th.IntegerType),
        th.Property("is_day", th.IntegerType),
        # Solar radiation
        th.Property("shortwave_radiation", th.NumberType),
        th.Property("direct_radiation", th.NumberType),
        th.Property("direct_normal_irradiance", th.NumberType),
        th.Property("diffuse_radiation", th.NumberType),
        th.Property("global_tilted_irradiance", th.NumberType),
        th.Property("sunshine_duration", th.NumberType),
        # Surface & soil (point depths — from default hourly_variables)
        th.Property("soil_temperature_0cm", th.NumberType),
        th.Property("soil_moisture_0_to_1cm", th.NumberType),
        # Surface & soil (range depths)
        th.Property("surface_temperature", th.NumberType),
        th.Property("soil_temperature_0_to_10cm", th.NumberType),
        th.Property("soil_temperature_10_to_40cm", th.NumberType),
        th.Property("soil_temperature_40_to_100cm", th.NumberType),
        th.Property("soil_temperature_100_to_200cm", th.NumberType),
        th.Property("soil_moisture_0_to_10cm", th.NumberType),
        th.Property("soil_moisture_10_to_40cm", th.NumberType),
        th.Property("soil_moisture_40_to_100cm", th.NumberType),
        th.Property("soil_moisture_100_to_200cm", th.NumberType),
        # Temperature (upper levels)
        th.Property("temperature_80m", th.NumberType),
        th.Property("temperature_120m", th.NumberType),
        # Precipitation (additional)
        th.Property("snowfall_water_equivalent", th.NumberType),
        th.Property("snow_depth_water_equivalent", th.NumberType),
        th.Property("snowfall_height", th.NumberType),
        # Humidity (additional)
        th.Property("wet_bulb_temperature_2m", th.NumberType),
        # Atmospheric
        th.Property("cape", th.NumberType),
        th.Property("convective_inhibition", th.NumberType),
        th.Property("vapour_pressure_deficit", th.NumberType),
        th.Property("evapotranspiration", th.NumberType),
        th.Property("et0_fao_evapotranspiration", th.NumberType),
        th.Property("freezing_level_height", th.NumberType),
        # UV
        th.Property("uv_index", th.NumberType),
        th.Property("uv_index_clear_sky", th.NumberType),
    ).to_dict()

    @property
    def partitions(self) -> list[dict]:
        """Generate partitions by (location_batch, ensemble_model)."""
        batches = self._batch_locations(
            self.get_resolved_locations(),
            self.config.get("max_locations_per_request", 10),
        )
        ensemble_models = self.config.get("ensemble_models", ["gfs025"])

        return [
            {"batch_idx": batch_idx, "locations": batch, "ensemble_model": model}
            for (batch_idx, batch), model
            in itertools.product(enumerate(batches), ensemble_models)
        ]

    def get_records(self, context: Context | None) -> Iterable[dict]:
        if context is None:
            return

        locations = context["locations"]
        ensemble_model = context["ensemble_model"]
        hourly_vars = self.config.get("hourly_variables", [])

        params = {
            **self._build_base_params(),
            **self._build_location_params(locations),
            "hourly": ",".join(hourly_vars),
            "models": ensemble_model,
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
                model=ensemble_model,
            )
