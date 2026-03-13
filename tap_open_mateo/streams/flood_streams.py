"""Flood stream definitions for Open-Meteo.

Provides river discharge forecasts from the GloFAS model.
Supports ensemble mode (50 members) or statistical aggregates
(mean, median, min, max, p25, p75).
Data available from 1984 onward with up to 210-day forecasts.
"""

from __future__ import annotations

import typing as t

from singer_sdk import typing as th

from tap_open_mateo.client import OpenMateoStream
from tap_open_mateo.helpers import pivot_columnar_to_rows, pivot_ensemble_to_rows

if t.TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers.types import Context

FLOOD_DAILY_PROPERTIES = th.PropertiesList(
    th.Property("location_name", th.StringType, required=True),
    th.Property("latitude", th.NumberType, required=True),
    th.Property("longitude", th.NumberType, required=True),
    th.Property("elevation", th.NumberType),
    th.Property("model", th.StringType),
    th.Property("member", th.IntegerType),
    th.Property("time", th.DateTimeType, required=True),
    th.Property("granularity", th.StringType, required=True),
    # Ensemble variable (populated when flood_ensemble=true)
    th.Property("river_discharge", th.NumberType),
    # Statistical variables (populated when flood_ensemble=false)
    th.Property("river_discharge_mean", th.NumberType),
    th.Property("river_discharge_median", th.NumberType),
    th.Property("river_discharge_max", th.NumberType),
    th.Property("river_discharge_min", th.NumberType),
    th.Property("river_discharge_p25", th.NumberType),
    th.Property("river_discharge_p75", th.NumberType),
)

DEFAULT_FLOOD_VARIABLES = [
    "river_discharge",
]

DEFAULT_FLOOD_STATS_VARIABLES = [
    "river_discharge_mean",
    "river_discharge_median",
    "river_discharge_max",
    "river_discharge_min",
    "river_discharge_p25",
    "river_discharge_p75",
]


class FloodDailyStream(OpenMateoStream):
    """River discharge forecasts from GloFAS at daily resolution.

    When flood_ensemble=true, returns 50 ensemble member rows using
    pivot_ensemble_to_rows. When false, returns statistical aggregates
    (mean, median, max, min, p25, p75) using pivot_columnar_to_rows.
    """

    name = "flood_daily"
    path = "/v1/flood"
    _free_url_base = "https://flood-api.open-meteo.com"
    _paid_url_base = "https://customer-flood-api.open-meteo.com"
    primary_keys = ("location_name", "member", "time", "granularity")
    replication_key = None
    schema = FLOOD_DAILY_PROPERTIES.to_dict()

    def _build_base_params(self) -> dict:
        """Flood API does not accept weather unit params."""
        params: dict[str, str] = {
            "timeformat": "iso8601",
        }
        self._apply_optional_params(params)
        return params

    @property
    def partitions(self) -> list[dict]:
        return self._location_batch_partitions()

    def get_records(self, context: Context | None) -> Iterable[dict]:
        if context is None:
            return

        locations = context["locations"]
        use_ensemble = self.config.get("flood_ensemble", False)

        if use_ensemble:
            variables = self.config.get("flood_variables", DEFAULT_FLOOD_VARIABLES)
        else:
            variables = self.config.get("flood_variables", DEFAULT_FLOOD_STATS_VARIABLES)

        params = {
            **self._build_base_params(),
            **self._build_location_params(locations),
            "daily": ",".join(variables),
            "forecast_days": str(self.config.get("flood_forecast_days", 92)),
            "past_days": str(self.config.get("past_days", 7)),
        }

        if use_ensemble:
            params["ensemble"] = "true"

        data = self._make_request(f"{self.url_base}{self.path}", params)
        if not data:
            return

        data_items = data if isinstance(data, list) else [data]
        location_list = locations if isinstance(data, list) else [locations[0]]

        for loc_data, loc in zip(data_items, location_list):
            section = self._validate_response_section(loc_data, "daily")
            if not section:
                continue

            if use_ensemble:
                records = pivot_ensemble_to_rows(
                    section_data=section,
                    times=section.get("time", []),
                    base_variables=variables,
                    location_name=loc["name"],
                    latitude=loc_data.get("latitude", loc["latitude"]),
                    longitude=loc_data.get("longitude", loc["longitude"]),
                    elevation=loc_data.get("elevation"),
                    model="glofas_seamless",
                    granularity="daily",
                )
            else:
                records = pivot_columnar_to_rows(
                    section_data=section,
                    times=section.get("time", []),
                    location_name=loc["name"],
                    latitude=loc_data.get("latitude", loc["latitude"]),
                    longitude=loc_data.get("longitude", loc["longitude"]),
                    elevation=loc_data.get("elevation"),
                    model="glofas_seamless",
                    granularity="daily",
                )
                # Add member=0 for non-ensemble mode to match primary_keys
                for record in records:
                    record["member"] = 0

            yield from records
