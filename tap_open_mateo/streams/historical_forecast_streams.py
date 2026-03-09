"""Historical Forecast stream definitions for Open-Meteo.

Provides archived model forecast data from 2022 onward,
distinct from the Historical Weather (ERA5 reanalysis) endpoint.
"""

from __future__ import annotations

import typing as t

from tap_open_mateo.client import OpenMateoStream
from tap_open_mateo.streams.forecast_streams import (
    FORECAST_DAILY_PROPERTIES,
    FORECAST_HOURLY_PROPERTIES,
)

if t.TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers.types import Context


class HistoricalForecastHourlyStream(OpenMateoStream):
    """Archived weather model forecasts at hourly resolution.

    Uses date-chunked partitions to split large date ranges into
    manageable API requests. Data available from 2022 onward.
    """

    name = "historical_forecast_hourly"
    path = "/v1/forecast"
    _free_url_base = "https://historical-forecast-api.open-meteo.com"
    _paid_url_base = "https://customer-historical-forecast-api.open-meteo.com"
    primary_keys = ("location_name", "model", "time", "granularity")
    replication_key = "time"
    schema = FORECAST_HOURLY_PROPERTIES.to_dict()

    @property
    def partitions(self) -> list[dict]:
        return self._date_chunked_partitions(
            "historical_forecast_start_date", "historical_forecast_end_date"
        )

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


class HistoricalForecastDailyStream(OpenMateoStream):
    """Archived weather model forecasts at daily resolution.

    Same partitioning as hourly but queries daily-aggregated variables.
    """

    name = "historical_forecast_daily"
    path = "/v1/forecast"
    _free_url_base = "https://historical-forecast-api.open-meteo.com"
    _paid_url_base = "https://customer-historical-forecast-api.open-meteo.com"
    primary_keys = ("location_name", "model", "time", "granularity")
    replication_key = "time"
    schema = FORECAST_DAILY_PROPERTIES.to_dict()

    @property
    def partitions(self) -> list[dict]:
        return self._date_chunked_partitions(
            "historical_forecast_start_date", "historical_forecast_end_date"
        )

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
