"""Historical (ERA5) stream definitions for Open-Meteo."""

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


class HistoricalHourlyStream(OpenMateoStream):
    """ERA5 historical weather data at hourly resolution.

    Partitioned by (location_batch, date_chunk) for manageable request sizes.
    Supports incremental sync via replication_key='time'.
    """

    name = "historical_hourly"
    path = "/v1/archive"
    _free_url_base = "https://archive-api.open-meteo.com"
    _paid_url_base = "https://customer-archive-api.open-meteo.com"
    primary_keys = ("location_name", "model", "time", "granularity")
    replication_key = "time"
    schema = FORECAST_HOURLY_PROPERTIES.to_dict()

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
    """

    name = "historical_daily"
    path = "/v1/archive"
    _free_url_base = "https://archive-api.open-meteo.com"
    _paid_url_base = "https://customer-archive-api.open-meteo.com"
    primary_keys = ("location_name", "model", "time", "granularity")
    replication_key = "time"
    schema = FORECAST_DAILY_PROPERTIES.to_dict()

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
