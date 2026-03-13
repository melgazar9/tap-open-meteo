"""Geocoding and Elevation stream definitions for Open-Meteo.

These are utility/lookup endpoints with non-time-series responses.
Geocoding resolves location names to coordinates.
Elevation returns terrain altitude for coordinates.
"""

from __future__ import annotations

import logging
import typing as t

from singer_sdk import typing as th

from tap_open_mateo.client import OpenMateoStream

if t.TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers.types import Context

GEOCODING_PROPERTIES = th.PropertiesList(
    th.Property("id", th.IntegerType, required=True),
    th.Property("search_term", th.StringType, required=True),
    th.Property("name", th.StringType),
    th.Property("latitude", th.NumberType),
    th.Property("longitude", th.NumberType),
    th.Property("elevation", th.NumberType),
    th.Property("feature_code", th.StringType),
    th.Property("country_code", th.StringType),
    th.Property("country_id", th.IntegerType),
    th.Property("country", th.StringType),
    th.Property("timezone", th.StringType),
    th.Property("population", th.IntegerType),
    th.Property("admin1_id", th.IntegerType),
    th.Property("admin1", th.StringType),
    th.Property("admin2_id", th.IntegerType),
    th.Property("admin2", th.StringType),
    th.Property("admin3_id", th.IntegerType),
    th.Property("admin3", th.StringType),
    th.Property("admin4_id", th.IntegerType),
    th.Property("admin4", th.StringType),
    th.Property("postcodes", th.ArrayType(th.StringType)),
)

ELEVATION_PROPERTIES = th.PropertiesList(
    th.Property("location_name", th.StringType, required=True),
    th.Property("latitude", th.NumberType, required=True),
    th.Property("longitude", th.NumberType, required=True),
    th.Property("elevation", th.NumberType, required=True),
)


class GeocodingStream(OpenMateoStream):
    """Location search via the Open-Meteo Geocoding API.

    Not a time-series endpoint. Partitioned by search term from
    the geocoding_search_terms config. Each search term produces
    up to geocoding_count result records.
    """

    name = "geocoding"
    path = "/v1/search"
    _free_url_base = "https://geocoding-api.open-meteo.com"
    _paid_url_base = "https://geocoding-api.open-meteo.com"
    primary_keys = ("id", "search_term")
    replication_key = None
    schema = GEOCODING_PROPERTIES.to_dict()

    @property
    def partitions(self) -> list[dict] | None:
        """Generate one partition per search term."""
        search_terms = self.config.get("geocoding_search_terms", [])
        if not search_terms:
            return None
        return [{"search_term": term} for term in search_terms]

    def get_records(self, context: Context | None) -> Iterable[dict]:
        if context is None:
            return

        search_term = context["search_term"]
        params: dict[str, str] = {
            "name": search_term,
            "count": str(self.config.get("geocoding_count", 10)),
            "language": self.config.get("geocoding_language", "en"),
            "format": "json",
        }
        if api_key := self.config.get("api_key"):
            params["apikey"] = api_key

        data = self._make_request(f"{self.url_base}{self.path}", params)
        if not data or not isinstance(data, dict):
            return

        results = data.get("results", [])
        if not results:
            logging.info("Geocoding: no results for search term '%s'", search_term)
            return

        for result in results:
            result["search_term"] = search_term
            yield result


class ElevationStream(OpenMateoStream):
    """Terrain elevation lookup via the Open-Meteo Elevation API.

    Not a time-series endpoint. Uses location batching to query
    elevation for all configured locations. Returns one record
    per location with elevation in meters (Copernicus DEM 90m).
    """

    name = "elevation"
    path = "/v1/elevation"
    _free_url_base = "https://api.open-meteo.com"
    _paid_url_base = "https://customer-api.open-meteo.com"
    primary_keys = ("location_name",)
    replication_key = None
    schema = ELEVATION_PROPERTIES.to_dict()

    @property
    def partitions(self) -> list[dict]:
        return self._location_batch_partitions()

    def get_records(self, context: Context | None) -> Iterable[dict]:
        if context is None:
            return

        locations = context["locations"]
        params: dict[str, str] = {
            **self._build_location_params(locations),
        }
        if api_key := self.config.get("api_key"):
            params["apikey"] = api_key

        data = self._make_request(f"{self.url_base}{self.path}", params)
        if not data or not isinstance(data, dict):
            return

        elevations = data.get("elevation", [])
        for loc, elev in zip(locations, elevations):
            yield {
                "location_name": loc["name"],
                "latitude": loc["latitude"],
                "longitude": loc["longitude"],
                "elevation": elev,
            }
