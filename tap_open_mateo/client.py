"""REST client handling, including OpenMateoStream base class."""

from __future__ import annotations

import itertools
import logging
import random
import re
import time
import typing as t
from abc import ABC
from datetime import date, datetime, timedelta

import backoff
import requests
from singer_sdk.pagination import SinglePagePaginator
from singer_sdk.streams import RESTStream

from tap_open_mateo.helpers import pivot_columnar_to_rows

if t.TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers.types import Context


class OpenMateoStream(RESTStream, ABC):
    """Base class for all Open-Meteo API streams."""

    rest_method = "GET"
    _free_url_base = "https://api.open-meteo.com"
    _paid_url_base = "https://customer-api.open-meteo.com"

    def __init__(self, tap: t.Any) -> None:
        super().__init__(tap)
        self._max_requests_per_minute = int(
            self.config.get("max_requests_per_minute", 500)
        )
        self._min_interval = float(self.config.get("min_throttle_seconds", 0.15))
        self._throttle_lock = tap._shared_throttle_lock
        self._request_timestamps = tap._shared_request_timestamps
        self._skipped_partitions: list[dict] = []
        self._schema_checked: bool = False
        self._drift_checked_locations: set[str] = set()

    # --- Properties ---

    @property
    def url_base(self) -> str:
        """Return the API URL root, selecting paid or free tier."""
        return self._paid_url_base if self.config.get("api_key") else self._free_url_base

    @property
    def authenticator(self) -> None:  # type: ignore[override]
        """Open-Meteo uses API key in query params, not headers."""
        return None

    @property
    def http_headers(self) -> dict:
        """Return HTTP headers."""
        return {"Accept": "application/json"}

    def get_new_paginator(self) -> SinglePagePaginator:
        """Open-Meteo returns full time series in one response, no pagination."""
        return SinglePagePaginator()

    def finalize_state_progress_markers(self, state: dict | None = None) -> None:
        """Log skipped-partition summary after sync completes."""
        super().finalize_state_progress_markers(state)
        if self._skipped_partitions:
            logging.error(
                "*** SKIPPED PARTITION SUMMARY *** Stream %s: %d partition(s) were skipped "
                "due to API errors. Data for these partitions is MISSING from the output: %s",
                self.name,
                len(self._skipped_partitions),
                self._skipped_partitions,
            )

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Not used — get_records handles requests directly via _make_request."""
        return []

    # --- Rate Limiting ---

    @staticmethod
    def redact_api_key(msg: str) -> str:
        """Redact API key from log messages."""
        return re.sub(r"(apikey=)[^&\s]+", r"\1<REDACTED>", msg)

    def _throttle(self) -> None:
        """Sliding window rate limiter. Thread-safe via shared lock."""
        with self._throttle_lock:
            now = time.time()
            window_start = now - 60.0

            while self._request_timestamps and self._request_timestamps[0] < window_start:
                self._request_timestamps.popleft()

            if len(self._request_timestamps) >= self._max_requests_per_minute:
                oldest_request = self._request_timestamps[0]
                wait_time = oldest_request + 60.0 - now
                if wait_time > 0:
                    logging.info(
                        "Rate limit reached (%d/min). Waiting %.1fs",
                        self._max_requests_per_minute,
                        wait_time,
                    )
                    time.sleep(wait_time + random.uniform(0.1, 0.5))
                    now = time.time()

            if self._request_timestamps:
                last_request = self._request_timestamps[-1]
                min_wait = last_request + self._min_interval - now
                if min_wait > 0:
                    time.sleep(min_wait + random.uniform(0.05, 0.15))
                    now = time.time()

            self._request_timestamps.append(now)

    # --- HTTP Request ---

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException,),
        base=5,
        max_value=300,
        jitter=backoff.full_jitter,
        max_tries=5,
        max_time=120,
        giveup=lambda e: (
            isinstance(e, requests.exceptions.HTTPError)
            and e.response is not None
            and (
                400 <= e.response.status_code <= 599
                and e.response.status_code not in {429, 500, 502, 503, 504}
            )
        ),
        on_backoff=lambda details: logging.warning(
            "API request failed, retrying in %.1fs (attempt %d): %s",
            details["wait"],
            details["tries"],
            details["exception"],
        ),
    )
    def _make_request(self, url: str, params: dict) -> dict | list:
        """Centralized request handler with backoff, throttling, and error handling."""
        log_params = {k: ("<REDACTED>" if k == "apikey" else v) for k, v in params.items()}
        logging.info("Stream %s: Requesting %s with params: %s", self.name, self.redact_api_key(url), log_params)

        try:
            self._throttle()
            response = self.requests_session.get(url, params=params, timeout=(20, 60))
            response.raise_for_status()
            data = response.json()

            if isinstance(data, dict) and data.get("error"):
                error_msg = f"Open-Meteo API Error: {data.get('reason', 'Unknown error')}"
                logging.error(error_msg)
                raise requests.exceptions.HTTPError(error_msg, response=response)

            return data

        except requests.exceptions.RequestException as e:
            redacted_url = self.redact_api_key(str(e.request.url if e.request else url))

            if isinstance(e, (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout, requests.exceptions.Timeout)):
                logging.warning("Request timeout for %s, will retry", redacted_url)
                raise

            if isinstance(e, requests.exceptions.HTTPError) and e.response is not None:
                status_code = e.response.status_code

                if status_code >= 500:
                    logging.warning("Server error %d for %s, will retry", status_code, redacted_url)
                    raise

                if 400 <= status_code < 500 and status_code != 429:
                    response_body = e.response.text[:200] if e.response.text else "no body"
                    logging.error(
                        "SKIPPED PARTITION — Client error %d for %s: %s. "
                        "Data for this partition will be missing from output.",
                        status_code, redacted_url, response_body,
                    )
                    self._skipped_partitions.append({"stream": self.name, "url": redacted_url, "status_code": status_code})
                    if self.config.get("strict_mode", False):
                        raise
                    return {}

            raise

    # --- Param Builders ---

    def _build_base_params(self) -> dict:
        """Build common query parameters shared by all endpoints."""
        params: dict[str, str] = {
            "temperature_unit": self.config.get("temperature_unit", "fahrenheit"),
            "wind_speed_unit": self.config.get("wind_speed_unit", "mph"),
            "precipitation_unit": self.config.get("precipitation_unit", "inch"),
            "timezone": self.config.get("timezone", "America/Chicago"),
            "timeformat": "iso8601",
        }
        self._apply_optional_params(params)
        return params

    def _apply_optional_params(self, params: dict) -> None:
        """Add cell_selection and apikey to params if configured."""
        if cell_selection := self.config.get("cell_selection"):
            params["cell_selection"] = cell_selection
        if api_key := self.config.get("api_key"):
            params["apikey"] = api_key

    def _build_location_params(self, locations: list[dict]) -> dict:
        """Build latitude/longitude params for a batch of locations."""
        return {
            "latitude": ",".join(str(loc["latitude"]) for loc in locations),
            "longitude": ",".join(str(loc["longitude"]) for loc in locations),
        }

    # --- Partitioning Helpers ---

    def _batch_locations(self, locations: list[dict], batch_size: int) -> list[list[dict]]:
        """Split locations into batches for multi-location requests."""
        return [locations[i : i + batch_size] for i in range(0, len(locations), batch_size)]

    def _location_batch_partitions(self) -> list[dict]:
        """Generate partitions by location batch only (no date chunking)."""
        locations = self.get_resolved_locations()
        batch_size = self.config.get("max_locations_per_request", 10)
        batches = self._batch_locations(locations, batch_size)
        return [{"batch_idx": idx, "locations": batch} for idx, batch in enumerate(batches)]

    def _parse_config_date(self, key: str, default: str | None = None) -> date | None:
        """Parse a date string from config, returning a date object or None."""
        val = self.config.get(key, default)
        if val is None:
            return None
        return datetime.strptime(val, "%Y-%m-%d").date() if isinstance(val, str) else val

    def _get_chunk_ranges(self, start_date: date, end_date: date, chunk_days: int) -> list[tuple[date, date]]:
        """Generate date chunk ranges for historical/climate partitioning."""
        chunks = []
        current = start_date
        while current <= end_date:
            chunk_end = min(current + timedelta(days=chunk_days - 1), end_date)
            chunks.append((current, chunk_end))
            current = chunk_end + timedelta(days=1)
        return chunks

    _default_end_date_lag_days: int = 1

    def _date_chunked_partitions(
        self,
        start_key: str,
        end_key: str,
        chunk_days_key: str = "historical_chunk_days",
        default_chunk_days: int = 365,
        chunk_multiplier: int = 1,
    ) -> list[dict]:
        """Generate partitions by (location_batch, date_chunk).

        Used by historical and climate streams to split large date ranges.
        """
        start = self._parse_config_date(start_key)
        if not start:
            return []

        end = self._parse_config_date(end_key) or (
            date.today() - timedelta(days=self._default_end_date_lag_days)
        )
        chunk_days = self.config.get(chunk_days_key, default_chunk_days) * chunk_multiplier
        chunks = self._get_chunk_ranges(start, end, chunk_days)

        batches = self._batch_locations(
            self.get_resolved_locations(),
            self.config.get("max_locations_per_request", 10),
        )

        return [
            {
                "batch_idx": batch_idx,
                "locations": batch,
                "start_date": chunk_start.isoformat(),
                "end_date": chunk_end.isoformat(),
            }
            for (batch_idx, batch), (chunk_start, chunk_end)
            in itertools.product(enumerate(batches), chunks)
        ]

    # --- Response Extraction ---

    def _validate_response_section(self, data: dict, section_key: str) -> dict | None:
        """Validate that a response contains the expected data section."""
        section = data.get(section_key)
        if not section:
            logging.warning("Stream %s: Response missing '%s' section", self.name, section_key)
            return None

        times = section.get("time", [])
        if not times:
            logging.warning("Stream %s: Empty time array in '%s' section", self.name, section_key)
            return None

        n_times = len(times)
        mismatches = {
            k: len(v)
            for k, v in section.items()
            if k != "time" and isinstance(v, list) and len(v) != n_times
        }
        if mismatches:
            logging.warning("Stream %s: Variables with mismatched lengths (expected %d): %s", self.name, n_times, mismatches)

        return section

    # Maximum allowed coordinate drift between requested and returned location (degrees).
    # ~0.25 deg ≈ 28 km — beyond this the API is returning a substantially different grid cell.
    _max_coordinate_drift_deg: float = 0.25

    def _check_coordinate_drift(self, data: dict, location: dict) -> None:
        """Warn if API returned coordinates far from what we requested. Checked once per location."""
        loc_key = location["name"]
        if loc_key in self._drift_checked_locations:
            return
        self._drift_checked_locations.add(loc_key)

        api_lat = data.get("latitude")
        api_lon = data.get("longitude")
        if api_lat is None or api_lon is None:
            return

        lat_drift = abs(float(api_lat) - float(location["latitude"]))
        lon_drift = abs(float(api_lon) - float(location["longitude"]))
        if lat_drift > self._max_coordinate_drift_deg or lon_drift > self._max_coordinate_drift_deg:
            logging.warning(
                "Stream %s: Location '%s' coordinate drift — "
                "requested (%.4f, %.4f) but API returned (%.4f, %.4f). "
                "Drift: %.4f lat, %.4f lon. Consider adjusting coordinates or cell_selection.",
                self.name, location["name"],
                location["latitude"], location["longitude"],
                api_lat, api_lon, lat_drift, lon_drift,
            )

    def _extract_single_location(self, data: dict, location: dict, granularity: str, model: str) -> list[dict]:
        """Extract records from a single-location response."""
        self._check_coordinate_drift(data, location)
        section = self._validate_response_section(data, granularity)
        if not section:
            return []

        return pivot_columnar_to_rows(
            section_data=section,
            times=section.get("time", []),
            location_name=location["name"],
            latitude=data.get("latitude", location["latitude"]),
            longitude=data.get("longitude", location["longitude"]),
            elevation=data.get("elevation"),
            model=model,
            granularity=granularity,
        )

    def _extract_multi_location(self, data_list: list[dict], locations: list[dict], granularity: str, model: str) -> list[dict]:
        """Extract records from a multi-location response (array of objects)."""
        if len(data_list) > len(locations):
            logging.warning("Stream %s: More response items (%d) than locations (%d)", self.name, len(data_list), len(locations))

        return [
            record
            for loc_data, loc in zip(data_list, locations)
            for record in self._extract_single_location(loc_data, loc, granularity, model)
        ]

    def _fetch_and_extract(
        self, locations: list[dict], granularity: str, model: str, extra_params: dict
    ) -> list[dict]:
        """Fetch data from API and extract records. Handles single/multi-location."""
        params = {**self._build_base_params(), **self._build_location_params(locations), **extra_params}
        if model != "best_match":
            params["models"] = model

        data = self._make_request(f"{self.url_base}{self.path}", params)
        if not data:
            logging.warning(
                "Stream %s: Empty response from API, no records extracted for this request.",
                self.name,
            )
            return []

        if isinstance(data, list):
            return self._extract_multi_location(data, locations, granularity, model)
        return self._extract_single_location(data, locations[0], granularity, model)

    # --- Post Processing ---

    def _check_missing_fields(self, row: dict) -> None:
        """Log CRITICAL if API returns fields not in our schema (silent data loss).

        Follows tap-massive pattern: check once per stream, not every record.
        """
        if self._schema_checked:
            return
        self._schema_checked = True

        schema_fields = set(self.schema.get("properties", {}).keys())
        record_fields = set(row.keys())

        missing_in_schema = record_fields - schema_fields
        if missing_in_schema:
            logging.critical(
                "*** SCHEMA GAP *** Stream %s: API returned %d fields NOT in schema "
                "(these will be SILENTLY DROPPED): %s",
                self.name,
                len(missing_in_schema),
                sorted(missing_in_schema),
            )

        missing_in_record = schema_fields - record_fields
        if missing_in_record and len(missing_in_record) <= 20:
            logging.debug(
                "Stream %s: %d schema fields not in record (expected for optional fields): %s",
                self.name,
                len(missing_in_record),
                sorted(missing_in_record),
            )

    def post_process(self, row: dict, context: Context | None = None) -> dict | None:
        """Validate schema coverage for each record."""
        self._check_missing_fields(row)
        return row

    def get_resolved_locations(self) -> list[dict]:
        """Get resolved locations from tap's cached location data."""
        return self._tap.get_resolved_locations()  # type: ignore[attr-defined]
