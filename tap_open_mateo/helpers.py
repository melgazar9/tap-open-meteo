"""Helper functions for tap-open-mateo."""

from __future__ import annotations

import re


def clean_strings(string: str) -> str:
    """Clean strings to snake_case format."""
    if not string:
        return string

    # Replace camelCase and PascalCase with snake_case
    string = re.sub(r"(?<!^)(?=[A-Z])", "_", string).lower()

    # Replace any non-alphanumeric characters with underscores
    string = re.sub(r"[^a-zA-Z0-9_]", "_", string)

    # Remove consecutive underscores
    string = re.sub(r"_+", "_", string)

    # Remove leading/trailing underscores
    return string.strip("_")


def clean_json_keys(data: object) -> object:
    """Recursively clean JSON keys to snake_case."""
    if isinstance(data, dict):
        return {clean_strings(key): clean_json_keys(value) for key, value in data.items()}
    if isinstance(data, list):
        return [clean_json_keys(item) for item in data]
    return data


def _extract_var_value(section_data: dict, col_key: str, idx: int) -> object:
    """Safely extract a value from a columnar array at a given index."""
    values = section_data.get(col_key)
    return values[idx] if values and idx < len(values) else None


def pivot_columnar_to_rows(
    section_data: dict,
    times: list[str],
    location_name: str,
    latitude: float,
    longitude: float,
    elevation: float | None,
    model: str,
    granularity: str,
) -> list[dict]:
    """Convert Open-Meteo columnar format to row-based records.

    Input:  {"time": [t1, t2], "temp": [v1, v2], "wind": [w1, w2]}
    Output: [{"time": t1, "temp": v1, "wind": w1}, {"time": t2, "temp": v2, "wind": w2}]
    """
    variables = [k for k in section_data if k != "time"]
    base = {
        "location_name": location_name,
        "latitude": latitude,
        "longitude": longitude,
        "elevation": elevation,
        "model": model,
        "granularity": granularity,
    }

    return [
        {
            **base,
            "time": time_val,
            **{var: _extract_var_value(section_data, var, i) for var in variables},
        }
        for i, time_val in enumerate(times)
    ]


def pivot_ensemble_to_rows(
    section_data: dict,
    times: list[str],
    base_variables: list[str],
    location_name: str,
    latitude: float,
    longitude: float,
    elevation: float | None,
    model: str,
    granularity: str = "hourly",
) -> list[dict]:
    """Convert ensemble columnar response to normalized member rows.

    Detects _memberNN suffixes, extracts member number, emits one row per (time, member).
    Control run (no suffix) gets member=0.
    """
    member_pattern = re.compile(r"^(.+)_member(\d+)$")
    members_found = {0} | {
        int(m.group(2))
        for key in section_data
        if key != "time" and (m := member_pattern.match(key))
    }
    sorted_members = sorted(members_found)

    base = {
        "location_name": location_name,
        "latitude": latitude,
        "longitude": longitude,
        "elevation": elevation,
        "model": model,
        "granularity": granularity,
    }

    def _col_key(var: str, member_num: int) -> str:
        return var if member_num == 0 else f"{var}_member{member_num:02d}"

    return [
        {
            **base,
            "member": member_num,
            "time": time_val,
            **{var: _extract_var_value(section_data, _col_key(var, member_num), i) for var in base_variables},
        }
        for i, time_val in enumerate(times)
        for member_num in sorted_members
    ]


def pivot_previous_runs_to_rows(
    section_data: dict,
    times: list[str],
    base_variables: list[str],
    previous_run_days: list[int],
    location_name: str,
    latitude: float,
    longitude: float,
    elevation: float | None,
    model: str,
) -> list[dict]:
    """Convert previous runs columnar response to normalized rows.

    Detects _previous_dayN suffixes, extracts offset, emits one row per (time, offset).
    Offset 0 = current/latest forecast.
    """
    all_offsets = sorted({0, *previous_run_days})

    base = {
        "location_name": location_name,
        "latitude": latitude,
        "longitude": longitude,
        "elevation": elevation,
        "model": model,
        "granularity": "hourly",
    }

    def _col_key(var: str, offset: int) -> str:
        return var if offset == 0 else f"{var}_previous_day{offset}"

    return [
        {
            **base,
            "model_run_offset_days": offset,
            "time": time_val,
            **{var: _extract_var_value(section_data, _col_key(var, offset), i) for var in base_variables},
        }
        for i, time_val in enumerate(times)
        for offset in all_offsets
    ]
