"""OpenMateo entry point."""

from __future__ import annotations

from tap_open_mateo.tap import TapOpenMateo

TapOpenMateo.cli()
