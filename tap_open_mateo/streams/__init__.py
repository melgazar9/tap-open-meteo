"""Stream type classes for tap-open-mateo."""

from tap_open_mateo.streams.climate_streams import ClimateDailyStream
from tap_open_mateo.streams.ensemble_streams import EnsembleHourlyStream
from tap_open_mateo.streams.forecast_streams import (
    ForecastDailyStream,
    ForecastHourlyStream,
    PreviousRunsHourlyStream,
)
from tap_open_mateo.streams.historical_streams import (
    HistoricalDailyStream,
    HistoricalHourlyStream,
)

__all__ = [
    "ClimateDailyStream",
    "EnsembleHourlyStream",
    "ForecastDailyStream",
    "ForecastHourlyStream",
    "HistoricalDailyStream",
    "HistoricalHourlyStream",
    "PreviousRunsHourlyStream",
]
