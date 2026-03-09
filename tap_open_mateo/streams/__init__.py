"""Stream type classes for tap-open-mateo."""

from tap_open_mateo.streams.air_quality_streams import AirQualityHourlyStream
from tap_open_mateo.streams.climate_streams import ClimateDailyStream
from tap_open_mateo.streams.ensemble_streams import EnsembleHourlyStream
from tap_open_mateo.streams.flood_streams import FloodDailyStream
from tap_open_mateo.streams.forecast_streams import (
    ForecastDailyStream,
    ForecastHourlyStream,
    PreviousRunsHourlyStream,
)
from tap_open_mateo.streams.historical_forecast_streams import (
    HistoricalForecastDailyStream,
    HistoricalForecastHourlyStream,
)
from tap_open_mateo.streams.historical_streams import (
    HistoricalDailyStream,
    HistoricalHourlyStream,
)
from tap_open_mateo.streams.marine_streams import MarineDailyStream, MarineHourlyStream
from tap_open_mateo.streams.satellite_streams import (
    SatelliteRadiationDailyStream,
    SatelliteRadiationHourlyStream,
)
from tap_open_mateo.streams.seasonal_streams import (
    SeasonalDailyStream,
    SeasonalSixHourlyStream,
)
from tap_open_mateo.streams.utility_streams import ElevationStream, GeocodingStream

__all__ = [
    "AirQualityHourlyStream",
    "ClimateDailyStream",
    "ElevationStream",
    "EnsembleHourlyStream",
    "FloodDailyStream",
    "ForecastDailyStream",
    "ForecastHourlyStream",
    "GeocodingStream",
    "HistoricalDailyStream",
    "HistoricalForecastDailyStream",
    "HistoricalForecastHourlyStream",
    "HistoricalHourlyStream",
    "MarineDailyStream",
    "MarineHourlyStream",
    "PreviousRunsHourlyStream",
    "SatelliteRadiationDailyStream",
    "SatelliteRadiationHourlyStream",
    "SeasonalDailyStream",
    "SeasonalSixHourlyStream",
]
