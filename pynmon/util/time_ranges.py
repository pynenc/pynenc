"""
Time range utilities for pynmon views.

Provides time range parsing and resolution handling for timeline views.
"""

from datetime import UTC, datetime, timedelta

# Mapping of time range strings to timedelta values
TIME_RANGE_MAP: dict[str, timedelta] = {
    "1m": timedelta(minutes=1),
    "5m": timedelta(minutes=5),
    "15m": timedelta(minutes=15),
    "1h": timedelta(hours=1),
    "3h": timedelta(hours=3),
    "12h": timedelta(hours=12),
    "1d": timedelta(days=1),
    "3d": timedelta(days=3),
    "1w": timedelta(weeks=1),
}

# Mapping of resolution strings to seconds
RESOLUTION_MAP: dict[str, int | None] = {
    "auto": None,
    "10s": 10,
    "30s": 30,
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
}

# Default time range if none specified
DEFAULT_TIME_RANGE = timedelta(hours=1)


def parse_time_range(
    time_range: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> tuple[datetime, datetime]:
    """
    Parse time range parameters into start and end datetime objects.

    :param time_range: Preset range like "1h", "1d" or "custom"
    :param start_date: Custom start date (ISO format) if time_range is "custom"
    :param end_date: Custom end date (ISO format) if time_range is "custom"
    :return: Tuple of (start_datetime, end_datetime) in UTC
    """
    now = datetime.now(UTC)

    if time_range == "custom" and start_date and end_date:
        return _parse_custom_range(start_date, end_date, now)

    delta = TIME_RANGE_MAP.get(time_range, DEFAULT_TIME_RANGE)
    return now - delta, now


def _parse_custom_range(
    start_date: str,
    end_date: str,
    fallback_now: datetime,
) -> tuple[datetime, datetime]:
    """Parse custom date range with fallback on error."""
    try:
        start = _ensure_utc(datetime.fromisoformat(start_date))
        end = _ensure_utc(datetime.fromisoformat(end_date))
        return start, end
    except ValueError:
        # Fall back to last hour on parse error
        return fallback_now - DEFAULT_TIME_RANGE, fallback_now


def _ensure_utc(dt: datetime) -> datetime:
    """Ensure datetime has UTC timezone."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


def parse_resolution(resolution: str) -> int | None:
    """
    Parse resolution string to seconds.

    :param resolution: Resolution string like "auto", "10s", "1m"
    :return: Resolution in seconds, or None for auto
    """
    return RESOLUTION_MAP.get(resolution)
