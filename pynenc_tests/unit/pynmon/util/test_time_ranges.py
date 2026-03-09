"""
Unit tests for pynmon.util.time_ranges module.

Tests time range parsing and resolution handling utilities.
"""

from datetime import UTC, datetime, timedelta, timezone


from pynmon.util.time_ranges import (
    DEFAULT_TIME_RANGE,
    _ensure_utc,
    _parse_custom_range,
    parse_resolution,
    parse_time_range,
)


# ################################################################################### #
# PARSE_TIME_RANGE TESTS
# ################################################################################### #


def test_parse_time_range_preset_1h() -> None:
    """Test parsing 1h preset time range."""
    start, end = parse_time_range("1h")

    assert end > start
    # Should be approximately 1 hour difference
    diff = end - start
    assert timedelta(minutes=59) < diff < timedelta(minutes=61)


def test_parse_time_range_preset_1d() -> None:
    """Test parsing 1d preset time range."""
    start, end = parse_time_range("1d")

    diff = end - start
    assert timedelta(hours=23) < diff < timedelta(hours=25)


def test_parse_time_range_unknown_falls_back_to_default() -> None:
    """Test that unknown time range falls back to default (1 hour)."""
    start, end = parse_time_range("unknown_range")

    diff = end - start
    assert diff == DEFAULT_TIME_RANGE


def test_parse_time_range_custom_valid() -> None:
    """Test parsing custom time range with valid dates."""
    start_date = "2024-01-15T10:00:00"
    end_date = "2024-01-15T12:00:00"

    start, end = parse_time_range("custom", start_date, end_date)

    assert start.year == 2024
    assert start.month == 1
    assert start.day == 15
    assert start.hour == 10
    assert end.hour == 12
    assert start.tzinfo == UTC
    assert end.tzinfo == UTC


def test_parse_time_range_custom_with_timezone() -> None:
    """Test parsing custom time range with timezone-aware dates."""
    start_date = "2024-01-15T10:00:00+00:00"
    end_date = "2024-01-15T12:00:00+00:00"

    start, end = parse_time_range("custom", start_date, end_date)

    assert start.hour == 10
    assert end.hour == 12


def test_parse_time_range_custom_missing_start() -> None:
    """Test custom time range without start_date falls back to preset."""
    start, end = parse_time_range("custom", None, "2024-01-15T12:00:00")

    # Should fall back to default range since start_date is None
    diff = end - start
    assert diff == DEFAULT_TIME_RANGE


def test_parse_time_range_custom_missing_end() -> None:
    """Test custom time range without end_date falls back to preset."""
    start, end = parse_time_range("custom", "2024-01-15T10:00:00", None)

    # Should fall back to default range since end_date is None
    diff = end - start
    assert diff == DEFAULT_TIME_RANGE


# ################################################################################### #
# _PARSE_CUSTOM_RANGE TESTS
# ################################################################################### #


def test_parse_custom_range_valid_dates() -> None:
    """Test _parse_custom_range with valid ISO format dates."""
    fallback = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)

    start, end = _parse_custom_range(
        "2024-01-15T10:00:00",
        "2024-01-15T12:00:00",
        fallback,
    )

    assert start == datetime(2024, 1, 15, 10, 0, 0, tzinfo=UTC)
    assert end == datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)


def test_parse_custom_range_invalid_start_date() -> None:
    """Test _parse_custom_range falls back on invalid start date."""
    fallback = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)

    start, end = _parse_custom_range(
        "invalid-date",
        "2024-01-15T12:00:00",
        fallback,
    )

    # Should return fallback range
    assert end == fallback
    assert start == fallback - DEFAULT_TIME_RANGE


def test_parse_custom_range_invalid_end_date() -> None:
    """Test _parse_custom_range falls back on invalid end date."""
    fallback = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)

    start, end = _parse_custom_range(
        "2024-01-15T10:00:00",
        "not-a-date",
        fallback,
    )

    assert end == fallback
    assert start == fallback - DEFAULT_TIME_RANGE


def test_parse_custom_range_both_invalid() -> None:
    """Test _parse_custom_range falls back when both dates invalid."""
    fallback = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)

    start, end = _parse_custom_range(
        "garbage",
        "also-garbage",
        fallback,
    )

    assert end == fallback
    assert start == fallback - DEFAULT_TIME_RANGE


# ################################################################################### #
# _ENSURE_UTC TESTS
# ################################################################################### #


def test_ensure_utc_naive_datetime() -> None:
    """Test _ensure_utc adds UTC timezone to naive datetime."""
    naive_dt = datetime(2024, 1, 15, 10, 30, 0)
    assert naive_dt.tzinfo is None

    result = _ensure_utc(naive_dt)

    assert result.tzinfo == UTC
    assert result.year == 2024
    assert result.month == 1
    assert result.day == 15
    assert result.hour == 10
    assert result.minute == 30


def test_ensure_utc_already_utc() -> None:
    """Test _ensure_utc preserves already UTC datetime."""
    utc_dt = datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)

    result = _ensure_utc(utc_dt)

    assert result is utc_dt  # Should return same object
    assert result.tzinfo == UTC


def test_ensure_utc_other_timezone() -> None:
    """Test _ensure_utc preserves other timezone (doesn't convert)."""
    # Create a datetime with a different timezone
    other_tz = timezone(timedelta(hours=5))
    dt_with_tz = datetime(2024, 1, 15, 10, 30, 0, tzinfo=other_tz)

    result = _ensure_utc(dt_with_tz)

    # Should preserve the original timezone (not convert to UTC)
    assert result.tzinfo == other_tz
    assert result is dt_with_tz


# ################################################################################### #
# PARSE_RESOLUTION TESTS
# ################################################################################### #


def test_parse_resolution_auto() -> None:
    """Test parse_resolution returns None for auto."""
    assert parse_resolution("auto") is None


def test_parse_resolution_10s() -> None:
    """Test parse_resolution for 10 seconds."""
    assert parse_resolution("10s") == 10


def test_parse_resolution_1m() -> None:
    """Test parse_resolution for 1 minute."""
    assert parse_resolution("1m") == 60


def test_parse_resolution_1h() -> None:
    """Test parse_resolution for 1 hour."""
    assert parse_resolution("1h") == 3600


def test_parse_resolution_unknown() -> None:
    """Test parse_resolution returns None for unknown resolution."""
    assert parse_resolution("unknown") is None
