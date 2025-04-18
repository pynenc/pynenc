"""
Tests for CronCondition with execution time tracking.

This module verifies the behavior of CronCondition, particularly its
ability to respect previous execution times when determining if it
should trigger again.
"""

from datetime import datetime

from pynenc.trigger.conditions.cron import CronCondition, CronContext


def test_init() -> None:
    """Test initialization of CronCondition."""
    condition = CronCondition("0 12 * * *")
    assert condition.cron_expression == "0 12 * * *"


def test_condition_id() -> None:
    """Test condition_id generation."""
    # Same cron expression should have same ID
    condition1 = CronCondition("0 12 * * *")
    condition2 = CronCondition("0 12 * * *")
    assert condition1.condition_id == condition2.condition_id

    # Different cron expressions should have different IDs
    condition3 = CronCondition("0 0 * * *")
    assert condition1.condition_id != condition3.condition_id


def test_every_minute_cron_current_time() -> None:
    """Test that a cron expression for every minute matches the current time."""
    condition = CronCondition("* * * * *")
    context = CronContext()
    assert condition.is_satisfied_by(context)


def test_every_minute_cron_specific_time() -> None:
    """Test that a cron expression for every minute matches a specific time."""
    condition = CronCondition("* * * * *")
    timestamp = datetime(2023, 1, 1, 12, 0, 0)
    context = CronContext(timestamp=timestamp)
    assert condition.is_satisfied_by(context)


def test_is_satisfied_by_exact_match() -> None:
    """Test is_satisfied_by with time that exactly matches schedule."""
    # Create condition for noon every day
    condition = CronCondition("0 12 * * *")

    # Create context with exact match time
    context = CronContext(timestamp=datetime(2023, 1, 1, 12, 0, 0))

    # Context time exactly matches the schedule
    assert condition.is_satisfied_by(context)


def test_is_satisfied_by_within_window() -> None:
    """Test is_satisfied_by with time within check window."""
    # Create condition for noon every day
    condition = CronCondition("0 12 * * *")

    # Create context with time 30 seconds after scheduled time (within 60s window)
    context = CronContext(timestamp=datetime(2023, 1, 1, 12, 0, 30))

    # Should be satisfied as it's within the default 60-second window
    assert condition.is_satisfied_by(context)


def test_is_satisfied_by_outside_window() -> None:
    """Test is_satisfied_by with time outside check window."""
    # Create condition for noon every day
    condition = CronCondition("0 12 * * *")

    # Create context with time 90 seconds after scheduled time (outside 60s window)
    context = CronContext(timestamp=datetime(2023, 1, 1, 12, 1, 30))

    # Should not be satisfied as it's outside the default 60-second window
    assert not condition.is_satisfied_by(context)


def test_is_satisfied_by_custom_window() -> None:
    """Test is_satisfied_by with custom check window."""
    # Create condition for noon every day
    condition = CronCondition(
        "0 12 * * *",
        check_window_seconds=120,
    )

    # Create context with time 90 seconds after scheduled time but a 120s window
    context = CronContext(
        timestamp=datetime(2023, 1, 1, 12, 1, 30),
    )

    # Should be satisfied as it's within the custom 120-second window
    assert condition.is_satisfied_by(context)


def test_is_satisfied_by_different_day() -> None:
    """Test is_satisfied_by with different day but same time."""
    # Create condition for noon every day
    condition = CronCondition("0 12 * * *")

    # Create context with time on a different day but at noon
    context = CronContext(timestamp=datetime(2023, 1, 2, 12, 0, 0))

    # Should be satisfied as cron runs every day at noon
    assert condition.is_satisfied_by(context)


def test_is_satisfied_by_different_time() -> None:
    """Test is_satisfied_by with different time on same day."""
    # Create condition for noon every day
    condition = CronCondition("0 12 * * *")

    # Create context with time on the same day but at a different hour
    context = CronContext(timestamp=datetime(2023, 1, 1, 13, 0, 0))

    # Should not be satisfied as time doesn't match the schedule
    assert not condition.is_satisfied_by(context)


def test_is_satisfied_by_with_last_execution_too_recent() -> None:
    """Test that condition respects last_execution and doesn't trigger if too recent."""
    condition = CronCondition("*/5 * * * *")  # Every 5 minutes

    # Current time: 12:03
    current_time = datetime(2023, 1, 1, 12, 3, 0)

    # Last execution: 12:00 (only 3 minutes ago)
    last_execution = datetime(2023, 1, 1, 12, 0, 0)

    context = CronContext(timestamp=current_time, last_execution=last_execution)

    # Should not trigger yet since 5 minutes haven't elapsed
    assert not condition.is_satisfied_by(context)


def test_is_satisfied_by_with_last_execution_due() -> None:
    """Test that condition triggers when enough time has passed since last execution."""
    condition = CronCondition("*/5 * * * *")  # Every 5 minutes

    # Current time: 12:05
    current_time = datetime(2023, 1, 1, 12, 5, 0)

    # Last execution: 12:00 (5 minutes ago)
    last_execution = datetime(2023, 1, 1, 12, 0, 0)

    context = CronContext(timestamp=current_time, last_execution=last_execution)

    # Should trigger since 5 minutes have elapsed
    assert condition.is_satisfied_by(context)


def test_is_satisfied_by_with_last_execution_overdue() -> None:
    """Test that condition triggers when more than enough time has passed."""
    condition = CronCondition("*/5 * * * *")  # Every 5 minutes

    # Current time: 12:10
    current_time = datetime(2023, 1, 1, 12, 10, 0)

    # Last execution: 12:00 (10 minutes ago)
    last_execution = datetime(2023, 1, 1, 12, 0, 0)

    context = CronContext(timestamp=current_time, last_execution=last_execution)

    # Should trigger since more than 5 minutes have elapsed
    assert condition.is_satisfied_by(context)


def test_is_satisfied_by_no_last_execution() -> None:
    """Test that condition triggers when there's no last execution time."""
    condition = CronCondition("*/5 * * * *")  # Every 5 minutes

    # Current time: 12:05
    current_time = datetime(2023, 1, 1, 12, 5, 0)

    # No last execution
    context = CronContext(timestamp=current_time)

    # Should trigger since there's no last execution
    assert condition.is_satisfied_by(context)


def test_is_satisfied_by_with_last_execution_non_matching_time() -> None:
    """Test that condition checks both schedule and last execution time."""
    condition = CronCondition("0 * * * *")  # Every hour at minute 0

    # Current time: 12:30 (doesn't match the cron schedule)
    current_time = datetime(2023, 1, 1, 12, 30, 0)

    # Last execution: 12:00 (30 minutes ago)
    last_execution = datetime(2023, 1, 1, 12, 0, 0)

    context = CronContext(timestamp=current_time, last_execution=last_execution)

    # Should not trigger since current time doesn't match the cron schedule,
    # even though enough time has passed since last execution
    assert not condition.is_satisfied_by(context)


def test_affects_task() -> None:
    """Test affects_task method - should always return False."""
    condition = CronCondition("0 12 * * *")

    # Time-based conditions don't specifically affect any task
    assert not condition.affects_task("any_task")
