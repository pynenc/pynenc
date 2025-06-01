"""
Context for time-based conditions like cron schedules.

This module provides time-based trigger conditions, including cron schedule triggers,
that allow tasks to be executed at specific times or intervals.
"""

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, ClassVar

from croniter import croniter  # type: ignore[import]

from pynenc.trigger.conditions import ConditionContext
from pynenc.trigger.conditions.base import TriggerCondition

# Default timing constants for cron conditions
DEFAULT_CHECK_WINDOW_SECONDS = 60
DEFAULT_MIN_INTERVAL_SECONDS = 50
DEFAULT_PRECISION_TOLERANCE_SECONDS = 30
DEFAULT_STRICT_TIMING = False

if TYPE_CHECKING:
    from ...app import Pynenc


class CronContext(ConditionContext):
    """
    Context for time-based conditions like cron schedules.

    This class provides the context needed for evaluating time-based trigger conditions,
    including a check window in seconds and optional last execution time tracking.
    """

    def __init__(
        self,
        *,
        timestamp: datetime | None = None,
        last_execution: datetime | None = None,
    ) -> None:
        """
        Create a time context with custom check window, timestamp, and optional last execution.

        :param check_window_seconds: Duration in seconds to consider a match after scheduled time
        :param timestamp: Specific timestamp to use (primarily for testing), defaults to current time
        :param last_execution: Timestamp of the previous execution, or None if never executed
        """
        super().__init__()
        self.last_execution = last_execution
        self.timestamp = timestamp if timestamp else datetime.now(timezone.utc)

    @property
    def context_id(self) -> str:
        return f"cron_{self.timestamp.isoformat()}"

    def _to_json(self, app: "Pynenc") -> dict[str, Any]:
        """
        Create a serializable representation of this time context.

        :param app: Pynenc application instance
        :return: Dictionary with serialized context data
        """
        data: dict = {}
        if self.last_execution:
            data["last_execution"] = self.last_execution.isoformat()
        return data

    @classmethod
    def _from_json(cls, data: dict[str, Any], app: "Pynenc") -> "CronContext":
        """
        Create a CronContext from parsed JSON data.

        :param data: Dictionary with context data
        :param app: Pynenc application instance
        :return: A new CronContext instance
        """
        last_execution = None
        if last_execution_str := data.get("last_execution"):
            try:
                last_execution = datetime.fromisoformat(last_execution_str)
            except ValueError:
                app.logger.error(
                    f"Invalid last_execution date format: {last_execution_str}"
                )

        return cls(last_execution=last_execution)


class CronCondition(TriggerCondition[CronContext]):
    """
    Condition based on a cron schedule.

    Triggers a task at times matching a specified cron expression.
    """

    context_type: ClassVar[type[CronContext]] = CronContext

    def __init__(
        self,
        cron_expression: str,
        check_window_seconds: int = DEFAULT_CHECK_WINDOW_SECONDS,
        min_interval_seconds: int = DEFAULT_MIN_INTERVAL_SECONDS,
        precision_tolerance_seconds: int = DEFAULT_PRECISION_TOLERANCE_SECONDS,
        strict_timing: bool = DEFAULT_STRICT_TIMING,
    ) -> None:
        """
        Create a cron-based trigger condition.

        :param cron_expression: Standard cron expression (e.g., "0 0 * * *" for daily at midnight)
        :param check_window_seconds: Check window in seconds after scheduled time (default: 60)
        :param min_interval_seconds: Minimum interval between executions (default: 50)
        :param precision_tolerance_seconds: Precision tolerance for strict timing (default: 30)
        :param strict_timing: Whether to enforce strict timing mode (default: False)
        :raises ValueError: If the cron expression is invalid
        """
        self.cron_expression = cron_expression
        self.check_window_seconds = check_window_seconds
        self.min_interval_seconds = min_interval_seconds
        self.precision_tolerance_seconds = precision_tolerance_seconds
        self.strict_timing = strict_timing
        self._validate_expression()

    def _validate_expression(self) -> None:
        """Validate the cron expression."""
        try:
            croniter(self.cron_expression)
        except ValueError as e:
            raise ValueError(f"Invalid cron expression: {self.cron_expression}") from e

        fields = self.cron_expression.split()
        if len(fields) == 6:
            raise ValueError(
                "Cron expressions with seconds precision (6 fields) are not supported. "
                "Use minute-level precision (5 fields) instead."
            )

    @property
    def condition_id(self) -> str:
        """
        Generate a unique ID for this cron condition.

        :return: A string ID based on the cron expression
        """
        return f"cron_{self.cron_expression}"

    def get_source_task_ids(self) -> set[str]:
        return set()

    def _to_json(self, app: "Pynenc") -> dict[str, Any]:
        """
        Create a serializable representation of this condition.

        :param app: Pynenc application instance
        :return: Dictionary with serialized condition data
        """
        return {
            "cron_expression": self.cron_expression,
            "check_window_seconds": self.check_window_seconds,
            "min_interval_seconds": self.min_interval_seconds,
            "precision_tolerance_seconds": self.precision_tolerance_seconds,
            "strict_timing": self.strict_timing,
        }

    @classmethod
    def _from_json(cls, data: dict[str, Any], app: "Pynenc") -> "CronCondition":
        """
        Create a CronCondition from parsed JSON data.

        :param data: Dictionary with condition data
        :param app: Pynenc application instance
        :return: A new CronCondition instance
        :raises ValueError: If the data is invalid for this condition type
        """
        cron_expression = data.get("cron_expression")
        if not cron_expression:
            raise ValueError("Missing required cron_expression in CronCondition data")

        return cls(
            cron_expression=cron_expression,
            check_window_seconds=data.get(
                "check_window_seconds", DEFAULT_CHECK_WINDOW_SECONDS
            ),
            min_interval_seconds=data.get(
                "min_interval_seconds", DEFAULT_MIN_INTERVAL_SECONDS
            ),
            precision_tolerance_seconds=data.get(
                "precision_tolerance_seconds", DEFAULT_PRECISION_TOLERANCE_SECONDS
            ),
            strict_timing=data.get("strict_timing", DEFAULT_STRICT_TIMING),
        )

    def _is_satisfied_by(self, context: CronContext) -> bool:
        """
        Check if the current time matches the cron schedule with flexible timing.

        This method implements configurable timing behavior:
        - Uses check_window_seconds for the maximum time after scheduled execution
        - Respects min_interval_seconds to prevent too-frequent executions
        - Applies precision_tolerance_seconds for high-precision matching
        - Honors strict_timing mode when enabled

        :param context: Time context with timestamp and optional last execution
        :return: True if the timestamp satisfies the cron condition
        """
        # Check minimum interval constraint if we have last execution
        if context.last_execution:
            time_since_last = (
                context.timestamp - context.last_execution
            ).total_seconds()
            if time_since_last < self.min_interval_seconds:
                return False

            # Also check if enough time has passed according to the cron schedule
            next_after_last = croniter(
                self.cron_expression, context.last_execution
            ).get_next(datetime)

            if context.timestamp < next_after_last:
                return False

        # Get the most recent scheduled time at or before the current timestamp
        cron = croniter(self.cron_expression, context.timestamp)

        # Check if current timestamp exactly matches a scheduled time
        if croniter.match(self.cron_expression, context.timestamp):
            # Exact match - time difference is 0
            time_diff_seconds = 0.0
        else:
            # Get previous scheduled time and calculate difference
            prev_time = cron.get_prev(datetime)
            time_diff_seconds = (context.timestamp - prev_time).total_seconds()

        # Check if we're within the allowed time window
        if not (0 <= time_diff_seconds <= self.check_window_seconds):
            return False

        # Apply strict timing if enabled
        if self.strict_timing and time_diff_seconds > self.precision_tolerance_seconds:
            return False

        # All checks passed
        return True

    def _time_components_match(self, time1: datetime, time2: datetime) -> bool:
        """
        Check if time components match based on cron expression fields.

        This handles special cases when the exact timestamp matches the cron schedule.

        :param time1: First timestamp to compare
        :param time2: Second timestamp to compare
        :return: True if relevant components match according to cron field count
        """
        field_count = len(self.cron_expression.split())

        # 5-field standard cron (minute, hour, day, month, weekday)
        if field_count == 5:
            return (
                time1.minute == time2.minute
                and time1.hour == time2.hour
                and time1.day == time2.day
                and time1.month == time2.month
            )

        # 6-field cron with seconds
        elif field_count == 6:
            return (
                time1.second == time2.second
                and time1.minute == time2.minute
                and time1.hour == time2.hour
                and time1.day == time2.day
                and time1.month == time2.month
            )

        # Default case
        return False
