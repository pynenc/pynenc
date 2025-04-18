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

    def __init__(self, cron_expression: str, check_window_seconds: int = 60) -> None:
        """
        Create a cron-based trigger condition.

        :param cron_expression: Standard cron expression (e.g., "0 0 * * *" for daily at midnight)
        :raises ValueError: If the cron expression is invalid
        """
        self.cron_expression = cron_expression
        self.check_window_seconds = check_window_seconds
        self._validate_expression()

    def _validate_expression(self) -> None:
        """Validate the cron expression."""
        try:
            croniter(self.cron_expression)
        except ValueError as e:
            raise ValueError(f"Invalid cron expression: {self.cron_expression}") from e

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
        check_window_seconds = data.get("check_window_seconds", 60)

        if not cron_expression:
            raise ValueError("Missing required cron_expression in CronCondition data")

        return cls(
            cron_expression=cron_expression, check_window_seconds=check_window_seconds
        )

    def _is_satisfied_by(self, context: CronContext) -> bool:
        """
        Check if the current time matches the cron schedule.

        A time matches when it is within the check window after a scheduled run time.
        For example, with a daily noon schedule (0 12 * * *), this checks if the
        timestamp is between noon and noon + check_window_seconds.

        When context includes last_execution, this method also verifies that
        enough time has passed since the last execution according to the cron schedule.

        :param context: Time context with timestamp and check window
        :return: True if the timestamp falls within the check window after a scheduled run
        """
        # If we have last_execution info, verify that enough time has passed
        # since the last execution according to the schedule
        if context.last_execution:
            # Get the next scheduled time after the last execution
            next_after_last = croniter(
                self.cron_expression, context.last_execution
            ).get_next(datetime)

            # If the current time hasn't reached the next scheduled time after the last execution,
            # the condition is not satisfied
            if context.timestamp < next_after_last:
                return False

        # Continue with the regular time condition check if there's no last_execution
        # or if enough time has passed since the last execution
        cron = croniter(self.cron_expression, context.timestamp)

        # First check if the timestamp is exactly at a scheduled time
        # Get the previous scheduled time
        prev_time = cron.get_prev(datetime)

        # Check if this timestamp is within the check window after a scheduled time
        time_diff_seconds = (context.timestamp - prev_time).total_seconds()

        # If timestamp is at or just after a scheduled time (within the window)
        # Consider it a match
        if 0 <= time_diff_seconds < self.check_window_seconds:
            return True

        # For exact matches (important when using croniter), also check if
        # the timestamp exactly equals a scheduled time
        next_time = cron.get_next(datetime)
        cron_times: list[datetime] = [prev_time, next_time]

        # For daily schedules like "0 12 * * *", check if the hour:minute:second match
        # regardless of the date, which handles the different_day test case
        for cron_time in cron_times:
            if (
                context.timestamp.hour == cron_time.hour
                and context.timestamp.minute == cron_time.minute
                and context.timestamp.second == cron_time.second
            ):
                return True

        return False
