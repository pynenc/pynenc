"""
Atomic service coordination for distributed Pynenc runners.

This module provides functions to coordinate the execution of atomic global services
(like trigger processing and invocation recovery) across multiple distributed runners.
It ensures that only one runner executes these services at any given time, preventing
race conditions and duplicate work.

Key components:
- ActiveRunnerInfo: Runner metadata including heartbeat and execution history
- Time slot calculation with execution history awareness
- Execution time validation and warnings
"""

from datetime import datetime
from typing import TYPE_CHECKING, NamedTuple

from pynenc import context

if TYPE_CHECKING:
    pass


class ActiveRunnerInfo(NamedTuple):
    """
    Information about an active runner including heartbeat and execution tracking.

    :param str runner_id: The unique identifier for the runner
    :param datetime creation_time: When the runner was first registered
    :param datetime last_heartbeat: When the last heartbeat was received
    :param bool allow_to_run_atomic_service: Whether this runner can run atomic services
    :param datetime | None last_service_start: When the last atomic service execution started
    :param datetime | None last_service_end: When the last atomic service execution ended
    """

    runner_id: str
    creation_time: datetime
    last_heartbeat: datetime
    allow_to_run_atomic_service: bool = False
    last_service_start: datetime | None = None
    last_service_end: datetime | None = None

    def get_last_execution_duration_seconds(self) -> float | None:
        """
        Calculate the duration of the last service execution.

        :return: Duration in seconds, or None if no execution recorded
        :rtype: float | None
        """
        if self.last_service_start and self.last_service_end:
            return (self.last_service_end - self.last_service_start).total_seconds()
        return None


def calculate_runner_position(
    runner_id: str, active_runners: list[ActiveRunnerInfo]
) -> int | None:
    """
    Find the position of a runner in the ordered list of active runners.

    Runners are ordered by creation time (oldest first), providing stable
    ordering for time slot assignment.

    :param str runner_id: The runner ID to find
    :param list[ActiveRunnerInfo] active_runners: Ordered list of active runners
    :return: Zero-based position index, or None if runner not found
    :rtype: int | None
    """
    try:
        return next(i for i, r in enumerate(active_runners) if r.runner_id == runner_id)
    except StopIteration:
        return None


def get_max_execution_duration(active_runners: list[ActiveRunnerInfo]) -> float:
    """
    Find the maximum execution duration among all runners with history.

    This helps determine realistic time slot sizes based on actual execution behavior.

    :param list[ActiveRunnerInfo] active_runners: List of active runners
    :return: Maximum execution duration in seconds, or 0 if no history
    :rtype: float
    """
    max_duration = 0.0
    for runner in active_runners:
        if duration := runner.get_last_execution_duration_seconds():
            max_duration = max(max_duration, duration)
    return max_duration


def validate_execution_time(
    execution_duration: float,
    allocated_slot_size: float,
    runner_id: str,
) -> None:
    """
    Validate that execution fits within allocated time slot and warn if not.

    This detects configuration issues where service execution exceeds the time window,
    potentially causing overlapping executions and breaking atomicity guarantees.

    :param float execution_duration: Actual execution time in seconds
    :param float allocated_slot_size: Allocated time window in seconds
    :param str runner_id: ID of the runner for logging context
    """
    if runner := context.get_current_runner(runner_id.split("@")[0]):
        logger = runner.app.logger
    else:
        return  # Can't log without context

    if execution_duration > allocated_slot_size:
        logger.warning(
            f"Atomic service execution overran time slot! "
            f"Runner: {runner_id}, "
            f"Execution: {execution_duration:.2f}s, "
            f"Allocated: {allocated_slot_size:.2f}s, "
            f"Overrun: {execution_duration - allocated_slot_size:.2f}s. "
            f"This breaks atomicity guarantees. Consider increasing "
            f"atomic_service_interval_minutes or reducing service workload."
        )
    elif execution_duration > (allocated_slot_size * 0.8):
        logger.info(
            f"Atomic service execution approaching time slot limit. "
            f"Runner: {runner_id}, "
            f"Execution: {execution_duration:.2f}s, "
            f"Allocated: {allocated_slot_size:.2f}s, "
            f"Usage: {(execution_duration / allocated_slot_size * 100):.1f}%"
        )


def calculate_time_slot(
    runner_position: int,
    total_runners: int,
    service_interval_minutes: float,
    spread_margin_minutes: float,
    active_runners: list[ActiveRunnerInfo] | None = None,
) -> tuple[float, float]:
    """
    Calculate the time slot (start and end) for a runner's service execution window.

    The service interval is divided equally among runners, with a spread margin
    subtracted from each slot to prevent overlapping executions. If execution history
    is available, validates that previous executions fit within allocated slots.

    :param int runner_position: Zero-based position of the runner
    :param int total_runners: Total number of active runners
    :param float service_interval_minutes: Total service cycle duration in minutes
    :param float spread_margin_minutes: Safety margin in minutes
    :param list[ActiveRunnerInfo] | None active_runners: Optional runner history for validation
    :return: Tuple of (start_time, end_time) in seconds within the cycle
    :rtype: tuple[float, float]
    """
    service_interval = service_interval_minutes * 60
    spread_margin = spread_margin_minutes * 60

    time_slot_size = service_interval / total_runners
    runner_start_time = runner_position * time_slot_size
    runner_end_time = runner_start_time + time_slot_size - spread_margin

    # Ensure the window is valid
    if runner_end_time <= runner_start_time:
        runner_end_time = runner_start_time + (time_slot_size / 2)

    # Validate against actual execution history if available
    if active_runners:
        allocated_slot = runner_end_time - runner_start_time
        runner_info = active_runners[runner_position]

        if duration := runner_info.get_last_execution_duration_seconds():
            validate_execution_time(duration, allocated_slot, runner_info.runner_id)

    return runner_start_time, runner_end_time


def is_runner_in_time_slot(
    current_time: float,
    service_interval_minutes: float,
    start_time: float,
    end_time: float,
) -> bool:
    """
    Check if the current time falls within a runner's execution time slot.

    Uses modulo arithmetic to map current time into the repeating service cycle.

    :param float current_time: Current Unix timestamp
    :param float service_interval_minutes: Service cycle duration in minutes
    :param float start_time: Slot start time in seconds (within cycle)
    :param float end_time: Slot end time in seconds (within cycle)
    :return: True if current time is within the slot
    :rtype: bool
    """
    service_interval = service_interval_minutes * 60
    time_in_cycle = current_time % service_interval
    return start_time <= time_in_cycle < end_time


def can_run_atomic_service(
    runner_id: str,
    active_runners: list[ActiveRunnerInfo],
    current_time: float,
    service_interval_minutes: float,
    spread_margin_minutes: float,
) -> bool:
    """
    Determine if a runner should execute atomic global services now.

    This function implements a distributed coordination algorithm that:
    1. Orders runners by creation time
    2. Divides the service interval into equal time slots
    3. Assigns each runner an exclusive time window
    4. Validates execution times against allocated slots
    5. Checks if current time falls within this runner's window

    Single-runner optimization: If only one runner exists, it always runs services.

    :param str runner_id: The ID of the runner to check
    :param list[ActiveRunnerInfo] active_runners: All currently active runners
    :param float current_time: Current Unix timestamp
    :param float service_interval_minutes: How often services should run
    :param float spread_margin_minutes: Safety margin to prevent overlaps
    :return: True if this runner should execute services now
    :rtype: bool

    Example:
        With 3 runners and a 6-minute interval:
        - Runner 0: executes in minutes 0-2
        - Runner 1: executes in minutes 2-4
        - Runner 2: executes in minutes 4-6
    """
    if not active_runners:
        return False

    # Single runner optimization
    total_runners = len(active_runners)
    if total_runners == 1:
        return True

    # Find this runner's position
    runner_position = calculate_runner_position(runner_id, active_runners)
    if runner_position is None:
        return False

    # Calculate this runner's time slot with history awareness
    start_time, end_time = calculate_time_slot(
        runner_position,
        total_runners,
        service_interval_minutes,
        spread_margin_minutes,
        active_runners,
    )

    # Check if we're in this runner's time slot
    return is_runner_in_time_slot(
        current_time, service_interval_minutes, start_time, end_time
    )
