"""
Heartbeat utility for Pynenc runners.

Provides functions to start background threads that periodically send heartbeats for runner contexts.
Also includes heartbeat interval calculation to prevent race conditions.
"""

import threading
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.runner.runner_context import RunnerContext


def calculate_heartbeat_interval(
    check_interval_minutes: float, timeout_minutes: float
) -> float:
    """
    Calculate the safe heartbeat sleep interval in seconds.

    This function ensures the heartbeat interval is significantly smaller than the timeout
    to prevent false positives where a healthy runner is incorrectly detected as dead.

    The heartbeat must occur at least 3 times within a timeout period to ensure that
    even with timing variations, the heartbeat check will always succeed before timeout.

    :param float check_interval_minutes: The configured check interval in minutes
    :param float timeout_minutes: The configured timeout in minutes
    :return: The safe heartbeat sleep interval in seconds
    :rtype: float
    """
    check_interval_seconds = check_interval_minutes * 60
    timeout_seconds = timeout_minutes * 60

    # Ensure heartbeat occurs at least 3 times per timeout period
    # This provides a safety margin: even with one missed heartbeat, we're still safe
    max_safe_interval = timeout_seconds / 3

    # Use the smaller of the two: either the configured check interval or the max safe interval
    return min(check_interval_seconds, max_safe_interval)


def start_invocation_runner_heartbeat(
    app: "Pynenc", runner_ctx: "RunnerContext"
) -> threading.Thread:
    """
    Start a background thread that periodically sends a heartbeat for a runner context.

    This is used for invocation runners (not atomic service runners).

    CRITICAL: Registers the first heartbeat synchronously before returning to prevent
    a race condition where recovery could mark the runner as dead before the background
    thread has a chance to send its first heartbeat.
    """
    sleep_interval = calculate_heartbeat_interval(
        app.conf.atomic_service_check_interval_minutes,
        app.conf.atomic_service_runner_considered_dead_after_minutes,
    )

    # CRITICAL: Register first heartbeat synchronously to prevent race condition
    # Without this, recovery service could run before the background thread sends
    # its first heartbeat, causing false positive dead runner detection
    app.orchestrator.register_runner_heartbeat(runner_ctx, can_run_atomic_service=False)
    app.logger.info(
        f"Initial heartbeat registered for {runner_ctx.runner_id}, "
        f"starting background thread with interval={sleep_interval}s"
    )

    def heartbeat_loop(sleep_time: float) -> None:
        """
        Continuous loop for sending heartbeats.
        """
        while True:
            try:
                time.sleep(sleep_time)
                app.orchestrator.register_runner_heartbeat(
                    runner_ctx, can_run_atomic_service=False
                )
            except Exception as e:
                # Log error but don't crash thread
                app.logger.error(f"Error in heartbeat loop: {e}")
                time.sleep(1)  # Prevent tight loop on error

    thread = threading.Thread(
        target=heartbeat_loop, args=(sleep_interval,), daemon=True
    )
    thread.start()
    return thread
