import logging
import statistics
import threading
import time
from typing import Any, NamedTuple, Optional

from pynenc.builder import PynencBuilder
from pynenc.util.redis_debug_client import (
    get_redis_stats_report,
    get_sorted_redis_commands,
    patch_redis_client,
    start_redis_debugging,
    start_tracking_redis_stats,
    stop_tracking_redis_stats,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

patch_redis_client()
start_redis_debugging(monitor_pools=True, monitor_interval=10)

# Create app with MultiThreadRunner
app = (
    PynencBuilder()
    .redis()
    .app_id("test_redis_degradation")
    .serializer("json")
    .multi_thread_runner()
    # TODO huge degradation in performance with cycle control
    .task_control(cycle_control=False, blocking_control=False)
    .build()
)


@app.task
def simple_task(task_id: int) -> int:
    """Simple task that just returns its ID."""
    return task_id


@app.task
def wait_for_tasks(num_tasks: int) -> list[int]:
    """Task that waits for multiple simple tasks to complete."""
    return list(simple_task.parallelize((i,) for i in range(num_tasks)).results)


class IterationResult(NamedTuple):
    """Results from a single test iteration"""

    batch_size: int
    iteration: int
    execution_time: float
    tasks_per_second: float
    redis_stats: Optional[str] = None  # Store Redis stats for this iteration
    redis_commands: Optional[list[Any]] = None  # Store command data for comparison


def run_synchronous(batch_size: int) -> float:
    """Run tasks synchronously and return execution time."""
    app.conf.dev_mode_force_sync_tasks = True
    start_time = time.perf_counter()
    _ = [simple_task(i) for i in range(batch_size)]
    end_time = time.perf_counter()
    return end_time - start_time


def run_iteration(batch_size: int, iteration: int) -> IterationResult:
    """Run a batch of tasks for a specific iteration and measure performance."""
    app.conf.dev_mode_force_sync_tasks = False
    app.runner.conf.invocation_wait_results_sleep_time_sec = 1

    # Start fresh Redis stats tracking for this iteration
    # app.purge()
    stop_tracking_redis_stats()
    start_tracking_redis_stats()

    # Log the start of the iteration
    app.logger.info(f"Starting iteration {iteration} with batch size {batch_size}")

    start_time = time.perf_counter()
    result = wait_for_tasks(batch_size).result
    end_time = time.perf_counter()
    execution_time = end_time - start_time

    # Validate results
    assert (
        len(result) == batch_size
    ), f"Expected {batch_size} results, got {len(result)}"
    assert set(result) == set(range(batch_size)), "Results don't match expected values"

    tasks_per_second = batch_size / execution_time

    app.logger.info(
        f"Iteration {iteration} completed: {tasks_per_second:.2f} tasks/s in {execution_time:.2f}s"
    )

    # Get Redis stats for this iteration
    redis_stats = get_redis_stats_report()  # Limit table width
    redis_commands = get_sorted_redis_commands()

    return IterationResult(
        batch_size,
        iteration,
        execution_time,
        tasks_per_second,
        redis_stats,
        redis_commands,
    )


# Update the compare_redis_commands function


def compare_redis_commands(first_commands: list[Any], last_commands: list[Any]) -> str:
    """
    Compare Redis commands between first and last iteration to identify degraded operations.

    Returns a formatted report of the most degraded commands.
    """
    # Create a map of caller+command to stats for first iteration
    first_map = {}
    for caller, cmd, stats in first_commands:
        key = f"{caller}|{cmd}"
        first_map[key] = stats

    # Compare with last iteration to find degraded commands
    degraded_commands = []
    for caller, cmd, stats in last_commands:
        key = f"{caller}|{cmd}"
        if key in first_map:
            first_stats = first_map[key]
            # Calculate degradation percentage in average time
            if first_stats.avg_time > 0 and first_stats.count > 0:
                degradation = (
                    (stats.avg_time - first_stats.avg_time) / first_stats.avg_time
                ) * 100
                # Include commands with any degradation, we'll sort later
                degraded_commands.append((caller, cmd, first_stats, stats, degradation))

    # Sort by degradation percentage (highest first)
    degraded_commands.sort(key=lambda x: x[4], reverse=True)

    # Format the report
    if not degraded_commands:
        return "No command degradation detected between iterations."

    report = [
        "Most Degraded Redis Commands (First vs Last Iteration):",
        "Shows how command performance changed from first to last iteration",
        "",
    ]

    # Define column widths with much wider caller column
    cmd_width = 25
    caller_width = 60  # Increased from 40
    stats_width = 15
    change_width = 12

    # Table header
    header = (
        f"{'Command':<{cmd_width}} {'Caller':<{caller_width}} "
        f"{'First Avg(ms)':<{stats_width}} {'Last Avg(ms)':<{stats_width}} "
        f"{'Change(%)':<{change_width}} {'Count 1st→Last':<{stats_width}}"
    )
    report.append(header)
    report.append("-" * len(header))

    # Get the number of degraded commands to show (at least 10, but show all if available)
    commands_to_show = max(
        15, min(len(degraded_commands), 20)
    )  # Show up to 20 commands, at least 15

    # Add each degraded command
    for caller, cmd, first_stats, last_stats, degradation in degraded_commands[
        :commands_to_show
    ]:
        # Truncate caller for display
        if len(caller) > caller_width:
            caller_display = "..." + caller[-(caller_width - 3) :]
        else:
            caller_display = caller

        # Format command for display
        if len(cmd) > cmd_width:
            cmd_display = cmd[: cmd_width - 3] + "..."
        else:
            cmd_display = cmd

        # Format the count change
        count_change = f"{first_stats.count}→{last_stats.count}"

        # Create the formatted line
        report.append(
            f"{cmd_display:<{cmd_width}} {caller_display:<{caller_width}} "
            f"{first_stats.avg_time*1000:<{stats_width}.2f} {last_stats.avg_time*1000:<{stats_width}.2f} "
            f"{degradation:+{change_width}.1f} {count_change:<{stats_width}}"
        )

    # Add a section to show improved commands too (negative degradation)
    improved_commands = [x for x in degraded_commands if x[4] < 0]
    if improved_commands:
        # Sort by improvement (most improved first)
        improved_commands.sort(key=lambda x: x[4])

        report.append("")
        report.append("Most Improved Redis Commands (First vs Last Iteration):")
        report.append("-" * len(header))

        # Show up to 5 most improved commands
        for caller, cmd, first_stats, last_stats, degradation in improved_commands[:5]:
            # Truncate caller for display
            if len(caller) > caller_width:
                caller_display = "..." + caller[-(caller_width - 3) :]
            else:
                caller_display = caller

            # Format command for display
            if len(cmd) > cmd_width:
                cmd_display = cmd[: cmd_width - 3] + "..."
            else:
                cmd_display = cmd

            # Format the count change
            count_change = f"{first_stats.count}→{last_stats.count}"

            # Create the formatted line
            report.append(
                f"{cmd_display:<{cmd_width}} {caller_display:<{caller_width}} "
                f"{first_stats.avg_time*1000:<{stats_width}.2f} {last_stats.avg_time*1000:<{stats_width}.2f} "
                f"{degradation:+{change_width}.1f} {count_change:<{stats_width}}"
            )

    # Add a summary section
    if degraded_commands:
        report.append("")
        report.append("Summary:")

        # Calculate average degradation across all commands
        avg_degradation = sum(d[4] for d in degraded_commands) / len(degraded_commands)
        median_degradation = sorted([d[4] for d in degraded_commands])[
            len(degraded_commands) // 2
        ]

        # Count significantly degraded commands (>10%)
        significant_degradations = sum(1 for d in degraded_commands if d[4] > 10)

        report.append(f"- Average command degradation: {avg_degradation:.1f}%")
        report.append(f"- Median command degradation: {median_degradation:.1f}%")
        report.append(
            f"- Commands with >10% degradation: {significant_degradations} of {len(degraded_commands)}"
        )

    return "\n".join(report)


def test_redis_degradation() -> None:
    """Test Redis performance degradation with repeated iterations of the same batch size."""
    # Test parameters - adjusted for iteration focus
    batch_size = 500  # Fixed batch size for all iterations
    num_iterations = 10  # Number of iterations to run
    max_degradation_percent = 5  # Maximum allowed degradation within iterations
    warmup_iterations = 2  # Number of initial iterations to ignore (warmup)
    min_iterations = 6  # Minimum iterations to consider for degradation analysis

    # Purge app state
    app.purge()

    # Start the runner
    def run_in_thread() -> None:
        app.runner.run()

    runner_thread = threading.Thread(target=run_in_thread, daemon=True)
    runner_thread.start()
    time.sleep(0.3)  # Let runner initialize

    # Run iterations
    iteration_results: list[IterationResult] = []

    try:
        app.logger.info(
            f"Starting Redis degradation test with {num_iterations} iterations of batch size {batch_size}"
        )

        # Run synchronous baseline for comparison
        sync_time = run_synchronous(batch_size)
        sync_tps = batch_size / sync_time

        app.logger.info(
            f"Synchronous baseline - {batch_size} tasks: {sync_time:.2f}s "
            f"({sync_tps:.2f} tasks/s)"
        )

        # Switch to distributed mode
        app.conf.dev_mode_force_sync_tasks = False

        for i in range(num_iterations):
            # Run the iteration
            result = run_iteration(batch_size, i + 1)
            iteration_results.append(result)

            # Skip degradation checks during warmup
            if i < warmup_iterations:
                app.logger.info(f"Warmup iteration {i+1}, skipping degradation check")
                continue

            # Only analyze after we have at least one post-warmup result
            if i >= warmup_iterations:
                # Get previous non-warmup iterations
                previous_iterations = iteration_results[warmup_iterations:i]

                if previous_iterations:
                    # Calculate baseline as the average of previous iterations
                    baseline_tps = statistics.mean(
                        r.tasks_per_second for r in previous_iterations
                    )
                    current_tps = result.tasks_per_second

                    # Calculate degradation percentage
                    degradation_percent = (
                        (1 - (current_tps / baseline_tps)) * 100
                        if baseline_tps > 0
                        else 0
                    )

                    app.logger.info(
                        f"Iteration {i+1} vs baseline: {current_tps:.2f} tasks/s vs {baseline_tps:.2f} tasks/s "
                        f"(Degradation: {degradation_percent:.1f}%)"
                    )

                    # Check if degradation exceeds limit (after min_iterations)
                    if (
                        min_iterations <= i
                        and degradation_percent > max_degradation_percent
                    ):
                        # Format detailed failure message
                        fail_msg = (
                            f"\n\nPERFORMANCE DEGRADATION DETECTED WITHIN ITERATIONS"
                            f"\n-------------------------------------------------"
                            f"\nBatch size: {batch_size}"
                            f"\nIteration {i+1} degraded {degradation_percent:.1f}% from baseline"
                            f"\nBaseline (avg of previous iterations): {baseline_tps:.2f} tasks/s"
                            f"\nCurrent iteration: {current_tps:.2f} tasks/s"
                            f"\nAllowed maximum degradation: {max_degradation_percent:.1f}%"
                            f"\n"
                            f"\nDetail of all iterations:"
                        )

                        for idx, res in enumerate(iteration_results):
                            warmup_tag = " (warmup)" if idx < warmup_iterations else ""
                            ratio_to_sync = res.tasks_per_second / sync_tps

                            fail_msg += (
                                f"\n - Iteration {res.iteration}: {res.execution_time:.2f}s, "
                                f"{res.tasks_per_second:.2f} tasks/s ({ratio_to_sync:.2%} of sync speed)"
                                f"{warmup_tag}"
                            )

                        # Add Redis command comparison between first non-warmup and current iteration
                        first_non_warmup = iteration_results[warmup_iterations]
                        current = iteration_results[i]
                        if first_non_warmup.redis_commands and current.redis_commands:
                            fail_msg += f"\n\n{compare_redis_commands(first_non_warmup.redis_commands, current.redis_commands)}"

                        # Add Redis stats for final iteration
                        fail_msg += f"\n\nRedis Stats for Iteration {i+1}:\n{result.redis_stats}"

                        assert degradation_percent <= max_degradation_percent, fail_msg

        # Performance analysis at the end of all iterations
        if len(iteration_results) > warmup_iterations:
            # Analyze non-warmup iterations
            active_results = iteration_results[warmup_iterations:]

            min_tps = min(r.tasks_per_second for r in active_results)
            max_tps = max(r.tasks_per_second for r in active_results)
            avg_tps = statistics.mean(r.tasks_per_second for r in active_results)
            stdev_tps = (
                statistics.stdev(r.tasks_per_second for r in active_results)
                if len(active_results) > 1
                else 0
            )

            # Calculate max degradation as percentage from maximum TPS
            max_degradation = (1 - (min_tps / max_tps)) * 100 if max_tps > 0 else 0

            app.logger.info(
                f"Performance summary for batch size {batch_size}:"
                f"\n - Average: {avg_tps:.2f} tasks/s"
                f"\n - Min: {min_tps:.2f} tasks/s"
                f"\n - Max: {max_tps:.2f} tasks/s"
                f"\n - Std Dev: {stdev_tps:.2f} tasks/s"
                f"\n - Max degradation: {max_degradation:.1f}%"
                f"\n - Sync speed ratio: {avg_tps/sync_tps:.2%}"
            )

            # Check if overall degradation exceeds limit
            if max_degradation > max_degradation_percent:
                # Add command comparison if we have significant degradation
                first_active = active_results[0]
                last_active = active_results[-1]

                if first_active.redis_commands and last_active.redis_commands:
                    comparison = compare_redis_commands(
                        first_active.redis_commands, last_active.redis_commands
                    )
                    app.logger.warning(
                        f"Overall max degradation ({max_degradation:.1f}%) exceeds threshold "
                        f"({max_degradation_percent:.1f}%)\n\n{comparison}"
                    )
                else:
                    app.logger.warning(
                        f"Overall max degradation ({max_degradation:.1f}%) exceeds threshold "
                        f"({max_degradation_percent:.1f}%), but individual iterations remained within limits"
                    )

    finally:
        # Stop Redis stats tracking
        stop_tracking_redis_stats()

        # Stop the runner
        app.runner.stop_runner_loop()
        runner_thread.join(timeout=5.0)
        app.purge()
