import multiprocessing
import os
import random
import string
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from time import perf_counter
from typing import TYPE_CHECKING, NamedTuple

import pytest

from pynenc import Task
from pynenc.arg_cache import DisabledArgCache

if TYPE_CHECKING:
    from pynenc import Pynenc
    from pynenc.runner.base_runner import BaseRunner


@dataclass
class BatchPerformanceConfig:
    """Configuration for batch parallelization performance tests."""

    data_size_kb: int  # Size of the shared argument in KB
    num_tasks: int  # Number of tasks to run in parallel
    batch_size: int  # Size of each batch for parallel processing
    single_task_time: float  # Time for a single task execution


class BatchPerformanceResults(NamedTuple):
    """Results from batch performance test execution."""

    total_time: float  # Wall-clock time for the entire execution
    num_batches: int  # Number of batches used
    data_size_kb: int  # Size of shared data in KB
    num_tasks: int  # Number of tasks processed
    results: list  # The actual results returned
    ideal_parallel_time: float  # Theoretical perfect parallelization time
    task_state_times: dict[str, dict[str, float]]  # Timing of task state transitions


def generate_large_data(size_kb: int) -> str:
    """Generate a random string of the specified size in KB."""
    chars = string.ascii_letters + string.digits
    # 1 KB is roughly 1000 characters
    return "".join(random.choice(chars) for _ in range(size_kb * 1000))


def calculate_batch_metrics(
    config: BatchPerformanceConfig,
    results: BatchPerformanceResults,
    runner: "BaseRunner",
) -> dict:
    """Calculate performance metrics for batch processing."""
    cpu_count = multiprocessing.cpu_count()
    max_parallel = min(cpu_count, runner.max_parallel_slots)

    # Calculate timing metrics
    avg_time_per_task = results.total_time / results.num_tasks

    # Estimate throughput
    throughput = results.num_tasks / results.total_time
    throughput_per_cpu = throughput / max_parallel

    # Calculate batch overhead - estimate based on ideal vs actual time
    total_batch_overhead = results.total_time - results.ideal_parallel_time
    per_batch_overhead = (
        total_batch_overhead / results.num_batches if results.num_batches > 0 else 0
    )

    # Calculate efficiency metrics
    parallelization_efficiency = results.ideal_parallel_time / results.total_time

    # Calculate the bottleneck factor - how much time is lost to batch overhead vs task execution
    bottleneck_factor = total_batch_overhead / (
        config.single_task_time * results.num_tasks
    )

    # Calculate task state transition times
    avg_waiting_time = 0.0
    avg_running_time = 0.0

    # Process task state times
    state_counts: dict = defaultdict(int)
    state_times: dict = defaultdict(float)

    for _, states in results.task_state_times.items():
        # Calculate time from CREATED to RUNNING
        if "CREATED" in states and "RUNNING" in states:
            wait_time = states["RUNNING"] - states["CREATED"]
            state_times["waiting"] += wait_time
            state_counts["waiting"] += 1

        # Calculate time in RUNNING state
        if "RUNNING" in states and "SUCCESS" in states:
            run_time = states["SUCCESS"] - states["RUNNING"]
            state_times["running"] += run_time
            state_counts["running"] += 1

        # Calculate total time from CREATED to SUCCESS
        if "CREATED" in states and "SUCCESS" in states:
            total_time = states["SUCCESS"] - states["CREATED"]
            state_times["total"] += total_time
            state_counts["total"] += 1

    # Calculate averages if data is available
    if state_counts["waiting"] > 0:
        avg_waiting_time = state_times["waiting"] / state_counts["waiting"]

    if state_counts["running"] > 0:
        avg_running_time = state_times["running"] / state_counts["running"]

    if state_counts["total"] > 0:
        avg_total_time = state_times["total"] / state_counts["total"]
    else:
        avg_total_time = 0.0

    # Calculate the arg_cache effectiveness
    arg_cache_min_size = (
        config.data_size_kb * 1000
    )  # Convert KB to bytes for comparison
    arg_cache_threshold = runner.app.arg_cache.conf.min_size_to_cache
    arg_cache_status = (
        "ACTIVE" if arg_cache_min_size >= arg_cache_threshold else "INACTIVE"
    )

    return {
        "data_size_kb": config.data_size_kb,
        "num_tasks": config.num_tasks,
        "batch_size": config.batch_size,
        "single_task_time": f"{config.single_task_time:.3f}s",
        "total_time": f"{results.total_time:.3f}s",
        "avg_time_per_task": f"{avg_time_per_task:.3f}s",
        "num_batches": results.num_batches,
        "parallel_slots": max_parallel,
        "throughput": f"{throughput:.2f} tasks/sec",
        "throughput_per_cpu": f"{throughput_per_cpu:.2f} tasks/sec/cpu",
        "ideal_parallel_time": f"{results.ideal_parallel_time:.3f}s",
        "total_batch_overhead": f"{total_batch_overhead:.3f}s",
        "per_batch_overhead": f"{per_batch_overhead:.3f}s",
        "parallelization_efficiency": f"{parallelization_efficiency*100:.1f}%",
        "batch_overhead_percentage": f"{(total_batch_overhead/results.total_time)*100:.1f}%",
        "bottleneck_factor": f"{bottleneck_factor:.2f}x",
        "avg_waiting_time": f"{avg_waiting_time:.3f}s",
        "avg_running_time": f"{avg_running_time:.3f}s",
        "avg_total_task_time": f"{avg_total_time:.3f}s",
        "arg_cache_status": arg_cache_status,
        "arg_cache_threshold": f"{arg_cache_threshold} bytes",
        "arg_size": f"{arg_cache_min_size} bytes",
        "runner_type": runner.__class__.__name__,
    }


def print_task_timeline(
    app: "Pynenc", task_state_times: dict, test_start_time: float, batch_size: int
) -> None:
    """Print a timeline of task execution to help visualize batch processing."""
    # Group tasks by batch based on their creation times
    batch_tasks = defaultdict(list)

    # Sort invocations by creation time
    sorted_invs = sorted(
        task_state_times.items(), key=lambda x: x[1].get("CREATED", float("inf"))
    )

    # Assign to batches
    for i, (inv_id, states) in enumerate(sorted_invs):
        batch_num = i // batch_size
        batch_tasks[batch_num].append((inv_id, states))

    # Print a timeline header
    app.logger.info("\n" + "=" * 80)
    app.logger.info("TASK EXECUTION TIMELINE (times in seconds from test start)")
    app.logger.info("=" * 80)

    # For each batch
    for batch_num, tasks in sorted(batch_tasks.items()):
        app.logger.info(f"\nBATCH {batch_num+1}:")
        app.logger.info("-" * 80)
        app.logger.info(
            f"{'INVOCATION':12} | {'CREATED':10} | {'QUEUED':10} | {'RUNNING':10} | {'SUCCESS':10} | {'TOTAL':10}"
        )
        app.logger.info("-" * 80)

        # Calculate batch stats
        batch_created = float("inf")
        batch_completed = 0

        for inv_id, states in tasks:
            # Calculate relative times from test start
            created = states.get("CREATED", None)
            queued = states.get("QUEUED", None)
            running = states.get("RUNNING", None)
            success = states.get("SUCCESS", None)

            # Track batch timing
            if created and created < batch_created:
                batch_created = created
            if success and success > batch_completed:
                batch_completed = success

            # Convert to relative time from test start
            if created:
                created_rel = created - test_start_time
            else:
                created_rel = None

            if queued:
                queued_rel = queued - test_start_time
            else:
                queued_rel = None

            if running:
                running_rel = running - test_start_time
            else:
                running_rel = None

            if success:
                success_rel = success - test_start_time
            else:
                success_rel = None

            # Calculate total time if possible
            if created and success:
                total_time = success - created
            else:
                total_time = None

            # Format for display
            def format_time(t: float) -> str:
                return f"{t:.3f}s" if t is not None else "N/A"

            inv_short = inv_id[:8]
            app.logger.info(
                f"{inv_short:12} | {format_time(created_rel):10} | {format_time(queued_rel):10} | "
                f"{format_time(running_rel):10} | {format_time(success_rel):10} | {format_time(total_time):10}"
            )

        # Print batch summary
        if batch_created != float("inf") and batch_completed != 0:
            batch_duration = batch_completed - batch_created
            app.logger.info(f"Batch {batch_num+1} total time: {batch_duration:.3f}s")

            # If there's a next batch, calculate gap
            if batch_num + 1 in batch_tasks:
                next_batch_created = min(
                    [
                        states.get("CREATED", float("inf"))
                        for inv_id, states in batch_tasks[batch_num + 1]
                    ]
                )
                if next_batch_created != float("inf"):
                    gap = next_batch_created - batch_completed
                    app.logger.info(f"Gap to next batch: {gap:.3f}s")

    app.logger.info("=" * 80)


def test_batch_parallelization_overhead(
    task_batch_process_shared_data: Task,
    task_process_large_shared_arg: Task,
) -> None:
    """Test the overhead of batch parallelization with large shared arguments."""
    # Skip if running in GitHub Actions
    if os.environ.get("GITHUB_ACTIONS") == "true":
        pytest.skip("Skipping large batch test in GitHub Actions")
    if isinstance(task_batch_process_shared_data.app.arg_cache, DisabledArgCache):
        pytest.skip("Skipping test due to disabled arg_cache")

    # Use fixed parameters for consistent testing - adjusted for better diagnostics
    data_size_kb = 20_000  # 20 MB
    num_tasks = 100
    batch_size = 50

    app = task_batch_process_shared_data.app
    app.logger.info(
        f"Testing batch parallelization with {num_tasks} tasks, "
        f"{data_size_kb} KB data, and batch size {batch_size}"
    )

    # Set logging for detailed diagnostics
    app.conf.logging_level = "info"

    # Log arg_cache configuration
    app.logger.info(
        f"ArgCache configuration: min_size_to_cache={app.arg_cache.conf.min_size_to_cache} bytes"
    )

    # Generate large shared data
    shared_data = generate_large_data(data_size_kb)
    app.logger.info(f"Generated {len(shared_data)/1000:.1f} KB of shared data")

    # Start runner thread
    runner_thread = threading.Thread(target=lambda: app.runner.run(), daemon=True)
    runner_thread.start()

    # First, measure the time for a single task execution
    app.logger.info("Measuring single task execution time...")
    single_start_time = perf_counter()
    _ = task_process_large_shared_arg(shared_data, 0).result
    single_task_time = perf_counter() - single_start_time

    app.logger.info(f"Single task execution completed in {single_task_time:.3f}s")

    # Set the batch size in the task module's configuration
    app.logger.info(f"Setting batch size to {batch_size}")
    task_process_large_shared_arg.options["parallel_batch_size"] = batch_size

    # Get test start time for timeline visualization
    test_start_time = time.time()

    # Execute the test
    config = BatchPerformanceConfig(
        data_size_kb=data_size_kb,
        num_tasks=num_tasks,
        batch_size=batch_size,
        single_task_time=single_task_time,
    )

    # Calculate ideal parallel time
    cpu_count = multiprocessing.cpu_count()
    max_parallel = min(cpu_count, app.runner.max_parallel_slots)
    ideal_parallel_time = (num_tasks * single_task_time) / max_parallel

    app.logger.info(f"Starting batch test with {max_parallel} parallel slots")
    app.logger.info(
        f"Theoretical minimum time (perfect parallelization): {ideal_parallel_time:.3f}s"
    )

    # Measure performance
    start_time = perf_counter()
    group_invocation = task_batch_process_shared_data(shared_data, num_tasks)
    result_list = group_invocation.result
    total_time = perf_counter() - start_time

    # Calculate number of batches
    num_batches = (num_tasks + batch_size - 1) // batch_size  # Ceiling division

    app.logger.info(
        f"Batch test completed in {total_time:.3f}s ({num_batches} batches)"
    )

    # Collect task execution timing data
    task_state_times = {}

    # Get invocations with timing data
    invocations = []
    try:
        # Get the subtasks from batch_process task if possible
        task_id = "tests.integration.apps.combinations.tasks.process_large_shared_arg"
        task = app.get_task(task_id)
        if task:
            invocations = list(app.orchestrator.get_existing_invocations(task=task))
            app.logger.info(
                f"Found {len(invocations)} subtask invocations for timing analysis"
            )
    except Exception as e:
        app.logger.error(f"Error retrieving subtask invocations: {e}")

    # Get state transition timing for each invocation
    for invocation in invocations:
        inv_id = invocation.invocation_id
        history = app.state_backend.get_history(invocation)

        if not history:
            continue

        # Extract timestamps for state transitions
        state_times = {}
        for entry in history:
            if entry.status:
                state_times[entry.status.name] = entry.timestamp.timestamp()

        task_state_times[inv_id] = state_times

    # Print a timeline visualization of task execution
    print_task_timeline(app, task_state_times, test_start_time, batch_size)

    # Cleanup
    app.runner.stop_runner_loop()
    runner_thread.join(timeout=5)
    if runner_thread.is_alive():
        pytest.fail("Runner thread did not terminate within 5 seconds")

    # Calculate and report metrics
    performance_results = BatchPerformanceResults(
        total_time=total_time,
        num_batches=num_batches,
        data_size_kb=data_size_kb,
        num_tasks=num_tasks,
        results=result_list,
        ideal_parallel_time=ideal_parallel_time,
        task_state_times=task_state_times,
    )

    metrics = calculate_batch_metrics(config, performance_results, app.runner)
    app.logger.info(f"Batch performance metrics: {metrics}")

    # Validate results consistency
    assert (
        len(result_list) == num_tasks
    ), f"Expected {num_tasks} results, got {len(result_list)}"

    # Check that all tasks received the correct data
    for data_len, idx in result_list:
        assert data_len == len(
            shared_data
        ), f"Task {idx} received incorrect data length"
        assert 0 <= idx < num_tasks, f"Invalid task index: {idx}"

    # Log optimization recommendations
    app.logger.info("\nPERFORMANCE OPTIMIZATION RECOMMENDATIONS:")

    # Check if arg_cache is the bottleneck
    arg_size = data_size_kb * 1000
    if arg_size >= app.arg_cache.conf.min_size_to_cache:
        app.logger.info(
            "- The large argument is being cached (size > min_size_to_cache threshold)\n"
            "  This is good for sharing the argument, but creates overhead for the initial caching."
        )
    else:
        app.logger.info(
            "- The argument size is below caching threshold - arguments are being passed directly.\n"
            "  Consider enabling caching by lowering min_size_to_cache if the same data is reused."
        )

    # Check batch size configuration
    if batch_size < 50:
        app.logger.info(
            "- Batch size is small, causing high overhead. Consider increasing batch size."
        )
    elif batch_size > 500:
        app.logger.info(
            "- Batch size is very large, which might increase latency. Consider a smaller batch size."
        )

    # Look at batch overhead
    bottleneck_factor = float(metrics["bottleneck_factor"].rstrip("x"))
    if bottleneck_factor > 5:
        app.logger.error(
            f"SEVERE BOTTLENECK: {bottleneck_factor}x overhead compared to task execution time\n"
            "This indicates a serious parallelization issue - batch creation is likely taking too long."
        )

        # Specific recommendations
        app.logger.info(
            "RECOMMENDATIONS FOR REDUCING BATCH OVERHEAD:\n"
            "1. Increase batch size to reduce the number of batching operations\n"
            "2. Use arg_cache for shared arguments by ensuring min_size_to_cache is set appropriately\n"
            "3. Consider optimizing the broker connections (pipeline operations)\n"
            "4. Check for slow operations - consider using a dedicated instance\n"
            "5. Monitor memory usage - high memory pressure can slow operations"
        )
    elif bottleneck_factor > 2:
        app.logger.warning(
            f"SIGNIFICANT BOTTLENECK: {bottleneck_factor}x overhead compared to task execution time"
        )
    else:
        app.logger.info(
            f"ACCEPTABLE OVERHEAD: {bottleneck_factor}x overhead compared to task execution time"
        )

    # Check batch overhead
    batch_overhead_pct = float(metrics["batch_overhead_percentage"].rstrip("%")) / 100

    # More detailed diagnostic information based on the metrics
    efficiency = float(metrics["parallelization_efficiency"].rstrip("%")) / 100
    if efficiency < 0.3:  # Less than 30% efficiency
        app.logger.error(
            f"CRITICAL PERFORMANCE ISSUE: Parallelization efficiency is only {efficiency*100:.1f}%\n"
            f"- Each batch takes ~{metrics['per_batch_overhead']} of overhead\n"
            f"- Batch size might be too small or communication overhead is too high"
        )
    elif efficiency < 0.6:  # Less than 60% efficiency
        app.logger.warning(
            f"SUBOPTIMAL PERFORMANCE: Parallelization efficiency is {efficiency*100:.1f}%\n"
            f"- Batch overhead is {metrics['per_batch_overhead']} per batch\n"
            f"- Consider increasing batch size or reducing communication overhead"
        )

    # For very small batches (high overhead), we're more lenient
    max_acceptable_overhead = 0.1  # 10% of total time as overhead for batching

    # Assert that batch overhead is acceptable
    assert batch_overhead_pct <= max_acceptable_overhead, (
        f"Batch overhead ({metrics['batch_overhead_percentage']}) exceeds the acceptable "
        f"limit ({max_acceptable_overhead*100:.1f}%) for batch size {batch_size}"
    )
