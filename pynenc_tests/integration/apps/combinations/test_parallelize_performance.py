"""
Integration performance test for distributed CPU work.

Checks system CPU load before running to avoid unreliable results under high background load.
Key components:
- DistributedPerformanceConfig: test configuration dataclass
- DistributedPerformanceResults: results container
- calculate_distributed_metrics: metrics calculation
- test_distributed_cpu_work_performance: main test with CPU load check
"""

import multiprocessing
import os
import threading
from dataclasses import dataclass
from time import perf_counter
from typing import TYPE_CHECKING, NamedTuple

import psutil
import pytest

from pynenc import Task

if TYPE_CHECKING:
    from pynenc import Pynenc
    from pynenc.runner.base_runner import BaseRunner


@dataclass
class DistributedPerformanceConfig:
    """Configuration for distributed CPU work performance."""

    total_iterations: int  # Total work to be split
    tasks_per_cpu_multiplier: float  # Number of tasks per CPU core
    num_sub_tasks: int  # Number of sub-tasks to split into


class DistributedPerformanceResults(NamedTuple):
    """Results from distributed performance test execution."""

    invocation_times: dict[
        str, tuple[float, float, float, int]
    ]  # invocation_id -> (start, end, elapsed, iterations)
    total_execution_time: float  # Wall-clock time for the entire invocation


def calculate_distributed_metrics(
    config: DistributedPerformanceConfig,
    results: DistributedPerformanceResults,
    runner: "BaseRunner",
) -> dict:
    cpu_count = multiprocessing.cpu_count()
    individual_times = [v[2] for v in results.invocation_times.values()]
    num_tasks = len(individual_times)
    if num_tasks == 0:
        avg_sub_task_time = (
            min_sub_task_time
        ) = (
            max_sub_task_time
        ) = (
            total_cpu_time
        ) = (
            sequential_time
        ) = parallelization_factor = efficiency_per_cpu = avg_iters = 0.0
        avg_sub_task_time_str = (
            min_sub_task_time_str
        ) = (
            max_sub_task_time_str
        ) = (
            total_cpu_time_str
        ) = (
            sequential_time_str
        ) = (
            wall_clock_time_str
        ) = parallelization_factor_str = efficiency_per_cpu_str = "N/A"
        individual_sub_task_times = []
        individual_sub_task_iters = []
    else:
        avg_sub_task_time = sum(individual_times) / num_tasks
        min_sub_task_time = min(individual_times)
        max_sub_task_time = max(individual_times)
        total_cpu_time = sum(individual_times)
        sequential_time = avg_sub_task_time * config.num_sub_tasks
        parallelization_factor = (
            sequential_time / results.total_execution_time
            if results.total_execution_time
            else 0.0
        )
        efficiency_per_cpu = (
            parallelization_factor / min(cpu_count, config.num_sub_tasks)
            if min(cpu_count, config.num_sub_tasks)
            else 0.0
        )
        avg_iters = sum(individual_times) / num_tasks
        avg_sub_task_time_str = f"{avg_sub_task_time:.3f}s"
        min_sub_task_time_str = f"{min_sub_task_time:.3f}s"
        max_sub_task_time_str = f"{max_sub_task_time:.3f}s"
        total_cpu_time_str = f"{total_cpu_time:.3f}s"
        sequential_time_str = f"{sequential_time:.3f}s"
        wall_clock_time_str = f"{results.total_execution_time:.3f}s"
        parallelization_factor_str = f"{parallelization_factor:.2f}x"
        efficiency_per_cpu_str = f"{efficiency_per_cpu:.2f}x"
        individual_sub_task_times = [f"{t:.3f}s" for t in individual_times]
        individual_sub_task_iters = individual_times
    return {
        "num_sub_tasks": config.num_sub_tasks,
        "tasks_per_cpu_multiplier": f"{config.tasks_per_cpu_multiplier:.2f}x",
        "avg_sub_task_time": avg_sub_task_time_str,
        "min_sub_task_time": min_sub_task_time_str,
        "max_sub_task_time": max_sub_task_time_str,
        "total_cpu_time": total_cpu_time_str,
        "sequential_time": sequential_time_str,
        "wall_clock_time": wall_clock_time_str,
        "parallelization_factor": parallelization_factor_str,
        "efficiency_per_cpu": efficiency_per_cpu_str,
        "max_parallel_slots": runner.max_parallel_slots,
        "runner_type": runner.__class__.__name__,
        "individual_sub_task_times": individual_sub_task_times,
        "individual_sub_task_iters": individual_sub_task_iters,
        "avg_sub_task_iters": avg_iters if num_tasks else "N/A",
    }


def get_test_config(
    app: "Pynenc", tasks_per_cpu_multiplier: float
) -> tuple[DistributedPerformanceConfig, float]:
    """Get test configuration based on runner type and system resources."""
    cpu_count = multiprocessing.cpu_count()
    num_sub_tasks = int(tasks_per_cpu_multiplier * cpu_count)
    # Remove total_iterations, add min_runtime_sec for time-based benchmarking
    min_runtime_sec = 0.5
    return (
        DistributedPerformanceConfig(
            total_iterations=0,  # Not used anymore
            tasks_per_cpu_multiplier=tasks_per_cpu_multiplier,
            num_sub_tasks=num_sub_tasks,
        ),
        min_runtime_sec,
    )


MIN_CPUS_FOR_PERFORMANCE_TEST = 4


@pytest.mark.parametrize("tasks_per_cpu_multiplier", [3.0])
def test_distributed_cpu_work_performance(
    task_distribute_cpu_work: Task,
    tasks_per_cpu_multiplier: float,
) -> None:
    """
    Test the performance of distribute_cpu_work using parallelize with a CPU multiplier.
    Each sub-task runs for a fixed period, and we collect both execution time and iteration count for each.
    This test provides detailed logs for each sub-task and overall parallelization efficiency.
    Skips test if system CPU load is above 50% to avoid unreliable results.
    :param Task task_distribute_cpu_work: The distributed CPU work task
    :param float tasks_per_cpu_multiplier: Multiplier for sub-task count per CPU
    """
    # Check current CPU load and skip if too high
    cpu_percent = psutil.cpu_percent(interval=1)
    if cpu_percent > 50:
        pytest.skip(
            f"High CPU load detected ({cpu_percent}%), skipping performance test"
        )

    # Skip if insufficient CPU cores
    cpu_count = multiprocessing.cpu_count()
    if os.environ.get("GITHUB_ACTIONS") == "true":
        pytest.skip("Skipping test to run in GitHub")
    if cpu_count < MIN_CPUS_FOR_PERFORMANCE_TEST:
        pytest.skip(
            f"Need at least {MIN_CPUS_FOR_PERFORMANCE_TEST} CPUs (found {cpu_count})"
        )
    app = task_distribute_cpu_work.app
    app.logger.info(f"Testing with {cpu_count} CPUs")
    app.conf.logging_level = "info"

    # Define test configuration dynamically
    config, min_runtime_sec = get_test_config(app, tasks_per_cpu_multiplier)
    app.logger.info(f"Test config: {config}, min_runtime_sec={min_runtime_sec}")

    runner_thread = threading.Thread(target=lambda: app.runner.run(), daemon=True)
    runner_thread.start()

    start_time = perf_counter()
    distributed_results = task_distribute_cpu_work(
        min_runtime_sec=min_runtime_sec,
        num_sub_tasks=config.num_sub_tasks,
    )
    # Normalize results into a mapping for iteration without reassigning a differently-typed variable
    if isinstance(distributed_results, dict):
        results_map = distributed_results
    else:
        results_map = {
            inv.invocation_id: inv.result
            for inv in getattr(distributed_results, "invocations", [])
        }
    total_time = perf_counter() - start_time
    app.logger.info(f"Distributed run: total_time={total_time:.3f}s")
    app.logger.info("Sub-task results:")
    for inv_id, (start, end, elapsed, iters) in results_map.items():
        app.logger.info(
            f"  {inv_id}: start={start:.3f}, end={end:.3f}, elapsed={elapsed:.3f}s, iterations={iters}"
        )

    # Cleanup
    app.runner.stop_runner_loop()
    runner_thread.join(timeout=5)
    if runner_thread.is_alive():
        pytest.fail("Runner thread did not terminate within 5 seconds")

    # Calculate results
    results = DistributedPerformanceResults(results_map, total_time)
    individual_times = [v[2] for v in results_map.values()]
    performance_data = calculate_distributed_metrics(config, results, app.runner)
    app.logger.info("\n===== Distributed Performance Results =====")
    app.logger.info(f"Num sub-tasks: {performance_data['num_sub_tasks']}")
    app.logger.info(f"Avg sub-task time: {performance_data['avg_sub_task_time']}")
    avg_iters = performance_data["avg_sub_task_iters"]
    if isinstance(avg_iters, float):
        app.logger.info(f"Avg sub-task iterations: {avg_iters:.0f}")
    else:
        app.logger.info(f"Avg sub-task iterations: {avg_iters}")
    app.logger.info(f"Wall clock time: {performance_data['wall_clock_time']}")
    app.logger.info(
        f"Parallelization factor: {performance_data['parallelization_factor']}"
    )
    app.logger.info(f"Efficiency per CPU: {performance_data['efficiency_per_cpu']}")
    app.logger.info("==========================================\n")

    # Set expected time factors based on runner type
    runner_type = app.runner.__class__.__name__
    if runner_type == "MultiThreadRunner":
        MIN_TIME_FACTOR = 1.5
        MAX_TIME_FACTOR = 5
    elif runner_type == "PersistentProcessRunner":
        MIN_TIME_FACTOR = 3
        MAX_TIME_FACTOR = 6
    elif runner_type == "ProcessRunner":
        MIN_TIME_FACTOR = 3
        MAX_TIME_FACTOR = 7
    else:
        MIN_TIME_FACTOR = 3
        MAX_TIME_FACTOR = 5

    # Calculate expected times
    if individual_times:
        min_sub_task_time = min(individual_times)
        max_sub_task_time = max(individual_times)
        expected_min_time = MIN_TIME_FACTOR * min_sub_task_time
        expected_max_time = MAX_TIME_FACTOR * max_sub_task_time
        assert expected_min_time <= total_time <= expected_max_time, (
            f"Wall-clock time {total_time:.3f}s outside expected range "
            f"[{expected_min_time:.3f}s, {expected_max_time:.3f}s] "
            f"({MIN_TIME_FACTOR}x min={MIN_TIME_FACTOR*min_sub_task_time:.3f}s, "
            f"{MAX_TIME_FACTOR}x max={MAX_TIME_FACTOR*max_sub_task_time:.3f}s)\n"
            f"Performance data: {performance_data}"
        )

        avg_sub_task_time = sum(individual_times) / len(individual_times)
        for i, t in enumerate(individual_times):
            assert 0.25 * avg_sub_task_time <= t <= 3.0 * avg_sub_task_time, (
                f"Sub-task {i} time {t:.3f}s deviates too far from average {avg_sub_task_time:.3f}s "
                f"(must be within 25%-200%)"
            )
    else:
        app.logger.warning(
            "No sub-task times available; skipping time-based assertions."
        )
