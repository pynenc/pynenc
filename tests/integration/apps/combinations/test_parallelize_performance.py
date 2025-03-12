import multiprocessing
import threading
from dataclasses import dataclass
from time import perf_counter
from typing import TYPE_CHECKING, NamedTuple

import pytest

from pynenc import Task

if TYPE_CHECKING:
    from pynenc import Pynenc
    from pynenc.runner.base_runner import BaseRunner


@dataclass
class DistributedPerformanceConfig:
    """Configuration for distributed CPU work performance tests."""

    total_iterations: int  # Total work to be split
    tasks_per_cpu_multiplier: float  # Number of tasks per CPU core
    num_sub_tasks: int  # Number of sub-tasks to split into


class DistributedPerformanceResults(NamedTuple):
    """Results from distributed performance test execution."""

    individual_times: list[float]  # Times for each sub-task
    total_execution_time: float  # Wall-clock time for the entire invocation


def calculate_distributed_metrics(
    config: DistributedPerformanceConfig,
    results: DistributedPerformanceResults,
    runner: "BaseRunner",
) -> dict:
    cpu_count = multiprocessing.cpu_count()
    avg_sub_task_time = sum(results.individual_times) / len(results.individual_times)
    min_sub_task_time = min(results.individual_times)
    max_sub_task_time = max(results.individual_times)
    total_cpu_time = sum(results.individual_times)
    sequential_time = avg_sub_task_time * config.num_sub_tasks
    parallelization_factor = sequential_time / results.total_execution_time
    efficiency_per_cpu = parallelization_factor / min(cpu_count, config.num_sub_tasks)

    return {
        "num_sub_tasks": config.num_sub_tasks,
        "tasks_per_cpu_multiplier": f"{config.tasks_per_cpu_multiplier:.2f}x",
        "avg_sub_task_time": f"{avg_sub_task_time:.3f}s",
        "min_sub_task_time": f"{min_sub_task_time:.3f}s",
        "max_sub_task_time": f"{max_sub_task_time:.3f}s",
        "total_cpu_time": f"{total_cpu_time:.3f}s",
        "sequential_time": f"{sequential_time:.3f}s",
        "wall_clock_time": f"{results.total_execution_time:.3f}s",
        "parallelization_factor": f"{parallelization_factor:.2f}x",
        "efficiency_per_cpu": f"{efficiency_per_cpu:.2f}x",
        "max_parallel_slots": runner.max_parallel_slots,
        "runner_type": runner.__class__.__name__,
        "individual_sub_task_times": [f"{t:.3f}s" for t in results.individual_times],
    }


def get_test_config(
    app: "Pynenc", tasks_per_cpu_multiplier: float
) -> DistributedPerformanceConfig:
    """Get test configuration based on runner type and system resources."""
    cpu_count = multiprocessing.cpu_count()
    num_sub_tasks = int(tasks_per_cpu_multiplier * cpu_count)
    total_iterations = 190_600_000

    return DistributedPerformanceConfig(
        total_iterations=total_iterations,
        tasks_per_cpu_multiplier=tasks_per_cpu_multiplier,
        num_sub_tasks=num_sub_tasks,
    )


MIN_CPUS_FOR_PERFORMANCE_TEST = 4


@pytest.mark.parametrize("tasks_per_cpu_multiplier", [3.0])
def test_distributed_cpu_work_performance(
    app: "Pynenc",
    task_cpu_intensive_no_conc: Task,
    task_distribute_cpu_work: Task,
    tasks_per_cpu_multiplier: float,
) -> None:
    """Test the performance of distribute_cpu_work using parallelize with a CPU multiplier."""
    # Skip if insufficient CPU cores
    cpu_count = multiprocessing.cpu_count()
    if cpu_count < MIN_CPUS_FOR_PERFORMANCE_TEST:
        pytest.skip(
            f"Need at least {MIN_CPUS_FOR_PERFORMANCE_TEST} CPUs (found {cpu_count})"
        )

    app.conf.logging_level = "info"

    # Define test configuration dynamically
    config = get_test_config(app, tasks_per_cpu_multiplier)
    app.logger.info(f"Test config: {config}")

    runner_thread = threading.Thread(target=lambda: app.runner.run(), daemon=True)
    runner_thread.start()

    start_time = perf_counter()
    distributed_invocation = task_distribute_cpu_work(
        total_iterations=config.total_iterations,
        num_sub_tasks=config.num_sub_tasks,
    )
    individual_times = distributed_invocation.result
    total_time = perf_counter() - start_time
    app.logger.info(
        f"Distributed run: total_time={total_time:.3f}s, sub_task_times={individual_times}"
    )

    # Cleanup
    app.runner.stop_runner_loop()
    runner_thread.join(timeout=5)
    if runner_thread.is_alive():
        pytest.fail("Runner thread did not terminate within 5 seconds")

    # Calculate results
    results = DistributedPerformanceResults(individual_times, total_time)
    performance_data = calculate_distributed_metrics(config, results, app.runner)
    app.logger.info(f"Performance data: {performance_data}")

    # New assertion: Wall-clock time between 3x min and 4x max
    min_sub_task_time = min(individual_times)
    max_sub_task_time = max(individual_times)
    expected_min_time = 3 * min_sub_task_time
    expected_max_time = 4 * max_sub_task_time
    assert expected_min_time <= total_time <= expected_max_time, (
        f"Wall-clock time {total_time:.3f}s outside expected range "
        f"[{expected_min_time:.3f}s, {expected_max_time:.3f}s] "
        f"(3x min={3*min_sub_task_time:.3f}s, 4x max={4*max_sub_task_time:.3f}s)\n"
        f"Performance data: {performance_data}"
    )

    # Check variance in individual execution times
    avg_sub_task_time = sum(individual_times) / len(individual_times)
    for i, t in enumerate(individual_times):
        assert 0.5 * avg_sub_task_time <= t <= 2.0 * avg_sub_task_time, (
            f"Sub-task {i} time {t:.3f}s deviates too far from average {avg_sub_task_time:.3f}s "
            f"(must be within 50%-200%)"
        )
