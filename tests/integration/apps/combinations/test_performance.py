import multiprocessing
import threading
from dataclasses import dataclass
from time import perf_counter, sleep
from typing import TYPE_CHECKING, NamedTuple

import pytest

from pynenc import Task
from pynenc.runner.process_runner import ProcessRunner

if TYPE_CHECKING:
    from pynenc import Pynenc
    from pynenc.runner.base_runner import BaseRunner


@dataclass
class PerformanceTestConfig:
    """Configuration for performance tests based on runner type."""

    iterations: int
    num_tasks: int
    expected_min: float = 0.0
    expected_max: float | None = None
    expected_min_parallelization: float | None = None


# ################################################################################### #
# TEST CONFIG GENERATION: Get test configuration based on runner type
# ################################################################################### #


def get_test_config(app: "Pynenc") -> PerformanceTestConfig:
    """Get test configuration based on runner type."""
    if app.runner.mem_compatible():
        # Thread runner - expect nearly sequential execution due to GIL
        return PerformanceTestConfig(
            iterations=600_000, num_tasks=5, expected_min=0.7, expected_max=1.3
        )
    elif isinstance(app.runner, ProcessRunner):
        # Process runner - starts one independent process per tasks
        # to avoid overloading the system and the overhead of starting too many processes
        return PerformanceTestConfig(
            iterations=300_000,
            num_tasks=multiprocessing.cpu_count(),
            expected_min_parallelization=0.5,
        )
    else:
        # MultiThread and PersistentProcess runners
        # These should reach highest levels of parallelism
        return PerformanceTestConfig(
            iterations=300_000,
            num_tasks=multiprocessing.cpu_count() * 10,
            expected_min_parallelization=0.95,
        )


# ################################################################################### #
# ################################################################################### #


class PerformanceResults(NamedTuple):
    """Results from performance test execution."""

    individual_times: list[float]
    total_execution_time: float
    calibration_time: float


def run_performance_test(
    task: Task, config: PerformanceTestConfig
) -> PerformanceResults:
    """Execute performance test with given configuration."""
    app = task.app
    # Start runner
    thread = threading.Thread(target=lambda: app.runner.run(), daemon=True)
    thread.start()

    # Calibration run
    calibration = task(iterations=config.iterations).result
    app.logger.info(
        f"Calibration: {calibration:.3f}s for {config.iterations} iterations"
    )

    # Parallel test
    start_time = perf_counter()
    invocations = [task(config.iterations, i) for i in range(config.num_tasks)]
    individual_times = [inv.result for inv in invocations]
    total_time = perf_counter() - start_time

    # Cleanup
    app.runner.stop_runner_loop()
    sleep(0.5)
    thread.join(timeout=5)

    return PerformanceResults(individual_times, total_time, calibration)


def calculate_performance_metrics(
    config: PerformanceTestConfig, results: PerformanceResults, runner: "BaseRunner"
) -> dict:
    """Calculate performance metrics from test results."""
    avg_task_time = sum(results.individual_times) / len(results.individual_times)
    total_cpu_time = sum(results.individual_times)
    calibration_factor = (
        results.calibration_time * config.num_tasks
    ) / results.total_execution_time
    parallelization_factor = total_cpu_time / results.total_execution_time

    return {
        "num_tasks": config.num_tasks,
        "calibration_time": f"{results.calibration_time:.3f}s",
        "avg_task_time": f"{avg_task_time:.3f}s",
        "total_cpu_time": f"{total_cpu_time:.3f}s",
        "wall_clock_time": f"{results.total_execution_time:.3f}s",
        "parallelization_factor": f"{parallelization_factor:.2f}x",
        "calibration_parallelization_factor": f"{calibration_factor:.2f}x",
        "max_parallel_slots": runner.max_parallel_slots,
        "runner_type": runner.__class__.__name__,
        "mem_compatible": runner.mem_compatible(),
        "individual_task_times": [f"{t:.3f}s" for t in results.individual_times],
    }


MIN_CPUS_FOR_PERFORMANCE_TEST = 4


def test_parallel_performance(task_cpu_intensive_no_conc: Task) -> None:
    """Test performance characteristics of different runners."""
    # Skip test if running on a single CPU (CI environments)
    cpu_count = multiprocessing.cpu_count()
    if cpu_count < MIN_CPUS_FOR_PERFORMANCE_TEST:
        pytest.skip(
            f"Performance tests require at least {MIN_CPUS_FOR_PERFORMANCE_TEST} CPU cores "
            f"(found {cpu_count})"
        )
    app = task_cpu_intensive_no_conc.app
    app.conf.logging_level = "info"
    config = get_test_config(app)
    results = run_performance_test(task_cpu_intensive_no_conc, config)

    # Calculate metrics
    performance_data = calculate_performance_metrics(
        config=config, results=results, runner=app.runner
    )

    # Assertions
    if app.runner.mem_compatible():
        calibration_factor = float(
            performance_data["calibration_parallelization_factor"].rstrip("x")
        )
        assert config.expected_min is not None and config.expected_max is not None
        assert config.expected_min <= calibration_factor <= config.expected_max, (
            f"Thread runner calibration_parallelization_factor={calibration_factor:.2f}x "
            f"outside expected range [{config.expected_min}-{config.expected_max}].\n"
            f"Performance data: {performance_data}"
        )
    else:
        parallelization_factor = float(
            performance_data["parallelization_factor"].rstrip("x")
        )
        assert (
            parallelization_factor is not None
            and config.expected_min_parallelization is not None
        )
        assert parallelization_factor >= config.expected_min_parallelization, (
            f"Process runner parallelization factor {parallelization_factor:.2f}x "
            f"below minimum expected {config.expected_min_parallelization:.2f}x "
            f"with max_parallel_slots={performance_data['max_parallel_slots']} "
            f"and num_tasks={performance_data['num_tasks']}\n"
            f"Performance data: {performance_data}"
        )
