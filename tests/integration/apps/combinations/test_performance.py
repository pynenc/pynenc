import multiprocessing
import threading
from dataclasses import dataclass
from time import perf_counter
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
            iterations=10_000_000,
            num_tasks=multiprocessing.cpu_count(),
            expected_min_parallelization=0.5,
        )
    else:
        # MultiThread runner - start one ThreadRunner per process
        # It can handler more tasks with minimal overloading
        return PerformanceTestConfig(
            iterations=300_000,
            num_tasks=multiprocessing.cpu_count() * 100,
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
    app: "Pynenc", task: Task, config: PerformanceTestConfig
) -> PerformanceResults:
    """Execute performance test with given configuration."""
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
    invocations = [task(iterations=config.iterations) for _ in range(config.num_tasks)]
    individual_times = [inv.result for inv in invocations]
    total_time = perf_counter() - start_time

    # Cleanup
    app.runner.stop_runner_loop()
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


MIN_CPUS_FOR_PERFORMANCE_TEST = 2


def test_parallel_performance(app: "Pynenc", task_cpu_intensive_no_conc: Task) -> None:
    """Test performance characteristics of different runners."""
    # Skip test if running on a single CPU (CI environments)
    cpu_count = multiprocessing.cpu_count()
    if cpu_count < MIN_CPUS_FOR_PERFORMANCE_TEST:
        pytest.skip(
            f"Performance tests require at least {MIN_CPUS_FOR_PERFORMANCE_TEST} CPU cores "
            f"(found {cpu_count})"
        )

    app.conf.logging_level = "info"
    config = get_test_config(app)
    results = run_performance_test(app, task_cpu_intensive_no_conc, config)

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


# def test_parallel_performance(app: "Pynenc", task_cpu_intensive_no_conc: Task) -> None:
#     app.conf.logging_level = "info"

#     # Start runner
#     def run_in_thread() -> None:
#         # if isinstance(app.runner.conf, ConfigMultiThreadRunner):
#         #     app.runner.conf.min_processes = 10
#         #     app.runner.conf.max_threads = 1
#         app.runner.run()

#     thread = threading.Thread(target=run_in_thread, daemon=True)
#     thread.start()

#     # First run a single task to calibrate
#     iterations = 600_000
#     calibration = task_cpu_intensive_no_conc(iterations=iterations).result
#     app.logger.info(f"Calibration: {calibration:.3f}s for {iterations} iterations")

#     # Now run the parallel test
#     if app.runner.mem_compatible():
#         # Mem tasks run sequentially (GIL), do not trigger many of them
#         iterations = 600_000
#         num_tasks = 5
#         expected_min = 0.7
#         expected_max = 1.3
#     elif isinstance(app.runner, ProcessRunner):
#         # Subprocesses runner starts a Task per process, we limit tasks to CPU count
#         # to avoid overloading the system and the overhead of starting too many processes
#         iterations = 10_000_000
#         num_tasks = multiprocessing.cpu_count()
#         expected_min_parallelization = 0.5
#     else:
#         # MultiThreadRunner can handle more tasks, as it creates a ThreadRunner per process
#         # It will avoid the overhead of starting too many processes
#         iterations = 600_000
#         num_tasks = multiprocessing.cpu_count() * 10
#         expected_min_parallelization = 0.95
#     invocations = []

#     # Launch all tasks and measure total time
#     start_time = perf_counter()
#     for _ in range(num_tasks):
#         invocations.append(task_cpu_intensive_no_conc(iterations=iterations))

#     # Collect all execution times
#     individual_times = [inv.result for inv in invocations]
#     total_execution_time = perf_counter() - start_time

#     # Stop runner
#     app.runner.stop_runner_loop()
#     thread.join(timeout=5)

#     avg_task_time = sum(individual_times) / len(individual_times)
#     total_cpu_time = sum(individual_times)
#     calibration_parallelization_factor = (
#         calibration * num_tasks
#     ) / total_execution_time
#     parallelization_factor = total_cpu_time / total_execution_time
#     # Use app.runner.max_parallel_slots instead of cpu_count
#     max_parallel_slots = app.runner.max_parallel_slots

#     # Performance metrics for assertion messages
#     performance_data = {
#         "num_tasks": num_tasks,
#         "calibration_time": f"{calibration:.3f}s",
#         "avg_task_time": f"{avg_task_time:.3f}s",
#         "total_cpu_time": f"{total_cpu_time:.3f}s",
#         "wall_clock_time": f"{total_execution_time:.3f}s",
#         "parallelization_factor": f"{parallelization_factor:.2f}x",
#         "calibration_parallelization_factor": f"{calibration_parallelization_factor:.2f}x",
#         "max_parallel_slots": max_parallel_slots,
#         "runner_type": app.runner.__class__.__name__,
#         "mem_compatible": app.runner.mem_compatible(),
#         "individual_task_times": [f"{t:.3f}s" for t in individual_times],
#     }

#     if app.runner.mem_compatible():
#         # For thread runner, expect nearly sequential execution
#         # total_cpu_time ≈ total_execution_time (parallelization factor ≈ 1)
#         assert expected_min <= calibration_parallelization_factor <= expected_max, (
#             f"Thread runner {calibration_parallelization_factor=:.2f}x outside expected range "
#             f"[{expected_min}-{expected_max}].\nPerformance data: {performance_data}"
#         )
#     else:
#         # For process runner, expect parallel execution
#         # total_cpu_time ≈ total_execution_time * cpu_count
#         assert parallelization_factor >= expected_min_parallelization, (
#             f"Process runner parallelization factor {parallelization_factor:.2f}x below minimum expected "
#             f"{expected_min_parallelization:.2f}x with {max_parallel_slots=} and {num_tasks=} "
#             f"\nPerformance data: {performance_data}"
#         )
