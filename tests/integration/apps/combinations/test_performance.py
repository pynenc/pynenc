import multiprocessing
import threading
from dataclasses import dataclass
from time import perf_counter, sleep
from typing import TYPE_CHECKING, NamedTuple

import pytest

from pynenc import Task
from pynenc.runner.process_runner import ProcessRunner
from tests.util import create_test_logger

if TYPE_CHECKING:
    from pynenc import Pynenc
    from pynenc.runner.base_runner import BaseRunner


logger = create_test_logger(__name__)


@dataclass
class PerformanceTestConfig:
    """Configuration for performance tests based on runner type."""

    runtime_sec: float
    num_tasks: int
    expected_min: float = 0.0
    expected_max: float | None = None
    expected_min_parallelization: float | None = None


# ################################################################################### #
# TEST CONFIG GENERATION: Get test configuration based on runner type
# ################################################################################### #


def get_test_config(app: "Pynenc") -> PerformanceTestConfig:
    """
    Get test configuration based on runner type.
    The timing constraint ensures each task runs for a fixed period, so parallelization is measured by wall clock time vs total CPU time.
    For thread runners, allow a higher maximum calibration factor since tasks may overlap more than expected due to GIL and scheduling.
    This test is designed to verify that Pynenc does not introduce excessive overhead, regardless of how much CPU each task acquires.
    """
    if app.runner.mem_compatible():
        # Thread runner - expect nearly sequential execution due to GIL, but allow higher parallelization factor due to time constraint
        return PerformanceTestConfig(
            runtime_sec=0.5, num_tasks=5, expected_min=0.7, expected_max=2.2
        )
    elif isinstance(app.runner, ProcessRunner):
        return PerformanceTestConfig(
            runtime_sec=0.5,
            num_tasks=multiprocessing.cpu_count(),
            expected_min_parallelization=0.5,
        )
    else:
        return PerformanceTestConfig(
            runtime_sec=0.5,
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
    individual_iters: list[int]
    calibration_iters: int


def run_performance_test(
    task: Task, config: PerformanceTestConfig
) -> PerformanceResults:
    """Execute performance test with given configuration."""
    app = task.app
    thread = threading.Thread(target=lambda: app.runner.run(), daemon=True)
    thread.start()

    logger.info(f"Starting warm-up run with {config.runtime_sec}s runtime")
    warmup_start, warmup_end, warmup_time, warmup_iters = task(
        min_runtime_sec=config.runtime_sec
    ).result
    logger.info(
        f"Warm-up: start={warmup_start:.3f}, end={warmup_end:.3f}, elapsed={warmup_time:.3f}s, iterations={warmup_iters} for {config.runtime_sec}s runtime"
    )

    logger.info(f"Starting calibration run with {config.runtime_sec}s runtime")
    calib_start, calib_end, calibration_time, calibration_iters = task(
        min_runtime_sec=config.runtime_sec
    ).result
    logger.info(
        f"Calibration: start={calib_start:.3f}, end={calib_end:.3f}, elapsed={calibration_time:.3f}s, iterations={calibration_iters} for {config.runtime_sec}s runtime"
    )

    start_time = perf_counter()
    logger.info(f"Starting parallel test with {config.num_tasks} tasks")
    invocations = [
        task(min_runtime_sec=config.runtime_sec, call_id=i)
        for i in range(config.num_tasks)
    ]
    individual_results = [inv.result for inv in invocations]
    for idx, (start, end, elapsed, iters) in enumerate(individual_results):
        logger.info(
            f"  Task {idx}: start={start:.3f}, end={end:.3f}, elapsed={elapsed:.3f}s, iterations={iters}"
        )
    individual_times = [res[2] for res in individual_results]
    individual_iters = [res[3] for res in individual_results]
    total_time = perf_counter() - start_time

    app.runner.stop_runner_loop()
    sleep(0.5)
    thread.join(timeout=5)

    results = PerformanceResults(
        individual_times,
        total_time,
        calibration_time,
        individual_iters,
        calibration_iters,
    )
    return results


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
    avg_iters = (
        sum(getattr(results, "individual_iters", [0] * config.num_tasks))
        / config.num_tasks
    )
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
        "individual_task_iters": getattr(results, "individual_iters", []),
        "calibration_iters": getattr(results, "calibration_iters", 0),
        "avg_task_iters": avg_iters,
    }


MIN_CPUS_FOR_PERFORMANCE_TEST = 4


def test_parallel_performance(task_cpu_intensive_no_conc: Task) -> None:
    """
    Test performance characteristics of different runners.
    Each task runs for a fixed period (runtime_sec), so parallelization is measured by comparing wall clock time to total CPU time.
    The calibration factor for thread runners may exceed 1.0 due to time-based exit, not true sequential execution.
    This test ensures Pynenc does not introduce excessive overhead, regardless of CPU acquisition by tasks.
    """
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

    # Display detailed results before assertions
    logger.info("\n===== Performance Results =====")
    logger.info(
        f"Calibration: {performance_data['calibration_time']} iterations={performance_data['calibration_iters']}"
    )
    for idx, (t, iters) in enumerate(
        zip(
            performance_data["individual_task_times"],
            performance_data["individual_task_iters"],
        )
    ):
        logger.info(f"  Task {idx}: time={t}, iterations={iters}")
    logger.info(f"Avg task iterations: {performance_data['avg_task_iters']:.0f}")
    logger.info(f"Wall clock time: {performance_data['wall_clock_time']}")
    logger.info(f"Parallelization factor: {performance_data['parallelization_factor']}")
    logger.info(
        f"Calibration parallelization factor: {performance_data['calibration_parallelization_factor']}"
    )
    logger.info("==============================\n")

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
