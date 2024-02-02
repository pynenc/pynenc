import threading

import tests.unit.orchestrator.orchestrator_tasks as tasks


def any_run_in_parallel(results: list[tasks.SleepResult]) -> bool:
    """Check if any tasks in the list ran in parallel (overlapping times)."""
    sorted_results = sorted(results, key=lambda x: x.start)
    for i in range(1, len(sorted_results)):
        if sorted_results[i].start < sorted_results[i - 1].end:
            tasks.app.logger.warning(
                f"Overlap: {sorted_results[i - 1]} and {sorted_results[i]}"
            )
            return True  # Found an overlap, tasks ran in parallel
    return False  # No overlap found, tasks did not run in parallel


def test_running_concurrency() -> None:
    """checks that the concurrency control will prevent running multiple tasks at the same time"""
    # first purge any invocation pending from previous runs
    tasks.app.purge()
    # TODO remove after fixing the bug: https://github.com/pynenc/pynenc/issues/45
    tasks.app.orchestrator.conf.cycle_control = False

    # start a runner
    def run_in_thread() -> None:
        tasks.app.conf.runner_cls = "ThreadRunner"
        tasks.app.runner.run()

    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()

    tasks.app.logger.info("check that without control runs in parallel")
    no_control_invocations = [
        tasks.sleep_without_running_concurrency(0.1) for _ in range(10)
    ]
    no_control_results = [i.result for i in no_control_invocations]
    if not any_run_in_parallel(no_control_results):
        raise ValueError(f"Expected parallel execution, got {no_control_results}")

    tasks.app.logger.info("check that with control does not run in parallel")
    controlled_invocations = [
        tasks.sleep_with_running_concurrency(0.1) for _ in range(10)
    ]
    controlled_results = [i.result for i in controlled_invocations]
    if any_run_in_parallel(controlled_results):
        raise ValueError(f"Expected sequential execution, got {controlled_results}")

    # stop the runner
    tasks.app.runner.stop_runner_loop()
    thread.join()
