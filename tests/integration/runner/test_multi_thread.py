import threading
import time

from pynenc import Pynenc
from tests.util import capture_logs  # your log capture helper

# Define a configuration for using MultiThreadRunner (and other Redis components)
config = {
    "serializer_cls": "JsonSerializer",
    # DEV MODE disabled: use distributed runner
    "runner_cls": "MultiThreadRunner",
    "orchestrator_cls": "RedisOrchestrator",
    "broker_cls": "RedisBroker",
    "state_backend_cls": "RedisStateBackend",
    # Tuning parameters
    "cycle_control": False,
    "blocking_control": False,
    "min_processes": 11,
    "runner_loop_sleep_time_sec": 0.01,
    "invocation_wait_results_sleep_time_sec": 0.01,
    "queue_timeout_sec": 1,
    # Logging level
    "logging_level": "info",
}

# Instantiate the app with the provided configuration.
app = Pynenc(config_values=config)


@app.task
def t_inner() -> str:
    # Simulate some work
    time.sleep(0.01)
    return "inner"


@app.task
def t_middle() -> str:
    # Simulate some work
    time.sleep(0.1)
    return f"middle -> {t_inner().result}"


@app.task
def t_outter() -> str:
    # Call t_inner and return a combined string
    return f"outter -> {t_middle().result}"


def test_no_waiting_warning_in_multithread_runner() -> None:
    """
    This test verifies that when executing tasks with a MultiThreadRunner,
    the waiting_for_results warning is not logged. Specifically, when a task is invoked
    from within a runner process, there should be no log message like:
    "waiting_for_results called on MultiThreadRunner from within a task. This should be handled by the ThreadRunner instance in the process."
    """
    # Start the runner in a separate thread.
    runner_thread = threading.Thread(target=lambda: app.runner.run(), daemon=True)
    runner_thread.start()

    # Capture logs during task execution.
    with capture_logs(app.logger) as log_buffer:
        # Execute the outer task, which depends on the inner task.
        result = t_outter().result

    # Verify that the result is as expected.
    assert result == "outter -> middle -> inner", f"Unexpected result: {result}"

    # Get the captured log output.
    log_output = log_buffer.getvalue()

    # Assert that the warning message is NOT present in the logs.
    warning_text = (
        "waiting_for_results called on MultiThreadRunner from within a task. "
        "This should be handled by the ThreadRunner instance in the process."
    )
    assert warning_text not in log_output, f"Warning found in logs: {log_output}"

    # Cleanup: stop the runner and join the thread.
    app.runner.stop_runner_loop()
    runner_thread.join(timeout=5)
