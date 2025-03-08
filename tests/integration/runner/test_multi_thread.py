import threading
import time
from typing import Any

from pynenc import Pynenc
from pynenc.runner.multi_thread_runner import MultiThreadRunner
from tests.util import capture_logs  # your log capture helper

# [Your config and app setup remains the same]
config = {
    "serializer_cls": "JsonSerializer",
    "runner_cls": "MultiThreadRunner",
    "orchestrator_cls": "RedisOrchestrator",
    "broker_cls": "RedisBroker",
    "state_backend_cls": "RedisStateBackend",
    "cycle_control": False,
    "blocking_control": False,
    "min_processes": 11,
    "runner_loop_sleep_time_sec": 0.01,
    "invocation_wait_results_sleep_time_sec": 0.01,
    "queue_timeout_sec": 1,
    "logging_level": "info",
}

app = Pynenc(config_values=config)


@app.task
def t_inner() -> str:
    time.sleep(0.01)
    return "inner"


@app.task
def t_middle() -> str:
    time.sleep(0.01)
    return f"middle -> {t_inner().result}"


@app.task
def t_outter() -> str:
    return f"outter -> {t_middle().result}"


def test_expected_warning_message(caplog: Any) -> None:
    """
    Test that the warning message in MultiThreadRunner._waiting_for_results
    matches the expected constant.
    Uses caplog to capture logs.

    So we know the following test will not raise a false positive
    """
    runner = MultiThreadRunner(app)

    with capture_logs(app.logger) as log_buffer:
        runner._waiting_for_results(None, [], None)
    log_output = log_buffer.getvalue()

    expected_warning = MultiThreadRunner.WAITING_FOR_RESULTS_WARNING
    assert (
        expected_warning in log_output
    ), f"Expected warning '{expected_warning}' not found in logs: {log_output}"


def test_waiting_for_result_not_call_in_parent_class(capfd: Any) -> None:
    """
    Test that no 'waiting_for_results' warning appears in logs from any process.
    Uses capfd to capture stdout/stderr, including subprocess logs.
    """
    # Start the runner in a separate thread
    runner_thread = threading.Thread(target=lambda: app.runner.run(), daemon=True)
    runner_thread.start()

    # Execute the task
    result = t_outter().result

    # Capture all output (stdout/stderr) from main thread and subprocesses
    captured = capfd.readouterr()

    # Verify the result
    assert result == "outter -> middle -> inner", f"Unexpected result: {result}"

    # Combine stdout and stderr for log checking
    all_output = captured.out + captured.err

    # Check that the warning message is not present
    warning_text = (
        "waiting_for_results called on MultiThreadRunner from within a task. "
        "This should be handled by the ThreadRunner instance in the process."
    )
    assert warning_text not in all_output, f"Warning found in output: {all_output}"

    # Cleanup
    app.runner.stop_runner_loop()
    runner_thread.join(timeout=5)
