import threading
from typing import TYPE_CHECKING

import pytest

from pynenc.runner.thread_runner import ThreadRunner
from pynenc_tests.conftest import MockPynenc
from pynenc_tests.util import capture_logs

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest

    from pynenc import Pynenc

mock_app = MockPynenc()


@mock_app.task
def add(x: int, y: int) -> int:
    add.logger.info(f"(in task log)adding {x} + {y}")
    return x + y


@pytest.fixture
def app(request: "FixtureRequest", app_instance: "Pynenc") -> "Pynenc":
    app = app_instance
    app.runner = ThreadRunner(app)
    app._tasks = mock_app._tasks
    app.conf.logging_level = "DEBUG"
    add.app = app
    app.purge()
    request.addfinalizer(app.purge)
    return app


@pytest.fixture
def app_no_truncate(request: "FixtureRequest", app_instance: "Pynenc") -> "Pynenc":
    """App fixture with truncate_log_ids disabled."""
    app = app_instance
    app.runner = ThreadRunner(app)
    app._tasks = mock_app._tasks
    app.conf.logging_level = "DEBUG"
    app.conf.truncate_log_ids = False
    add.app = app
    app.purge()
    request.addfinalizer(app.purge)
    return app


def test_task_runner_logs_truncated(app: "Pynenc") -> None:
    """
    Test that logs truncate long IDs by default for readability.
    """

    def run_in_thread() -> None:
        app.runner.run()

    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()

    # Start capturing logs BEFORE invoking the task to avoid race conditions
    # where the task completes before the context manager is entered
    with capture_logs(app.logger) as log_buffer:
        invocation = add(1, 2)
        assert invocation.result == 3
        app.runner.stop_runner_loop()
        thread.join()

    # Get all log lines (outside context to ensure buffer is complete)
    log_lines = log_buffer.getvalue().splitlines()
    in_task_log: str | None = None
    runner_log: str | None = None

    for line in log_lines:
        if "(in task log)" in line:
            in_task_log = line
        elif "[runner" in line.lower() or "ThreadRunner" in line:
            runner_log = line

    # Check that in-task logs contains task id
    assert in_task_log is not None, "Task log message not found"
    assert invocation.task.task_id.module in in_task_log
    assert invocation.task.task_id.func_name in in_task_log
    # Invocation ID should be truncated (first 7 chars, like Git short SHA)
    assert invocation.invocation_id[:7] in in_task_log

    # Check that logs in the runner contains the runner class name
    assert runner_log is not None, "Runner log message not found"
    assert "ThreadRunner" in runner_log


def test_task_runner_logs_full_ids(app_no_truncate: "Pynenc") -> None:
    """
    Test that logs show full IDs when truncate_log_ids is disabled.
    """
    app = app_no_truncate

    def run_in_thread() -> None:
        app.runner.run()

    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()

    # Start capturing logs BEFORE invoking the task to avoid race conditions
    # where the task completes before the context manager is entered
    with capture_logs(app.logger) as log_buffer:
        invocation = add(1, 2)
        assert invocation.result == 3
        app.runner.stop_runner_loop()
        thread.join()

    # Get all log lines (outside context to ensure buffer is complete)
    log_lines = log_buffer.getvalue().splitlines()
    in_task_log: str | None = None
    runner_log: str | None = None

    for line in log_lines:
        if "(in task log)" in line:
            in_task_log = line
        elif "[runner" in line.lower() or "ThreadRunner" in line:
            runner_log = line

    # Check that in-task logs contains task and FULL invocation ids
    assert in_task_log is not None, "Task log message not found"
    assert invocation.task.task_id.module in in_task_log
    assert invocation.task.task_id.func_name in in_task_log
    assert invocation.invocation_id in in_task_log

    # Check that logs in the runner contains the FULL runner id
    assert runner_log is not None, "Runner log message not found"
    assert app.runner.runner_id in runner_log
