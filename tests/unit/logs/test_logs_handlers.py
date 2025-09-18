import threading
from typing import TYPE_CHECKING, Optional

import pytest

from pynenc.runner.thread_runner import ThreadRunner
from tests.conftest import MockPynenc
from tests.util import capture_logs

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


def test_task_runner_logs(app: "Pynenc") -> None:
    """
    Test that the logs will add runner, task and invocations ids
    """

    def run_in_thread() -> None:
        app.runner.run()

    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()

    invocation = add(1, 2)

    with capture_logs(app.logger) as log_buffer:
        assert invocation.result == 3
        app.runner.stop_runner_loop()
        thread.join()

        # Get all log lines
        log_lines = log_buffer.getvalue().splitlines()
        in_task_log: Optional[str] = None
        runner_log: Optional[str] = None

        for line in log_lines:
            if "(in task log)" in line:
                in_task_log = line
            elif "[runner" in line:
                runner_log = line

        # Check that in-task logs contains task and invocation ids
        assert in_task_log is not None, "Task log message not found"
        assert invocation.task.task_id in in_task_log
        assert invocation.invocation_id in in_task_log

        # Check that logs in the runner contains the runner id
        assert runner_log is not None, "Runner log message not found"
        assert app.runner.runner_id in runner_log
