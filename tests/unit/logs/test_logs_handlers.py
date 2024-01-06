import threading

from _pytest.logging import LogCaptureFixture

from pynenc import Pynenc
from pynenc.runner.mem_runner import ThreadRunner

app = Pynenc()
app.runner = ThreadRunner(app)
app.conf.logging_level = "DEBUG"


@app.task
def add(x: int, y: int) -> int:
    add.logger.info(f"(in task log)adding {x} + {y}")
    return x + y


def test_task_runner_logs(caplog: LogCaptureFixture) -> None:
    """
    Test that the logs will add runner, task and invocations ids
    """

    def run_in_thread() -> None:
        add.app.runner.run()

    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()

    invocation = add(1, 2)

    with caplog.at_level("DEBUG"):
        assert invocation.result == 3
        add.app.runner.stop_runner_loop()
        thread.join()
        in_task_log, runner_log = None, None
        for record in caplog.records:
            if "(in task log)" in record.message:
                in_task_log = record.message
            elif "[runner" in record.message:
                runner_log = record.message
        # Check that in-task logs contains task and invocation ids
        assert in_task_log is not None
        assert invocation.task.task_id in in_task_log
        assert invocation.invocation_id in in_task_log
        # Check that logs in the runner contains the runner id
        assert runner_log is not None
        assert invocation.app.runner.runner_id in runner_log
