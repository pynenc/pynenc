from pynenc import Pynenc, context
from pynenc.runner.process_runner import ProcessRunner
from pynenc.runner.thread_runner import ThreadRunner


def test_runner_context() -> None:
    # app with ProcRunner
    app = Pynenc("common_id")
    proc_runner = ProcessRunner(app)
    # other_app with ThreadRunner
    other_app = Pynenc("common_id")
    thread_runner = ThreadRunner(other_app)
    assert app.runner == proc_runner  # type: ignore
    assert app.runner != thread_runner  # type: ignore
    assert other_app.runner == thread_runner  # type: ignore
    assert other_app.runner != proc_runner  # type: ignore
    context.set_current_runner("common_id", thread_runner)
    assert app.runner == thread_runner  # type: ignore
    assert other_app.runner == thread_runner  # type: ignore
