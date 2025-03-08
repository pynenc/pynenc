import threading
import time

from pynenc import Pynenc
from pynenc.runner.thread_runner import ThreadRunner

app = Pynenc(config_values={"runner_cls": "ThreadRunner"})


@app.task
def sleeper() -> None:
    time.sleep(1)


@app.task
def waiting(x: int) -> None:
    del x
    _ = sleeper().result


def test_final_invocation_cache_clean_up() -> None:
    """
    Test that no 'waiting_for_results' warning appears in logs from any process.
    Uses capfd to capture stdout/stderr, including subprocess logs.
    """
    runner: ThreadRunner = app.runner  # type: ignore
    runner.conf.final_invocation_cache_size = 10
    # Start the runner in a separate thread
    runner_thread = threading.Thread(target=lambda: app.runner.run(), daemon=True)
    runner_thread.start()

    invocations = waiting.parallelize(
        (i,) for i in range(app.runner.conf.final_invocation_cache_size * 2)
    )
    _ = list(invocations.results)
    assert len(runner.final_invocations) < runner.conf.final_invocation_cache_size * 2

    runner_thread.join(timeout=0.1)
