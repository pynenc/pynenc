import threading
import time
from typing import Any

import pytest

from pynenc import Pynenc
from pynenc.invocation.dist_invocation import DistributedInvocation, InvocationStatus
from pynenc.runner.thread_runner import ConfigThreadRunner, ThreadRunner

app = Pynenc(config_values={"runner_cls": "ThreadRunner"})


@app.task
def sleeper() -> None:
    time.sleep(0.1)


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
    conf: ConfigThreadRunner = runner.conf  # type: ignore
    runner._on_start()  # initialize runner internals
    conf.final_invocation_cache_size = 10
    # Start the runner in a separate thread
    runner_thread = threading.Thread(target=lambda: app.runner.run(), daemon=True)
    runner_thread.start()

    invocations = waiting.parallelize(
        (i,) for i in range(conf.final_invocation_cache_size * 2)
    )
    _ = list(invocations.results)
    assert len(runner.final_invocation_ids) < conf.final_invocation_cache_size * 2

    runner_thread.join(timeout=0.1)


# Create a minimal dummy invocation for testing
class DummyInvocation:
    def __init__(self) -> None:
        self.invocation_id = "dummy1"
        self._status = InvocationStatus.RUNNING

    def run(self) -> None:
        pass  # No-op for testing

    @property
    def status(self) -> InvocationStatus:
        return self._status


# This test forces thread creation to fail and checks that reroute_invocations is called.
def test_reroute_on_thread_start_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    # Create a Pynenc instance and get its runner (which is a ThreadRunner)
    app = Pynenc(config_values={"runner_cls": "ThreadRunner"})
    runner: ThreadRunner = app.runner  # type: ignore
    runner._on_start()  # initialize runner internals

    dummy = DummyInvocation()

    # Record calls to reroute_invocations
    reroute_calls = []

    def fake_reroute(invocations_to_reroute: DistributedInvocation) -> None:
        reroute_calls.append(invocations_to_reroute)

    monkeypatch.setattr(runner.app.orchestrator, "reroute_invocations", fake_reroute)

    # Monkeypatch threading.Thread so that starting a thread always fails.
    class FakeThread:
        def __init__(self, target: Any, daemon: Any, args: Any = None) -> None:
            self.target = target
            self.daemon = daemon
            self.args = args

        def start(self) -> None:
            raise RuntimeError("Simulated thread start failure")

        def join(self) -> None:
            pass

        def is_alive(self) -> bool:
            return False

    monkeypatch.setattr(
        threading,
        "Thread",
        lambda target, daemon, args=None: FakeThread(target, daemon, args),
    )

    # Force get_invocations_to_run to return our dummy invocation.
    monkeypatch.setattr(
        runner.app.orchestrator,
        "get_invocations_to_run",
        lambda max_num_invocations: [dummy],
    )

    # Run one loop iteration. When the thread fails to start, it should trigger reroute.
    runner.runner_loop_iteration()

    # Check that reroute_invocations was called with the dummy invocation.
    assert reroute_calls, "reroute_invocations was not called"
    # Expect reroute_calls[0] is a set containing dummy
    assert dummy in reroute_calls[0], "Dummy invocation was not rerouted"  # type: ignore
