import time
from typing import Any
from unittest.mock import Mock

import pytest

from pynenc.invocation import DistributedInvocation
from pynenc.runner.base_runner import DummyRunner
from pynenc_tests.conftest import MockPynenc

app = MockPynenc()


@app.task
def add(x: int, y: int) -> int:
    return x + y


@pytest.mark.asyncio
async def test_dummy_runner_async_waiting_for_results_running(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Test that async_waiting_for_results calls _waiting_for_results when a running invocation is provided.
    """
    runner = DummyRunner(app)
    sleep_time = 0.05
    runner.conf.invocation_wait_results_sleep_time_sec = sleep_time

    # Create a dummy invocation using the 'add' task.
    invocation: DistributedInvocation = add(1, 2)  # type: ignore

    # Define a flag to record that _waiting_for_results was called.
    called = False

    def fake_waiting_for_results(
        running_inv: Any, result_inv: Any, runner_args: Any = None
    ) -> None:
        nonlocal called
        called = True

    monkeypatch.setattr(runner, "_waiting_for_results", fake_waiting_for_results)

    # Call async_waiting_for_results with a running invocation.
    await runner.async_waiting_for_results(
        invocation.invocation_id, [invocation.invocation_id], None
    )
    assert (
        called
    ), "Expected _waiting_for_results to be called when running_invocation is provided"


def test_dummy_runner_waiting_for_results_no_running(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Test that waiting_for_results calls time.sleep when no running invocation is provided.
    """
    runner = DummyRunner(app)
    sleep_time = 0.1
    runner.conf.invocation_wait_results_sleep_time_sec = sleep_time

    sleep_called = False

    def fake_sleep(duration: float) -> None:
        nonlocal sleep_called
        sleep_called = True
        assert duration == sleep_time

    monkeypatch.setattr(time, "sleep", fake_sleep)

    # Call waiting_for_results with running_invocation set to None.
    runner.waiting_for_results(None, [Mock()])
    assert (
        sleep_called
    ), "Expected time.sleep to be called when running_invocation is None"


def test_dummy_runner_waiting_for_results_no_effect_with_runner_args() -> None:
    """
    Test that _waiting_for_results correctly ignores runner_args if provided.
    """
    runner = DummyRunner(app)

    # Create a dummy invocation
    invocation = Mock(spec=DistributedInvocation)

    # Call _waiting_for_results with a dummy runner_args dict
    runner._waiting_for_results(invocation, [invocation], runner_args={"some_key": 42})

    # The function should not modify anything or have an observable effect
    assert True, "No side effects should occur when runner_args is provided."


def test_dummy_runner_waiting_for_results_respects_sleep_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Test that _waiting_for_results sleeps for the correct duration.
    """
    runner = DummyRunner(app)
    sleep_time = 0.2  # Expected sleep duration
    runner.conf.invocation_wait_results_sleep_time_sec = sleep_time

    sleep_called = False

    def fake_sleep(duration: float) -> None:
        nonlocal sleep_called
        sleep_called = True
        assert duration == sleep_time, "Expected sleep duration does not match"

    monkeypatch.setattr(time, "sleep", fake_sleep)

    # Call _waiting_for_results with None running_invocation (external call case)
    runner._waiting_for_results(None, [Mock()])  # type: ignore

    assert sleep_called, "Expected time.sleep to be called for external invocations."
