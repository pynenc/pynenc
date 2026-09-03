import asyncio
import threading
import time
from unittest.mock import Mock, patch

import pytest

from pynenc.conf.validation_atomic_service import AtomicServiceConfigError
from pynenc.conf.config_runner import QueueSelectionStrategy
from pynenc.exceptions import RunnerNotExecutableError
from pynenc.runner.base_runner import DummyRunner
from pynenc_tests.conftest import MockPynenc
from pynenc_tests.util import capture_logs

mock_base_app = MockPynenc.with_id("pynenc_tests/unit/runner/test_base_runner.py")
queue_validation_app = MockPynenc.with_id(
    "pynenc_tests/unit/runner/test_base_runner_queue_validation.py"
)


@queue_validation_app.task(queue="payments")
def queued_payment_task() -> None:
    pass


def test_run() -> None:
    """Test that the runner method will always call on_start and on_stop"""

    def run_in_thread() -> None:
        mock_base_app.runner.run()

    # Create a thread to run the loop
    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    time.sleep(0.1)
    mock_base_app.runner.stop_runner_loop()
    thread.join()
    mock_base_app.runner._on_start.assert_called_once()
    mock_base_app.runner.runner_loop_iteration.assert_called()
    mock_base_app.runner._on_stop.assert_called_once()


def test_exception_handling_in_run_method() -> None:
    """Test that a general exception in the runner loop is logged and raised"""
    exception_message = "Test Exception"
    mock_base_app.runner.runner_loop_iteration.side_effect = Exception(
        exception_message
    )

    with capture_logs(mock_base_app.logger) as log_buffer:
        with pytest.raises(Exception) as exc_info:
            mock_base_app.runner.run()

        assert exception_message in str(exc_info.value)
        log_output = log_buffer.getvalue()
        assert "Exception in runner" in log_output
        assert exception_message in log_output


def test_keyboard_interrupt_handling_in_run_method() -> None:
    with capture_logs(mock_base_app.logger) as log_buffer:
        mock_base_app.runner.runner_loop_iteration.side_effect = KeyboardInterrupt
        mock_base_app.runner.run()
        log_output = log_buffer.getvalue()
        assert "KeyboardInterrupt received" in log_output
        assert "Stopping runner" in log_output


def test_runner_start_validates_atomic_service_config() -> None:
    # spread >= interval → no slot even for a single runner → hard failure
    app = MockPynenc(
        {
            "atomic_service_interval_minutes": 1.0,
            "atomic_service_spread_margin_minutes": 1.0,
        }
    )

    with pytest.raises(AtomicServiceConfigError, match="spread_margin"):
        app.runner.on_start()

    app.runner._on_start.assert_not_called()


def test_runner_start_does_not_validate_task_queues() -> None:
    queue_validation_app.runner.on_start()

    queue_validation_app.runner._on_start.assert_called_once()
    queue_validation_app.runner.on_stop()


def test_runner_start_keeps_queue_selection_permissive() -> None:
    app = MockPynenc.with_id(
        "pynenc_tests/unit/runner/test_base_runner_unknown_consumed_queue.py"
    )
    app.runner.conf.queues = ("undeclared",)

    with capture_logs(app.logger) as log_buffer:
        app.runner.on_start()

    assert app.runner.conf.queues == ("undeclared",)
    assert "not configured in broker.queues" not in log_buffer.getvalue()
    app.runner._on_start.assert_called_once()
    app.runner.on_stop()


def test_runner_queue_selection_round_robin_remembers_latest_queue() -> None:
    app = MockPynenc.with_id("test_runner_queue_selection_round_robin")
    runner = DummyRunner(app)
    app.broker.conf.queues = ("default", "payments", "reports")
    runner.conf.queues = ("default", "payments", "reports")
    runner.conf.queue_selection_strategy = QueueSelectionStrategy.ROUND_ROBIN

    assert runner.queue_names_for_retrieval() == ("default", "payments", "reports")
    runner.note_queue_retrieved("default")
    assert runner.queue_names_for_retrieval() == ("payments", "reports", "default")
    runner.note_queue_retrieved("reports")
    assert runner.queue_names_for_retrieval() == ("default", "payments", "reports")


def test_runner_queue_selection_round_robin_advances_only_after_success() -> None:
    app = MockPynenc.with_id("test_runner_queue_selection_round_robin_no_success")
    runner = DummyRunner(app)
    app.broker.conf.queues = ("default", "payments", "reports")
    runner.conf.queues = ("default", "payments", "reports")
    runner.conf.queue_selection_strategy = QueueSelectionStrategy.ROUND_ROBIN

    assert runner.queue_names_for_retrieval() == ("default", "payments", "reports")
    assert runner.queue_names_for_retrieval() == ("default", "payments", "reports")
    runner.note_queue_retrieved("payments")
    assert runner.queue_names_for_retrieval() == ("reports", "default", "payments")


def test_runner_empty_queue_selection_uses_broker_queue_order() -> None:
    app = MockPynenc.with_id("test_runner_queue_selection_default_broker_queues")
    runner = DummyRunner(app)
    app.broker.conf.queues = ("default", "payments", "reports")
    runner.conf.queues = ()
    runner.conf.queue_selection_strategy = QueueSelectionStrategy.ROUND_ROBIN

    assert runner.queue_names_for_retrieval() == ("default", "payments", "reports")
    runner.note_queue_retrieved("default")
    assert runner.queue_names_for_retrieval() == ("payments", "reports", "default")


def test_runner_queue_selection_ordered_always_starts_first_queue() -> None:
    app = MockPynenc.with_id("test_runner_queue_selection_ordered")
    runner = DummyRunner(app)
    app.broker.conf.queues = ("default", "payments", "reports")
    runner.conf.queues = ("default", "payments", "reports")
    runner.conf.queue_selection_strategy = QueueSelectionStrategy.ORDERED

    assert runner.queue_names_for_retrieval() == ("default", "payments", "reports")
    runner.note_queue_retrieved("default")
    assert runner.queue_names_for_retrieval() == ("default", "payments", "reports")


def test_runner_queue_selection_random_shuffles_queue_attempts() -> None:
    app = MockPynenc.with_id("test_runner_queue_selection_random")
    runner = DummyRunner(app)
    app.broker.conf.queues = ("default", "payments", "reports")
    runner.conf.queues = ("default", "payments", "reports")
    runner.conf.queue_selection_strategy = QueueSelectionStrategy.RANDOM

    with patch("pynenc.runner.base_runner.random.shuffle") as mock_shuffle:
        mock_shuffle.side_effect = lambda queues: queues.reverse()
        assert runner.queue_names_for_retrieval() == ("reports", "payments", "default")


def test_dummy_runner() -> None:
    """Test that the dummy runner cannot be run"""
    mock_base_app.runner = DummyRunner(mock_base_app)  # type: ignore
    with pytest.raises(RunnerNotExecutableError):
        mock_base_app.runner.run()
    with pytest.raises(RunnerNotExecutableError):
        mock_base_app.runner.on_start()
    with pytest.raises(RunnerNotExecutableError):
        mock_base_app.runner.on_stop()
    with pytest.raises(RunnerNotExecutableError):
        mock_base_app.runner._on_stop_runner_loop()
    with pytest.raises(RunnerNotExecutableError):
        mock_base_app.runner.runner_loop_iteration()
    with pytest.raises(RunnerNotExecutableError):
        _ = mock_base_app.runner.max_parallel_slots
    with pytest.raises(RunnerNotExecutableError):
        mock_base_app.runner.mem_compatible()
    with pytest.raises(RunnerNotExecutableError):
        mock_base_app.runner._on_stop_runner_loop()


@patch("pynenc.runner.base_runner.time.sleep")
def test_dummy_runner_waiting_for_result(mock_sleep: Mock) -> None:
    runner = DummyRunner(mock_base_app)
    runner.conf.invocation_wait_results_sleep_time_sec = -1313
    mock_sleep.reset_mock()
    runner.waiting_for_results(None, None)  # type: ignore
    mock_sleep.assert_any_call(-1313)


@pytest.mark.asyncio
async def test_dummy_runner_async_waiting_for_result() -> None:
    """Test async waiting for results in dummy runner."""
    runner = DummyRunner(mock_base_app)
    runner.conf.invocation_wait_results_sleep_time_sec = 0.1  # Small delay for testing

    with capture_logs(mock_base_app.logger) as log_buffer:
        await runner.async_waiting_for_results(None, [])  # type: ignore

        log_output = log_buffer.getvalue()
        assert "Async Waiting for" in log_output
        assert "from outside this runner" in log_output


@pytest.mark.asyncio
async def test_async_waiting_for_results_with_sleep_time() -> None:
    """Test that async_waiting_for_results respects configured sleep time."""
    runner = DummyRunner(mock_base_app)
    sleep_time = 0.1
    runner.conf.invocation_wait_results_sleep_time_sec = sleep_time

    start_time = asyncio.get_event_loop().time()
    await runner.async_waiting_for_results(None, [])  # type: ignore
    elapsed_time = asyncio.get_event_loop().time() - start_time

    assert elapsed_time >= sleep_time, "Sleep time was not respected"


def test_all_runners_can_be_instantiated() -> None:
    """Test that all concrete runner classes can be instantiated."""
    from pynenc.runner.base_runner import BaseRunner

    # Get all subclasses recursively
    def get_all_subclasses(cls: type) -> list[type]:
        subclasses: list[type] = []
        for c in cls.__subclasses__():
            if "mock" in c.__name__.lower():
                continue
            subclasses.append(c)
            subclasses.extend(get_all_subclasses(c))
        return subclasses

    runners = get_all_subclasses(BaseRunner)

    assert runners, "No runner subclasses found"

    for runner_class in runners:
        try:
            runner = runner_class(mock_base_app)
            # Check that the instance is created successfully
            assert isinstance(runner, BaseRunner)
        except Exception as e:
            pytest.fail(f"Failed to instantiate {runner_class.__name__}: {str(e)}")
