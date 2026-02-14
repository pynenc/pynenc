import signal
import time
from collections.abc import Callable, Generator
from typing import Any
from unittest.mock import ANY, Mock, patch

import pytest

from pynenc import Task
from pynenc.invocation import BaseInvocation, DistributedInvocation
from pynenc.runner.persistent_process_runner import (
    PersistentProcessRunner,
    persistent_process_main,
)
from pynenc.runner.runner_context import RunnerContext
from pynenc_tests.conftest import MockPynenc


def add(x: int, y: int) -> int:
    return x + y


@pytest.fixture
def app() -> MockPynenc:
    return MockPynenc()


@pytest.fixture
def runner(app: MockPynenc) -> PersistentProcessRunner:
    runner = PersistentProcessRunner(app)
    runner.running = True  # Set running to True to allow spawning processes
    runner._process_id_counter = 0  # Initialize _process_id_counter
    return runner


@pytest.fixture
def add_task(runner: PersistentProcessRunner) -> Task:
    return runner.app.task(add)


@pytest.fixture
def mock_process() -> Generator[Mock, None, None]:
    with patch("pynenc.runner.persistent_process_runner.Process") as mock:
        # Track all created process mocks for inspection
        created_mocks: list[Mock] = []
        pid_counter = iter(range(12345, 12345 + 100))

        def create_process_mock(*args: Any, **kwargs: Any) -> Mock:
            process_mock = Mock()
            process_mock.is_alive.return_value = True
            process_mock.pid = next(pid_counter)
            created_mocks.append(process_mock)
            return process_mock

        mock.side_effect = create_process_mock
        mock.created_mocks = created_mocks  # Allow tests to access created mocks
        yield mock


@pytest.fixture
def mock_manager() -> Generator[Mock, None, None]:
    with patch("pynenc.runner.persistent_process_runner.Manager") as mock_manager:
        mock_event = Mock()
        mock_event.is_set.return_value = False
        mock_manager.return_value.dict.return_value = {}
        mock_manager.return_value.Event.return_value = mock_event
        yield mock_manager


# ---- persistent_process_main Tests ----


def test_persistent_process_main_handles_sigterm(
    app: MockPynenc, mock_manager: Mock
) -> None:
    runner_cache: dict[str, Any] = {}
    stop_event = mock_manager.return_value.Event.return_value

    # Create a parent RunnerContext and serialize to JSON
    parent_runner_ctx = RunnerContext(
        runner_cls="PersistentProcessRunner",
        runner_id="PersistentProcessRunner@test-host-123",
        pid=123,
        hostname="test-host",
    )
    child_runner_id = "test-child-runner-id"

    def simulate_sigterm(signum: int, frame: Any) -> None:
        stop_event.set()

    with patch("signal.signal") as mock_signal:
        mock_signal.side_effect = lambda signum, handler: simulate_sigterm(signum, None)
        with patch.object(app.orchestrator, "get_invocations_to_run", return_value=[]):
            stop_event.is_set.side_effect = [False, True]  # Ensure loop exits
            persistent_process_main(
                app,
                runner_cache=runner_cache,
                stop_event=stop_event,
                parent_runner_ctx_json=parent_runner_ctx.to_json(),
                child_runner_id=child_runner_id,
            )
            mock_signal.assert_called_once_with(signal.SIGTERM, ANY)
            stop_event.set.assert_called_once()


# ---- PersistentProcessRunner Tests ----


def test_on_start_initializes_processes(
    runner: PersistentProcessRunner, mock_manager: Mock, mock_process: Mock
) -> None:
    runner.conf.num_processes = 2
    runner._on_start()
    assert len(runner.child_runner_ids) == 2
    assert mock_process.call_count == 2
    assert isinstance(runner.runner_cache, dict)
    assert runner.stop_event is not None


def test_spawn_persistent_process(
    runner: PersistentProcessRunner, mock_process: Mock, mock_manager: Mock
) -> None:
    runner._on_start()
    initial_count = len(runner.child_runner_ids)
    child_runner_id = runner._spawn_persistent_process()
    assert child_runner_id is not None
    assert child_runner_id in runner.child_runner_ids
    assert len(runner.child_runner_ids) == initial_count + 1
    assert mock_process.called


def test_spawn_persistent_process_after_stop(
    runner: PersistentProcessRunner, mock_process: Mock, mock_manager: Mock
) -> None:
    runner._on_start()
    runner.running = False
    with pytest.raises(
        RuntimeError, match="Trying to spawn new process after stopping loop"
    ):
        runner._spawn_persistent_process()


def test_terminate_all_processes(
    runner: PersistentProcessRunner, mock_process: Mock, mock_manager: Mock
) -> None:
    runner._on_start()
    # Create mock processes and assign to child_runner_ids
    mock_proc1 = Mock()
    mock_proc1.is_alive.return_value = True
    mock_proc2 = Mock()
    mock_proc2.is_alive.return_value = True
    runner.child_runner_ids = {
        "runner-1": mock_proc1,
        "runner-2": mock_proc2,
    }

    runner._terminate_all_processes()
    runner.stop_event.set.assert_called()  # type: ignore
    mock_proc1.terminate.assert_called_once()
    mock_proc1.join.assert_called_once_with(timeout=5)
    mock_proc2.terminate.assert_called_once()
    mock_proc2.join.assert_called_once_with(timeout=5)
    assert len(runner.child_runner_ids) == 0


def test_on_stop(
    runner: PersistentProcessRunner, mock_process: Mock, mock_manager: Mock
) -> None:
    runner._on_start()
    mock_proc = Mock()
    mock_proc.is_alive.return_value = True
    runner.child_runner_ids = {"runner-1": mock_proc}
    with patch.object(runner, "_terminate_all_processes") as mock_terminate:
        runner._on_stop()
        mock_terminate.assert_called_once()
        # Type ignore due to Manager being mocked
        runner.manager.shutdown.assert_called_once()  # type: ignore


def test_runner_loop_iteration_replaces_dead_processes(
    runner: PersistentProcessRunner, mock_process: Mock, mock_manager: Mock
) -> None:
    runner._on_start()
    runner.num_processes = 2
    dead_proc = Mock(is_alive=Mock(return_value=False))
    alive_proc = Mock(is_alive=Mock(return_value=True))
    runner.child_runner_ids = {"alive-runner": alive_proc, "dead-runner": dead_proc}

    def mock_spawn() -> str:
        new_runner_id = "new-spawned-runner"
        runner.child_runner_ids[new_runner_id] = Mock(is_alive=Mock(return_value=True))
        return new_runner_id

    with patch.object(runner, "_spawn_persistent_process", side_effect=mock_spawn):
        runner.runner_loop_iteration()
        assert "dead-runner" not in runner.child_runner_ids
        assert len(runner.child_runner_ids) == 2


def test_runner_loop_iteration_no_action_if_all_alive(
    runner: PersistentProcessRunner, mock_process: Mock, mock_manager: Mock
) -> None:
    runner._on_start()
    runner.num_processes = 2
    runner.child_runner_ids = {
        "runner-1": Mock(is_alive=Mock(return_value=True)),
        "runner-2": Mock(is_alive=Mock(return_value=True)),
    }
    with patch.object(runner, "_spawn_persistent_process") as mock_spawn:
        runner.runner_loop_iteration()
        mock_spawn.assert_not_called()
        assert len(runner.child_runner_ids) == 2


def test_waiting_for_results(runner: PersistentProcessRunner, add_task: Task) -> None:
    # Avoid starting processes by not calling _on_start
    runner.num_processes = 1  # Set to avoid real process spawning
    running_invocation: BaseInvocation[Any, Any] = add_task(1, 2)
    result_invocation: BaseInvocation[Any, Any] = add_task(3, 4)

    with patch("time.sleep") as mock_sleep:
        runner.waiting_for_results(
            Mock(spec=DistributedInvocation, base_invocation=running_invocation),
            [Mock(spec=DistributedInvocation, base_invocation=result_invocation)],
        )
        mock_sleep.assert_called_with(
            runner.conf.invocation_wait_results_sleep_time_sec
        )


def test_max_parallel_slots(
    runner: PersistentProcessRunner, mock_manager: Mock
) -> None:
    # Avoid starting processes
    runner.num_processes = 3
    assert runner.max_parallel_slots == 3


def test_mem_compatible(runner: PersistentProcessRunner) -> None:
    assert runner.mem_compatible() is False


def test_persistent_process_main_handle_terminate(
    app: MockPynenc, mock_manager: Mock
) -> None:
    """Test that SIGTERM triggers the handle_terminate function"""
    runner_cache: dict[str, Any] = {}
    stop_event = mock_manager.return_value.Event.return_value

    # Create a parent RunnerContext and serialize to JSON
    parent_runner_ctx = RunnerContext(
        runner_cls="PersistentProcessRunner",
        runner_id="PersistentProcessRunner@test-host-123",
        pid=123,
        hostname="test-host",
    )
    child_runner_id = "test-child-runner-id"

    def simulate_sigterm(signum: int, frame: Any) -> None:
        stop_event.set()

    with patch("signal.signal") as mock_signal:
        mock_signal.side_effect = simulate_sigterm
        with patch.object(app.orchestrator, "get_invocations_to_run", return_value=[]):
            # First iteration runs, second triggers SIGTERM effect
            stop_event.is_set.side_effect = [False, True]
            persistent_process_main(
                app,
                runner_cache=runner_cache,
                stop_event=stop_event,
                parent_runner_ctx_json=parent_runner_ctx.to_json(),
                child_runner_id=child_runner_id,
            )
            mock_signal.assert_called_once_with(signal.SIGTERM, ANY)


def test_on_stop_runner_loop(
    runner: PersistentProcessRunner, mock_process: Mock, mock_manager: Mock
) -> None:
    """Test the on_stop_runner_loop method"""
    runner._on_start()
    mock_proc1 = Mock()
    mock_proc1.is_alive.return_value = True
    mock_proc2 = Mock()
    mock_proc2.is_alive.return_value = True
    runner.child_runner_ids = {
        "runner-1": mock_proc1,
        "runner-2": mock_proc2,
    }
    with patch.object(runner, "_terminate_all_processes") as mock_terminate:
        runner._on_stop_runner_loop()
        mock_terminate.assert_called_once()


def test_spawn_persistent_process_failure(
    runner: PersistentProcessRunner, mock_process: Mock, mock_manager: Mock
) -> None:
    """Test error handling when process spawning fails"""

    # Configure the mock to fail on start BEFORE _on_start is called
    def create_failing_process(*args: Any, **kwargs: Any) -> Mock:
        process_mock = Mock()
        process_mock.is_alive.return_value = True
        process_mock.pid = 99999
        process_mock.start.side_effect = Exception("Process creation failed")
        return process_mock

    mock_process.side_effect = create_failing_process

    with pytest.raises(Exception, match="Process creation failed"):
        runner._on_start()  # This will try to spawn processes and fail


def test_persistent_process_main_sigterm_calls_handle_terminate(
    app: MockPynenc, mock_manager: Mock
) -> None:
    """Test that receiving SIGTERM explicitly calls handle_terminate and stops the process"""
    runner_cache: dict[str, Any] = {}
    stop_event = mock_manager.return_value.Event.return_value

    # Create a parent RunnerContext and serialize to JSON
    parent_runner_ctx = RunnerContext(
        runner_cls="PersistentProcessRunner",
        runner_id="PersistentProcessRunner@test-host-123",
        pid=123,
        hostname="test-host",
    )
    child_runner_id = "test-child-runner-id"

    # Capture the actual handler function that gets registered
    registered_handler: Callable[[int, Any], None] | None = None

    def capture_handler(signum: int, handler: Callable[[int, Any], None]) -> None:
        nonlocal registered_handler
        registered_handler = handler

    with patch("signal.signal") as mock_signal:
        mock_signal.side_effect = capture_handler
        with patch.object(app.orchestrator, "get_invocations_to_run", return_value=[]):
            stop_event.is_set.side_effect = [False, True]

            # Start the process main function in a separate thread to allow signal simulation
            import threading

            process_thread = threading.Thread(
                target=persistent_process_main,
                kwargs={
                    "app": app,
                    "runner_cache": runner_cache,
                    "stop_event": stop_event,
                    "parent_runner_ctx_json": parent_runner_ctx.to_json(),
                    "child_runner_id": child_runner_id,
                },
            )
            process_thread.start()

            # Give it a moment to start and register the signal handler
            time.sleep(0.1)

            # Simulate SIGTERM by calling the registered handler directly
            assert registered_handler is not None, "Signal handler was not registered"
            registered_handler(signal.SIGTERM, None)

            # Wait for the thread to complete
            process_thread.join(timeout=1.0)
            assert not process_thread.is_alive(), "Process thread failed to terminate"

            mock_signal.assert_called_once_with(signal.SIGTERM, ANY)
            stop_event.set.assert_called_once()


def test_get_active_child_runner_ids(
    runner: PersistentProcessRunner, mock_process: Mock, mock_manager: Mock
) -> None:
    """Test that get_active_child_runner_ids returns only alive child runners."""
    runner._on_start()

    # Create mock processes with different alive states
    alive_proc = Mock(is_alive=Mock(return_value=True))
    dead_proc = Mock(is_alive=Mock(return_value=False))
    runner.child_runner_ids = {
        "alive-runner-1": alive_proc,
        "alive-runner-2": alive_proc,
        "dead-runner": dead_proc,
    }

    active_ids = runner.get_active_child_runner_ids()

    assert "alive-runner-1" in active_ids
    assert "alive-runner-2" in active_ids
    assert "dead-runner" not in active_ids
    assert len(active_ids) == 2


def test_get_active_child_runner_ids_empty_before_start(
    runner: PersistentProcessRunner,
) -> None:
    """Test that get_active_child_runner_ids returns empty list before _on_start."""
    # Don't call _on_start - child_runner_ids attribute doesn't exist yet
    active_ids = runner.get_active_child_runner_ids()
    assert active_ids == []
