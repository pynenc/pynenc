import signal
import time
from collections.abc import Callable, Generator
from typing import Any
from unittest.mock import ANY, Mock, PropertyMock, patch

import pytest

from pynenc import Task
from pynenc.invocation import BaseInvocation, DistributedInvocation
from pynenc.runner.persistent_process_runner import (
    PersistentProcessRunner,
    persistent_process_main,
)
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
        mock.return_value.is_alive.return_value = True
        mock.return_value.pid = 12345  # Mock a PID for logging checks
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
    process_extra_id = "test-process"
    runner_cache: dict[str, Any] = {}
    stop_event = mock_manager.return_value.Event.return_value

    def simulate_sigterm(signum: int, frame: Any) -> None:
        stop_event.set()

    with patch("signal.signal") as mock_signal:
        mock_signal.side_effect = lambda signum, handler: simulate_sigterm(signum, None)
        with patch.object(app.orchestrator, "get_invocations_to_run", return_value=[]):
            stop_event.is_set.side_effect = [False, True]  # Ensure loop exits
            persistent_process_main(
                app,
                process_extra_id=process_extra_id,
                runner_cache=runner_cache,
                stop_event=stop_event,
            )
            mock_signal.assert_called_once_with(signal.SIGTERM, ANY)
            stop_event.set.assert_called_once()


# ---- PersistentProcessRunner Tests ----


def test_on_start_initializes_processes(
    runner: PersistentProcessRunner, mock_manager: Mock, mock_process: Mock
) -> None:
    runner.conf.num_processes = 2
    runner._on_start()
    assert len(runner.processes) == 2
    assert mock_process.call_count == 2
    assert isinstance(runner.runner_cache, dict)
    assert runner.stop_event is not None


def test_spawn_persistent_process(
    runner: PersistentProcessRunner, mock_process: Mock, mock_manager: Mock
) -> None:
    runner._on_start()
    with patch.object(runner.logger, "info") as mock_logger:
        process_key = runner._spawn_persistent_process()
        assert process_key in runner.processes
        assert mock_process.called
        mock_logger.assert_any_call(
            f"Spawned persistent process {process_key} with pid 12345"
        )


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
    runner.processes = {
        "proc1": mock_process.return_value,
        "proc2": mock_process.return_value,
    }

    runner._terminate_all_processes()
    runner.stop_event.set.assert_called()  # type: ignore
    for proc in runner.processes.values():
        proc.terminate.assert_called_once()  # type: ignore
        proc.join.assert_called_once_with(timeout=5)  # type: ignore
    assert len(runner.processes) == 0


def test_on_stop(
    runner: PersistentProcessRunner, mock_process: Mock, mock_manager: Mock
) -> None:
    runner._on_start()
    runner.processes = {"proc1": mock_process.return_value}
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
    runner.processes = {"proc1": alive_proc, "proc2": dead_proc}

    def mock_spawn() -> str:
        process_key = "procX"
        runner.processes[process_key] = Mock(is_alive=Mock(return_value=True))
        return process_key

    with patch.object(runner, "_spawn_persistent_process", side_effect=mock_spawn):
        runner.runner_loop_iteration()
        assert "proc2" not in runner.processes
        assert len(runner.processes) == 2


def test_runner_loop_iteration_no_action_if_all_alive(
    runner: PersistentProcessRunner, mock_process: Mock, mock_manager: Mock
) -> None:
    runner._on_start()
    runner.num_processes = 2
    runner.processes = {
        "proc1": Mock(is_alive=Mock(return_value=True)),
        "proc2": Mock(is_alive=Mock(return_value=True)),
    }
    with patch.object(runner, "_spawn_persistent_process") as mock_spawn:
        runner.runner_loop_iteration()
        mock_spawn.assert_not_called()
        assert len(runner.processes) == 2


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


def test_cache_property(runner: PersistentProcessRunner, mock_manager: Mock) -> None:
    # Manually initialize runner_cache without spawning processes
    runner.manager = mock_manager.return_value
    runner.runner_cache = runner.manager.dict()  # type: ignore
    assert runner.cache is runner.runner_cache


def test_generate_process_extra_id(
    runner: PersistentProcessRunner, app: MockPynenc
) -> None:
    with patch.object(
        PersistentProcessRunner, "runner_id", new_callable=PropertyMock
    ) as mock_runner_id:
        mock_runner_id.return_value = "test-runner"
        key1 = runner._generate_process_extra_id()
        key2 = runner._generate_process_extra_id()
        assert key1 != key2
        assert runner._process_id_counter == 2


def test_persistent_process_main_handle_terminate(
    app: MockPynenc, mock_manager: Mock
) -> None:
    """Test that SIGTERM triggers the handle_terminate function"""
    runner_cache: dict[str, Any] = {}
    stop_event = mock_manager.return_value.Event.return_value

    def simulate_sigterm(signum: int, frame: Any) -> None:
        stop_event.set()

    with patch("signal.signal") as mock_signal:
        mock_signal.side_effect = simulate_sigterm
        with patch.object(app.orchestrator, "get_invocations_to_run", return_value=[]):
            # First iteration runs, second triggers SIGTERM effect
            stop_event.is_set.side_effect = [False, True]
            persistent_process_main(
                app,
                process_extra_id="test-process",
                runner_cache=runner_cache,
                stop_event=stop_event,
            )
            mock_signal.assert_called_once_with(signal.SIGTERM, ANY)


def test_on_stop_runner_loop(
    runner: PersistentProcessRunner, mock_process: Mock, mock_manager: Mock
) -> None:
    """Test the on_stop_runner_loop method"""
    runner._on_start()
    runner.processes = {
        "proc1": mock_process.return_value,
        "proc2": mock_process.return_value,
    }
    with patch.object(runner, "_terminate_all_processes") as mock_terminate:
        runner._on_stop_runner_loop()
        mock_terminate.assert_called_once()


def test_spawn_persistent_process_failure(
    runner: PersistentProcessRunner, mock_process: Mock, mock_manager: Mock
) -> None:
    """Test error handling when process spawning fails"""
    runner._on_start()
    # Mock the Process.start() method to raise an exception
    mock_process.return_value.start.side_effect = Exception("Process creation failed")

    with pytest.raises(Exception, match="Process creation failed"):
        runner._spawn_persistent_process()


def test_persistent_process_main_sigterm_calls_handle_terminate(
    app: MockPynenc, mock_manager: Mock
) -> None:
    """Test that receiving SIGTERM explicitly calls handle_terminate and stops the process"""
    process_extra_id = "test-process"
    runner_cache: dict[str, Any] = {}
    stop_event = mock_manager.return_value.Event.return_value

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
                    "process_extra_id": process_extra_id,
                    "runner_cache": runner_cache,
                    "stop_event": stop_event,
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
