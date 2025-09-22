import time
from multiprocessing import Process
from unittest.mock import Mock, patch

import pytest

from pynenc.conf.config_runner import ConfigMultiThreadRunner
from pynenc.runner.multi_thread_runner import (
    MultiThreadRunner,
    ProcessState,
    ProcessStatus,
    thread_runner_process_main,
)
from pynenc_tests.conftest import MockPynenc


class TestConfig(ConfigMultiThreadRunner):
    min_processes = 2
    max_processes = 4
    enforce_max_processes = False
    invocation_wait_results_sleep_time_sec = 0.1
    idle_timeout_process_sec = 1
    runner_loop_sleep_time_sec = 0.01


@pytest.fixture
def app() -> MockPynenc:
    app = MockPynenc()
    # Override broker's count_invocations for our tests
    app.broker.count_invocations_mock.return_value = 0
    return app


@pytest.fixture
def multi_thread_runner(app: MockPynenc) -> MultiThreadRunner:
    runner = MultiThreadRunner(app)
    runner.conf = TestConfig()
    runner.processes = {}
    runner.shared_status = {}
    return runner


# ---- ProcessStatus Tests ----


@pytest.mark.parametrize(
    "last_update, active_count, state, now, idle_timeout, expected",
    [
        (100.0, 0, ProcessState.IDLE, 200.0, 50.0, True),  # Idle, timeout exceeded
        (100.0, 1, ProcessState.IDLE, 200.0, 50.0, True),  # Idle with tasks
        (100.0, 0, ProcessState.ACTIVE, 200.0, 50.0, False),  # Active, no tasks
        (100.0, 0, ProcessState.IDLE, 110.0, 50.0, False),  # Idle, timeout not exceeded
    ],
)
def test_process_status_is_idle(
    last_update: float,
    active_count: int,
    state: ProcessState,
    now: float,
    idle_timeout: float,
    expected: bool,
) -> None:
    status = ProcessStatus(last_update, active_count, state)
    assert status.is_idle(now, idle_timeout) is expected


# ---- thread_runner_process_main Tests ----


def test_thread_runner_process_main_updates_status(app: MockPynenc) -> None:
    runner_cache: dict = {}
    shared_status: dict[str, ProcessStatus] = {}
    process_key = "test-process"

    # Mock ThreadRunner to avoid actual execution
    with patch("pynenc.runner.multi_thread_runner.ThreadRunner") as mock_runner:
        # Make runner_loop_iteration raise KeyboardInterrupt after one iteration
        mock_runner.return_value.runner_loop_iteration.side_effect = KeyboardInterrupt()

        thread_runner_process_main(
            app,
            runner_cache=runner_cache,
            shared_status=shared_status,
            process_key=process_key,
        )

        assert process_key in shared_status
        status = shared_status[process_key]
        assert isinstance(status, ProcessStatus)
        assert status.state in (ProcessState.ACTIVE, ProcessState.IDLE)


# ---- MultiThreadRunner Tests ----


def test_scale_up_processes_with_pending_tasks(
    multi_thread_runner: MultiThreadRunner,
) -> None:
    # Set pending tasks count in the mocked broker
    multi_thread_runner.app.broker.count_invocations_mock.return_value = 5  # type: ignore
    multi_thread_runner.max_processes = 4

    with patch.object(
        multi_thread_runner, "_spawn_thread_runner_process"
    ) as mock_spawn:
        multi_thread_runner._scale_up_processes()
        # Should spawn up to max_processes (4) processes
        assert mock_spawn.call_count == 4


def test_terminate_idle_processes_respects_min_processes(
    multi_thread_runner: MultiThreadRunner,
) -> None:
    """Test that _terminate_idle_processes keeps min_processes running."""
    # Create 3 idle processes with different idle times
    current_time = time.time()
    for i in range(3):
        proc = Mock(spec=Process)
        proc.is_alive.return_value = True
        key = f"process-{i}"
        multi_thread_runner.processes[key] = proc
        multi_thread_runner.shared_status[key] = ProcessStatus(
            current_time - (100 + i),  # Different timestamps for deterministic order
            0,
            ProcessState.IDLE,
        )

    # Patch terminate and join to avoid actual process operations
    for proc in multi_thread_runner.processes.values():  # type: ignore
        proc.terminate = Mock()
        proc.join = Mock()

    multi_thread_runner._terminate_idle_processes()

    # Verify min_processes (2) processes remain
    assert len(multi_thread_runner.processes) == multi_thread_runner.conf.min_processes
    # Verify the oldest idle process was terminated
    assert "process-0" not in multi_thread_runner.processes
    # Verify the two most recent processes remain
    assert "process-1" in multi_thread_runner.processes
    assert "process-2" in multi_thread_runner.processes


def test_cleanup_dead_processes(multi_thread_runner: MultiThreadRunner) -> None:
    # Add mix of alive and dead processes
    processes = {
        "alive": Mock(spec=Process, is_alive=lambda: True),
        "dead1": Mock(spec=Process, is_alive=lambda: False),
        "dead2": Mock(spec=Process, is_alive=lambda: False),
    }
    multi_thread_runner.processes = processes.copy()  # type: ignore
    multi_thread_runner.shared_status = {k: Mock() for k in processes}

    multi_thread_runner._cleanup_dead_processes()

    assert len(multi_thread_runner.processes) == 1
    assert "alive" in multi_thread_runner.processes
    assert "dead1" not in multi_thread_runner.processes
    assert "dead2" not in multi_thread_runner.processes


def test_waiting_for_results_without_invocation(
    multi_thread_runner: MultiThreadRunner,
) -> None:
    with patch("time.sleep") as mock_sleep:
        multi_thread_runner.waiting_for_results(None, [])
        mock_sleep.assert_called_once_with(
            multi_thread_runner.conf.invocation_wait_results_sleep_time_sec
        )


def test_max_parallel_slots(multi_thread_runner: MultiThreadRunner) -> None:
    """Test max_parallel_slots returns the maximum of min_processes and max_processes."""
    # Given min_processes=2 from TestConfig
    multi_thread_runner.max_processes = 4  # Set max_processes explicitly
    assert multi_thread_runner.max_parallel_slots == 4

    # Test when max_processes is less than min_processes
    multi_thread_runner.max_processes = 1
    assert multi_thread_runner.max_parallel_slots == 2  # Should return min_processes


def test_scale_up_processes_enforce_max(multi_thread_runner: MultiThreadRunner) -> None:
    """Test that _scale_up_processes spawns processes up to max_processes when enforce_max_processes is True."""
    # Configure runner
    multi_thread_runner.conf.enforce_max_processes = True
    multi_thread_runner.max_processes = 3

    # Mock _spawn_thread_runner_process to update processes dict
    spawn_count = 0

    def mock_spawn() -> None:
        nonlocal spawn_count
        spawn_count += 1
        multi_thread_runner.processes[f"mock-process-{spawn_count}"] = Mock()

    with patch.object(
        multi_thread_runner, "_spawn_thread_runner_process", side_effect=mock_spawn
    ):
        multi_thread_runner._scale_up_processes()
        # Should spawn processes until reaching max_processes (3)
        assert spawn_count == 3
        assert len(multi_thread_runner.processes) == 3


def test_scale_up_processes_based_on_queue(
    multi_thread_runner: MultiThreadRunner,
) -> None:
    """Test that _scale_up_processes spawns processes based on queue when enforce_max_processes is False."""
    # Configure runner
    multi_thread_runner.conf.enforce_max_processes = False
    multi_thread_runner.max_processes = 4
    multi_thread_runner.app.broker.count_invocations_mock.return_value = 3  # type: ignore

    with patch.object(
        multi_thread_runner, "_spawn_thread_runner_process"
    ) as mock_spawn:
        multi_thread_runner._scale_up_processes()
        # Should spawn 3 processes (based on queued_invocations)
        assert mock_spawn.call_count == 3


def test_terminate_idle_processes_skips_when_enforce_max_exceeded(
    multi_thread_runner: MultiThreadRunner,
) -> None:
    """Test that _terminate_idle_processes skips when enforce_max_processes and over max."""
    # Configure runner
    multi_thread_runner.conf.enforce_max_processes = True
    multi_thread_runner.max_processes = 2

    # Create 3 processes (more than max_processes)
    for i in range(3):
        proc = Mock(spec=Process)
        proc.is_alive.return_value = True
        proc.terminate = Mock()
        proc.join = Mock()
        key = f"process-{i}"
        multi_thread_runner.processes[key] = proc
        multi_thread_runner.shared_status[key] = ProcessStatus(
            time.time() - 100, 0, ProcessState.IDLE
        )

    multi_thread_runner._terminate_idle_processes()

    # Verify no processes were terminated
    for proc in multi_thread_runner.processes.values():  # type: ignore
        proc.terminate.assert_not_called()


def test_waiting_for_results_logs_warning_with_invocation(
    multi_thread_runner: MultiThreadRunner,
) -> None:
    """Test that waiting_for_results logs warning when called with running_invocation."""
    from pynenc.invocation import DistributedInvocation

    # Create mock invocation
    mock_invocation = Mock(spec=DistributedInvocation)

    with patch.object(multi_thread_runner.logger, "warning") as mock_warning:
        multi_thread_runner.waiting_for_results(
            running_invocation_id=mock_invocation.invocation_id,
            result_invocation_ids=[],
        )

        # Verify warning was logged
        mock_warning.assert_called_once_with(
            "waiting_for_results called on MultiThreadRunner from within a task. "
            "This should be handled by the ThreadRunner instance in the process."
        )


def test_terminate_idle_processes_skips_missing_status(
    multi_thread_runner: MultiThreadRunner,
) -> None:
    """Test that _terminate_idle_processes skips processes with missing status and continues iteration."""
    # Configure runner to allow termination (min_processes = 1)
    multi_thread_runner.conf.min_processes = 1
    current_time = time.time()

    # Process 1 - No status
    proc1 = Mock(spec=Process)
    proc1.is_alive.return_value = True
    proc1.terminate = Mock()
    proc1.join = Mock()
    multi_thread_runner.processes["process-1"] = proc1
    # Deliberately not setting shared_status for process-1

    # Process 2 - With idle status (should be terminated)
    proc2 = Mock(spec=Process)
    proc2.is_alive.return_value = True
    proc2.terminate = Mock()
    proc2.join = Mock()
    multi_thread_runner.processes["process-2"] = proc2
    multi_thread_runner.shared_status["process-2"] = ProcessStatus(
        current_time - 100,  # Old timestamp to ensure idle
        0,
        ProcessState.IDLE,
    )

    # Process 3 - To ensure we're above min_processes
    proc3 = Mock(spec=Process)
    proc3.is_alive.return_value = True
    proc3.terminate = Mock()
    proc3.join = Mock()
    multi_thread_runner.processes["process-3"] = proc3
    multi_thread_runner.shared_status["process-3"] = ProcessStatus(
        current_time,  # Recent timestamp, not idle
        1,
        ProcessState.ACTIVE,
    )

    multi_thread_runner._terminate_idle_processes()

    # Verify process-1 (missing status) wasn't terminated
    proc1.terminate.assert_not_called()
    assert "process-1" in multi_thread_runner.processes

    # Verify process-2 (with idle status) was terminated
    proc2.terminate.assert_called_once()
    proc2.join.assert_called_once()
    assert "process-2" not in multi_thread_runner.processes

    # Verify process-3 (active) wasn't terminated
    proc3.terminate.assert_not_called()
    assert "process-3" in multi_thread_runner.processes


def test_safe_remove_shared_state_handles_errors(
    multi_thread_runner: MultiThreadRunner,
) -> None:
    """Test that _safe_remove_shared_state handles manager shutdown errors gracefully."""
    # Mock shared_status.pop to raise different errors
    test_cases = [
        EOFError("Mock EOF Error"),
        BrokenPipeError("Mock Broken Pipe Error"),
    ]

    for error in test_cases:
        # Setup mock with error
        mock_dict = Mock()
        mock_dict.pop.side_effect = error
        multi_thread_runner.shared_status = mock_dict

        # Verify error is caught and logged
        with patch.object(multi_thread_runner.logger, "debug") as mock_debug:
            # Should not raise exception
            multi_thread_runner._safe_remove_shared_state("test-key")

            # Verify error was logged
            mock_debug.assert_called_once_with(
                "Manager already stopped while removing state for test-key"
            )

        # Verify pop was attempted
        mock_dict.pop.assert_called_once_with("test-key", None)
