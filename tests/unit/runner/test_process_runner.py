import signal
from multiprocessing.managers import DictProxy, SyncManager
from typing import Generator
from unittest.mock import ANY, Mock, patch

import pytest

from pynenc import Task
from pynenc.exceptions import RunnerError
from pynenc.invocation import DistributedInvocation, InvocationStatus
from pynenc.runner.process_runner import ProcessRunner
from tests.conftest import MockPynenc


def add(x: int, y: int) -> int:
    return x + y


@pytest.fixture
def app() -> MockPynenc:
    return MockPynenc()


@pytest.fixture
def runner(app: MockPynenc) -> ProcessRunner:
    runner = ProcessRunner(app)
    return runner


@pytest.fixture
def add_task(runner: ProcessRunner) -> Task:
    return runner.app.task(add)


@pytest.fixture
def mock_process() -> Generator[Mock, None, None]:
    with patch("pynenc.runner.process_runner.Process") as mock:
        yield mock


@pytest.fixture
def mock_manager() -> Generator[SyncManager, None, None]:
    managed_dict_mock: dict = {}
    with patch("pynenc.runner.process_runner.Manager") as mock_manager:
        mock_manager.return_value.dict.return_value = managed_dict_mock
        yield mock_manager


def test_on_start(runner: ProcessRunner) -> None:
    runner._on_start()
    assert isinstance(runner.manager, SyncManager)
    assert isinstance(runner.wait_invocation, DictProxy)


def test_on_stop(mock_process: Mock, runner: ProcessRunner) -> None:
    runner._on_start()
    mock_invocation = Mock(spec=DistributedInvocation)
    runner.processes[mock_invocation] = mock_process
    runner._on_stop()

    mock_process.kill.assert_called_once()


def test_runner_loop_iteration(
    mock_process: Mock, add_task: Task, runner: ProcessRunner
) -> None:
    runner._on_start()
    runner.app.orchestrator.get_invocations_to_run = Mock(return_value=[add_task(1, 2)])  # type: ignore
    runner.runner_loop_iteration()
    runner.app.orchestrator.get_invocations_to_run.assert_called_once()
    mock_process.assert_called_once()
    assert len(runner.processes) == 1


def test_runner_loop_iteration_pause_waiting_invocations(
    mock_process: Mock,
    add_task: Task,
    runner: ProcessRunner,
    mock_manager: SyncManager,
    app: MockPynenc,
) -> None:
    runner._on_start()
    result_inv: DistributedInvocation = add_task(1, 2)  # type: ignore
    waiting_inv: DistributedInvocation = add_task(3, 4)  # type: ignore
    runner.wait_invocation[result_inv] = {waiting_inv}
    runner.processes[waiting_inv] = mock_process
    app.orchestrator.get_invocations_to_run = Mock(return_value=[])  # type: ignore
    with patch("os.kill") as mock_kill:
        # test that is the result_inv is still running, the waiting_inv is paused
        app.orchestrator.get_invocation_status.return_value = InvocationStatus.RUNNING
        runner.runner_loop_iteration()
        mock_kill.assert_called_with(ANY, signal.SIGSTOP)
        # test that is the result_inv is finished, the waiting_inv is resumed
        app.orchestrator.get_invocation_status.return_value = InvocationStatus.SUCCESS
        runner.runner_loop_iteration()
        mock_kill.assert_called_with(ANY, signal.SIGCONT)


def test_waiting_for_results(
    runner: ProcessRunner,
    add_task: Task,
    mock_process: Mock,
    mock_manager: SyncManager,
) -> None:
    runner._on_start()
    running_invocation: DistributedInvocation = add_task(1, 2)  # type: ignore
    result_invocation: DistributedInvocation = add_task(3, 4)  # type: ignore

    with patch(
        "pynenc.orchestrator.base_orchestrator.BaseOrchestrator.set_invocation_status",
        return_value=[add_task(1, 2)],
    ) as mock_set_invocation_status:
        runner.waiting_for_results(
            running_invocation,
            [result_invocation],
            {"wait_invocation": runner.wait_invocation},
        )
        # check that the invocation status is set to PAUSED
        mock_set_invocation_status.assert_called_once_with(
            running_invocation, InvocationStatus.PAUSED
        )
        # check that the waiting invocation is stored in the wait_invocation dict
        assert result_invocation in runner.wait_invocation
        assert running_invocation in runner.wait_invocation[result_invocation]


def test_waiting_for_results_returns_without_waiting_invocation(
    runner: ProcessRunner,
    add_task: Task,
    mock_manager: SyncManager,
) -> None:
    runner._on_start()
    running_invocation: DistributedInvocation = add_task(1, 2)  # type: ignore

    with patch(
        "pynenc.orchestrator.base_orchestrator.BaseOrchestrator.set_invocation_status",
        return_value=[add_task(1, 2)],
    ) as mock_set_invocation_status:
        runner.waiting_for_results(
            running_invocation, [], {"wait_invocation": runner.wait_invocation}
        )
        # check that the invocation status is set to PAUSED
        mock_set_invocation_status.assert_called_once_with(
            running_invocation, InvocationStatus.PAUSED
        )
        # there was no invocation to wait, the wait_invocation dict should be empty
        assert len(runner.wait_invocation) == 0


def test_waiting_for_results_no_args_error(
    runner: ProcessRunner,
    mock_manager: SyncManager,
    add_task: Task,
) -> None:
    runner._on_start()
    running_invocation: DistributedInvocation = add_task(1, 2)  # type: ignore
    result_invocation: DistributedInvocation = add_task(3, 4)  # type: ignore

    with patch(
        "pynenc.orchestrator.base_orchestrator.BaseOrchestrator.set_invocation_status",
        return_value=[add(1, 2)],
    ) as mock_set_invocation_status:
        with pytest.raises(RunnerError):
            runner.waiting_for_results(running_invocation, [result_invocation])
        # check that the invocation status is set to PAUSED
        mock_set_invocation_status.assert_called_once_with(
            running_invocation, InvocationStatus.PAUSED
        )
        # there was no invocation to wait, the wait_invocation dict should be empty
        assert len(runner.wait_invocation) == 0


def test_waiting_for_results_no_running_invocation(runner: ProcessRunner) -> None:
    runner._on_start()
    with patch("time.sleep") as mock_sleep:
        runner.conf.invocation_wait_results_sleep_time_sec = -2323
        runner.waiting_for_results(None, [])
        mock_sleep.assert_any_call(-2323)


def test_waiting_processes_property(
    runner: ProcessRunner, mock_manager: SyncManager, add_task: Task
) -> None:
    runner._on_start()
    # runner.wait_invocation: dict
    assert runner.wait_invocation == {}
    assert runner.waiting_processes == 0
    inv0: DistributedInvocation = add_task(1, 2)  # type: ignore
    inv1: DistributedInvocation = add_task(3, 4)  # type: ignore
    inv3: DistributedInvocation = add_task(5, 6)  # type: ignore
    inv4: DistributedInvocation = add_task(7, 8)  # type: ignore
    # 2 invocations are waiting
    runner.wait_invocation = {inv0: {inv1}, inv3: {inv4}}
    assert runner.waiting_processes == 2
    # 3 invocations waiting on the same invocation
    runner.wait_invocation = {inv0: {inv1, inv3, inv4}}
    assert runner.waiting_processes == 3
    # 1 invocation waiting on 2 invocations
    runner.wait_invocation = {inv0: {inv1}, inv3: {inv1}}
    assert runner.waiting_processes == 1
