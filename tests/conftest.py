from unittest.mock import MagicMock
from functools import cached_property
from typing import TYPE_CHECKING

import pytest

from pynenc import Pynenc
from pynenc.arguments import Arguments
from pynenc.orchestrator.base_orchestrator import BaseOrchestrator
from pynenc.broker.base_broker import BaseBroker
from pynenc.invocation import DistributedInvocation
from pynenc.runner import BaseRunner
from pynenc.state_backend.base_state_backend import BaseStateBackend
from pynenc.runner.base_runner import BaseRunner

if TYPE_CHECKING:
    from pynenc.task import Task


class MockBroker(BaseBroker):
    _route_invocation = MagicMock()
    _retrieve_invocation = MagicMock()

    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        self._route_invocation.reset_mock()
        self._retrieve_invocation.reset_mock()


class MockBaseOrchestrator(BaseOrchestrator):
    get_existing_invocations = MagicMock()
    set_invocation_status = MagicMock()
    set_invocations_status = MagicMock()
    get_invocation_status = MagicMock()
    waiting_for_result = MagicMock()

    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        self.get_existing_invocations.reset_mock()
        self.set_invocation_status.reset_mock()
        self.set_invocations_status.reset_mock()
        self.get_invocation_status.reset_mock()
        self.waiting_for_result.reset_mock()


class MockStateBackend(BaseStateBackend):
    _upsert_invocation = MagicMock()
    _get_invocation = MagicMock()
    _add_history = MagicMock()
    _get_history = MagicMock()
    _set_result = MagicMock()
    _get_result = MagicMock()
    _set_exception = MagicMock()
    _get_exception = MagicMock()

    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        self._upsert_invocation.reset_mock()
        self._get_invocation.reset_mock()
        self._add_history.reset_mock()
        self._get_history.reset_mock()
        self._set_result.reset_mock()
        self._get_result.reset_mock()
        self._set_exception.reset_mock()
        self._get_exception.reset_mock()


class MockRunner(BaseRunner):
    _on_start = MagicMock()
    _on_stop = MagicMock()
    runner_loop_iteration = MagicMock()
    waiting_for_result = MagicMock()

    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        self._on_start.reset_mock()
        self._on_stop.reset_mock()
        self.runner_loop_iteration.reset_mock()
        self.waiting_for_result.reset_mock()


class MockPynenc(Pynenc):
    def __init__(self) -> None:
        super().__init__()
        self._runner_instance: MockRunner = MockRunner(self)

    @cached_property
    def broker(self) -> MockBroker:
        return MockBroker(self)

    @cached_property
    def orchestrator(self) -> MockBaseOrchestrator:
        return MockBaseOrchestrator(self)

    @cached_property
    def state_backend(self) -> MockStateBackend:
        return MockStateBackend(self)

    @property  # type: ignore
    def runner(self) -> MockRunner:
        return self._runner_instance

    @runner.setter
    def runner(
        self, runner_instance: MockRunner
    ) -> None:  # This matches the base setter signature
        self._runner_instance = runner_instance


@pytest.fixture
def mock_base_app() -> MockPynenc:
    return MockPynenc()


@pytest.fixture
def dummy_task(mock_base_app: MockPynenc) -> "Task":
    @mock_base_app.task
    def dummy() -> None:
        ...

    return dummy


@pytest.fixture
def dummy_invocation(dummy_task: "Task") -> "DistributedInvocation":
    return DistributedInvocation(dummy_task, Arguments(dummy_task.func))
