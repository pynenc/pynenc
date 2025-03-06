from functools import cached_property
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

from pynenc import Pynenc
from pynenc.arg_cache.base_arg_cache import BaseArgCache
from pynenc.broker.base_broker import BaseBroker
from pynenc.call import Call
from pynenc.invocation import DistributedInvocation
from pynenc.orchestrator.base_orchestrator import BaseBlockingControl, BaseOrchestrator
from pynenc.runner.base_runner import BaseRunner
from pynenc.state_backend.base_state_backend import BaseStateBackend

if TYPE_CHECKING:
    from pynenc.task import Task


class MockBroker(BaseBroker):
    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        self.route_invocation_mock = MagicMock()
        self.retrieve_invocation_mock = MagicMock()
        self.count_invocations_mock = MagicMock()
        self.purge_mock = MagicMock()

    def route_invocation(self, *args: Any, **kwargs: Any) -> None:
        self.route_invocation_mock(*args, **kwargs)

    def retrieve_invocation(self, *args: Any, **kwargs: Any) -> Any:
        return self.retrieve_invocation_mock(*args, **kwargs)

    def count_invocations(self) -> Any:
        return self.count_invocations_mock()

    def purge(self) -> None:
        self.purge_mock()


class MockBlockingControl(BaseBlockingControl):
    def __init__(self) -> None:
        self.release_waiters_mock = MagicMock()
        self.waiting_for_results_mock = MagicMock()
        self.get_blocking_invocations_mock = MagicMock()

    def release_waiters(self, *args: Any, **kwargs: Any) -> Any:
        return self.release_waiters_mock(*args, **kwargs)

    def waiting_for_results(self, *args: Any, **kwargs: Any) -> Any:
        return self.waiting_for_results_mock(*args, **kwargs)

    def get_blocking_invocations(self, *args: Any, **kwargs: Any) -> Any:
        return self.get_blocking_invocations_mock(*args, **kwargs)


class MockBaseOrchestrator(BaseOrchestrator):
    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        self._get_existing_invocations_mock = MagicMock()
        self._set_invocation_status_mock = MagicMock()
        self._set_invocation_pending_status_mock = MagicMock()
        self._get_invocation_status_mock = MagicMock()
        self._increment_invocation_retries_mock = MagicMock()
        self._get_invocation_retries_mock = MagicMock()
        self._set_up_invocation_auto_purge_mock = MagicMock()
        self._purge_mock = MagicMock()
        self._cycle_control_mock = MagicMock()
        self._auto_purge_mock = MagicMock()
        self.blocking_control_mock = MockBlockingControl()

    def get_existing_invocations(self, *args: Any, **kwargs: Any) -> Any:
        return self._get_existing_invocations_mock(*args, **kwargs)

    def _set_invocation_status(self, *args: Any, **kwargs: Any) -> Any:
        return self._set_invocation_status_mock(*args, **kwargs)

    def _set_invocation_pending_status(self, *args: Any, **kwargs: Any) -> Any:
        return self._set_invocation_pending_status_mock(*args, **kwargs)

    def get_invocation_status(self, *args: Any, **kwargs: Any) -> Any:
        return self._get_invocation_status_mock(*args, **kwargs)

    def increment_invocation_retries(self, *args: Any, **kwargs: Any) -> Any:
        return self._increment_invocation_retries_mock(*args, **kwargs)

    def get_invocation_retries(self, *args: Any, **kwargs: Any) -> Any:
        return self._get_invocation_retries_mock(*args, **kwargs)

    def set_up_invocation_auto_purge(self, *args: Any, **kwargs: Any) -> Any:
        return self._set_up_invocation_auto_purge_mock(*args, **kwargs)

    @property
    def cycle_control(self) -> Any:
        return self._cycle_control_mock

    @property
    def blocking_control(self) -> Any:
        return self.blocking_control_mock

    @property
    def auto_purge(self) -> Any:
        return self._auto_purge_mock

    def purge(self, *args: Any, **kwargs: Any) -> Any:
        return self._purge_mock(*args, **kwargs)


class MockStateBackend(BaseStateBackend):
    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        self._purge_mock = MagicMock()
        self._upsert_invocation_mock = MagicMock()
        self._get_invocation_mock = MagicMock()
        self._add_history_mock = MagicMock()
        self._get_history_mock = MagicMock()
        self._set_result_mock = MagicMock()
        self._get_result_mock = MagicMock()
        self._set_exception_mock = MagicMock()
        self._get_exception_mock = MagicMock()

    def purge(self) -> None:
        return self._purge_mock()

    def _upsert_invocation(self, *args: Any, **kwargs: Any) -> Any:
        return self._upsert_invocation_mock(*args, **kwargs)

    def _get_invocation(self, *args: Any, **kwargs: Any) -> Any:
        return self._get_invocation_mock(*args, **kwargs)

    def _add_history(self, *args: Any, **kwargs: Any) -> Any:
        return self._add_history_mock(*args, **kwargs)

    def _get_history(self, *args: Any, **kwargs: Any) -> Any:
        return self._get_history_mock(*args, **kwargs)

    def _set_result(self, *args: Any, **kwargs: Any) -> Any:
        return self._set_result_mock(*args, **kwargs)

    def _get_result(self, *args: Any, **kwargs: Any) -> Any:
        return self._get_result_mock(*args, **kwargs)

    def _set_exception(self, *args: Any, **kwargs: Any) -> Any:
        return self._set_exception_mock(*args, **kwargs)

    def _get_exception(self, invocation: DistributedInvocation) -> Exception:
        return self._get_exception_mock(invocation)


class MockArgCache(BaseArgCache):
    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        # Reset all mocks in init to ensure clean state
        self._store_mock = MagicMock()
        self._retrieve_mock = MagicMock()
        self._purge_mock = MagicMock()

    def _store(self, *args: Any, **kwargs: Any) -> None:
        self._store_mock(*args, **kwargs)

    def _retrieve(self, *args: Any, **kwargs: Any) -> str:
        return self._retrieve_mock(*args, **kwargs)

    def _purge(self) -> None:
        self._purge_mock()


class MockRunner(BaseRunner):
    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        self._on_start_mock = MagicMock()
        self._on_stop_mock = MagicMock()
        self._on_stop_runner_loop_mock = MagicMock()
        self.runner_loop_iteration_mock = MagicMock()
        self._waiting_for_results_mock = MagicMock()
        self._max_parallel_slots = 2

    @staticmethod
    def mem_compatible() -> bool:
        return True

    def _on_start(self) -> None:
        return self._on_start_mock()

    def _on_stop(self) -> None:
        return self._on_stop_mock()

    def _on_stop_runner_loop(self) -> None:
        return self._on_stop_runner_loop_mock()

    def runner_loop_iteration(self) -> None:
        return self.runner_loop_iteration_mock()

    def _waiting_for_results(self, *args: Any, **kwargs: Any) -> None:
        return self._waiting_for_results_mock(*args, **kwargs)

    @property
    def max_parallel_slots(self) -> int:
        return self._max_parallel_slots

    @property
    def cache(self) -> dict:
        if self._runner_cache is None:
            self._runner_cache = {}
        return self._runner_cache


class MockPynenc(Pynenc):
    def __init__(self, app_id: str = "mock_pynenc") -> None:
        super().__init__(app_id=app_id)
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

    @cached_property
    def arg_cache(self) -> MockArgCache:
        return MockArgCache(self)

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


def dummy() -> None:
    ...


@pytest.fixture
def dummy_task(mock_base_app: MockPynenc) -> "Task":
    return mock_base_app.task(dummy)


@pytest.fixture
def dummy_invocation(dummy_task: "Task") -> "DistributedInvocation":
    return DistributedInvocation(Call(dummy_task), None)
