from functools import cached_property

import pytest

from pynenc import Pynenc
from pynenc.orchestrator.mem_orchestrator import MemOrchestrator, TaskInvocationCache
from pynenc.invocation import DistributedInvocation, InvocationStatus


class MockPynenc(Pynenc):
    @cached_property
    def orchestrator(self) -> MemOrchestrator:
        return MemOrchestrator(self)


@pytest.fixture(autouse=True)
def setup_vars() -> None:
    pytest.app = MockPynenc()
    pytest.mem_orc = pytest.app.orchestrator
    pytest.task_cache = TaskInvocationCache()

    @pytest.app.task
    def dummy_task(arg: str) -> str:
        return arg

    pytest.task = dummy_task


def test_cache_set_status() -> None:
    pass
