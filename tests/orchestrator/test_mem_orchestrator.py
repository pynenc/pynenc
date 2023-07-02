from dataclasses import dataclass

import pytest

from pynenc.arguments import Arguments
from pynenc.orchestrator.mem_orchestrator import TaskInvocationCache
from pynenc.invocation import DistributedInvocation, InvocationStatus
from tests.conftest import MockPynenc


@dataclass
class Vars:
    cache: TaskInvocationCache
    inv1: DistributedInvocation
    inv2: DistributedInvocation
    inv3: DistributedInvocation
    invocation_ids: set[str]


@pytest.fixture
def test_vars() -> Vars:
    """Test the implementation of abstract methods:
    set_invocation_status, get_existing_invocations
    """
    app = MockPynenc()

    @app.task
    def dummy(arg0: str, arg1: str) -> str:
        return f"{arg0}:{arg1}"

    inv1: DistributedInvocation = DistributedInvocation(
        dummy, Arguments(dummy.func, arg0="a", arg1="a")
    )
    inv2: DistributedInvocation = DistributedInvocation(
        dummy, Arguments(dummy.func, arg0="a", arg1="b")
    )
    inv3: DistributedInvocation = DistributedInvocation(
        dummy, Arguments(dummy.func, arg0="a", arg1="a")
    )
    cache = TaskInvocationCache[str]()
    cache.set_status(inv1, status=InvocationStatus.REGISTERED)
    cache.set_status(inv2, status=InvocationStatus.SUCCESS)
    cache.set_status(inv3, status=InvocationStatus.SUCCESS)
    invocation_ids = {inv1.invocation_id, inv2.invocation_id, inv3.invocation_id}
    return Vars(cache, inv1, inv2, inv3, invocation_ids)


def test_cache_set_status(test_vars: Vars) -> None:
    pass
    # This is already tested in the test_subclasses_***
    # So do nothing for now
    # Because that test will not work for RedisOrchestrator an others
    # and will need a refactor to reuse these tests in all subclasses
