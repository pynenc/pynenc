import pytest

from pynenc.call import Call
from pynenc.exceptions import PynencError, RetryError
from pynenc.invocation import ConcurrentInvocation, InvocationStatus
from tests.conftest import MockPynenc

app = MockPynenc()
app.conf.dev_mode_force_sync_tasks = True


@app.task
def add(x: int, y: int) -> int:
    return x + y


def test_distributed_invocation_instantiation() -> None:
    invocation = add(1, 2)
    assert isinstance(invocation, ConcurrentInvocation)
    assert isinstance(invocation.call, Call)


def test_distributed_invocation_no_serializable() -> None:
    invocation = add(1, 2)
    # test raises an exceptions when invocation.to_json()
    with pytest.raises(PynencError):
        invocation.to_json()
    with pytest.raises(PynencError):
        invocation.from_json(app, "")


def test_status_registered() -> None:
    invocation = add(1, 2)
    assert invocation.status == InvocationStatus.REGISTERED


def test_result_success() -> None:
    invocation = add(1, 2)
    assert invocation.result == 3
    assert invocation.status == InvocationStatus.SUCCESS


@app.task(max_retries=1)
def retry() -> int:
    raise RetryError("Test retry")


def test_max_retries() -> None:
    invocation = retry()
    assert invocation.num_retries == 0
    with pytest.raises(RetryError, match="Test retry"):
        _ = invocation.result
    assert invocation.status == InvocationStatus.FAILED


@app.task
def raise_exception() -> None:
    raise RuntimeError("Test exception")


def test_exception() -> None:
    invocation = raise_exception()
    with pytest.raises(RuntimeError, match="Test exception"):
        _ = invocation.result
    assert invocation.status == InvocationStatus.FAILED


@pytest.mark.asyncio
async def test_async_result_success() -> None:
    invocation = add(1, 2)
    result = await invocation.async_result()
    assert result == 3
    assert invocation.status == InvocationStatus.SUCCESS


@pytest.mark.asyncio
async def test_async_result_retry() -> None:
    invocation = retry()
    with pytest.raises(RetryError, match="Test retry"):
        await invocation.async_result()
    assert invocation.status == InvocationStatus.FAILED


@pytest.mark.asyncio
async def test_async_result_exception() -> None:
    invocation = raise_exception()
    with pytest.raises(RuntimeError, match="Test exception"):
        await invocation.async_result()
    assert invocation.status == InvocationStatus.FAILED
