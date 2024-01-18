from unittest.mock import patch

import pytest

from pynenc.invocation import DistributedInvocation
from pynenc.state_backend.redis_state_backend import RedisStateBackend
from tests.conftest import MockPynenc

app = MockPynenc()


@app.task
def add(x: int, y: int) -> int:
    return x + y


def test_get_invocation_found() -> None:
    state_backend = RedisStateBackend(app)

    # Mock a DistributedInvocation
    inv: DistributedInvocation = add(1, 2)  # type: ignore
    serialized_invocation = inv.to_json()

    # Mock the Redis client's get method to return the serialized invocation
    with patch.object(
        state_backend.client, "get", return_value=serialized_invocation.encode()
    ):
        retrieved_invocation: DistributedInvocation = state_backend._get_invocation(
            inv.invocation_id
        )

    # Check that the retrieved invocation matches the mock invocation
    assert retrieved_invocation.invocation_id == inv.invocation_id


def test_get_invocation_not_found() -> None:
    state_backend = RedisStateBackend(app)

    # Mock the Redis client's get method to return None
    with patch.object(state_backend.client, "get", return_value=None):
        with pytest.raises(KeyError) as exc_info:
            state_backend._get_invocation("non_existing_id")

    # Check that the error message contains the invocation ID
    assert "non_existing_id" in str(exc_info.value)


def test_get_result_not_found(mock_base_app: "MockPynenc") -> None:
    state_backend = RedisStateBackend(mock_base_app)
    inv: DistributedInvocation = add(1, 2)  # type: ignore

    with patch.object(state_backend.client, "get", return_value=None):
        with pytest.raises(KeyError) as exc_info:
            state_backend._get_result(inv)

    assert f"Result for invocation {inv.invocation_id}" in str(exc_info.value)


def test_get_result_found(mock_base_app: "MockPynenc") -> None:
    state_backend = RedisStateBackend(mock_base_app)
    inv: DistributedInvocation = add(1, 2)  # type: ignore
    serialized_result = "3"

    with patch.object(
        state_backend.client, "get", return_value=serialized_result.encode()
    ):
        retrieved_result = state_backend._get_result(inv)

    assert retrieved_result == 3


def test_get_exception_not_found(mock_base_app: "MockPynenc") -> None:
    state_backend = RedisStateBackend(mock_base_app)
    inv: DistributedInvocation = add(1, 2)  # type: ignore

    # Mock the Redis client's get method to return None
    with patch.object(state_backend.client, "get", return_value=None):
        with pytest.raises(KeyError) as exc_info:
            state_backend._get_exception(inv)

    # Check that the error message contains the invocation ID
    assert f"Exception for invocation {inv.invocation_id}" in str(exc_info.value)
