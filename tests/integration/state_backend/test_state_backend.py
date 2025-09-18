from datetime import datetime, timezone
from time import sleep
from typing import TYPE_CHECKING, Optional

import pytest

from pynenc.call import Call
from pynenc.exceptions import PynencError
from pynenc.invocation import DistributedInvocation, InvocationStatus
from pynenc.util.subclasses import get_all_subclasses
from tests.conftest import MockPynenc

if TYPE_CHECKING:
    from pynenc import Pynenc
    from pynenc.types import Params, Result


mock_app = MockPynenc()


@mock_app.task
def dummy() -> None:
    ...


@pytest.fixture
def invocation(app_instance: "Pynenc") -> "DistributedInvocation[Params, Result]":
    dummy.app = app_instance
    return DistributedInvocation(Call(dummy), None)


def test_store_invocation(invocation: "DistributedInvocation[Params, Result]") -> None:
    """Test that it will store and retrieve an invocation"""
    app = invocation.app
    app.state_backend.upsert_invocations([invocation])
    # upsert invocation is not blocking, so we need to wait for the async operation
    sleep(0.1)
    retrieved_invocation = app.state_backend.get_invocation(invocation.invocation_id)
    assert invocation == retrieved_invocation


def test_store_history_status(
    invocation: "DistributedInvocation[Params, Result]",
) -> None:
    """Test that it will store and retrieve the status change history"""
    app = invocation.app

    def _check_history(
        invocation_id: str, expected_statuses: list[InvocationStatus]
    ) -> None:
        app.state_backend.wait_for_invocation_async_operations(invocation_id)
        history = app.state_backend.get_history(invocation.invocation_id)
        assert len(history) == len(expected_statuses)
        prev_datetime = datetime.min.replace(tzinfo=timezone.utc)
        for expected_status, inv_hist in zip(expected_statuses, history):
            assert inv_hist.timestamp > prev_datetime
            assert inv_hist.status == expected_status
            prev_datetime = inv_hist.timestamp

    assert [] == app.state_backend.get_history(invocation.invocation_id)
    app.state_backend.add_histories(
        [invocation.invocation_id], status=InvocationStatus.REGISTERED
    )
    _check_history(invocation.invocation_id, [InvocationStatus.REGISTERED])
    app.state_backend.add_histories(
        [invocation.invocation_id], status=InvocationStatus.RUNNING
    )
    _check_history(
        invocation.invocation_id,
        [InvocationStatus.REGISTERED, InvocationStatus.RUNNING],
    )


def test_store_result(invocation: "DistributedInvocation[Params, Result]") -> None:
    """Test that it will store and retrieve a task result"""
    app = invocation.app
    app.state_backend.upsert_invocations([invocation])
    app.state_backend.set_result(invocation.invocation_id, result="res_x")
    assert "res_x" == app.state_backend.get_result(invocation.invocation_id)


def test_set_exception(invocation: "DistributedInvocation[Params, Result]") -> None:
    """Test that can store and retrieve different types of exceptions"""
    app = invocation.app
    test_exception = ValueError("Test exception message")

    # Store the exception using set_exception
    app.state_backend.set_exception(invocation.invocation_id, test_exception)

    # Retrieve the stored exception using get_exception
    retrieved_exception = app.state_backend.get_exception(invocation.invocation_id)

    # Validate that the retrieved exception is the same as the stored one
    assert isinstance(retrieved_exception, ValueError)
    assert str(retrieved_exception) == "Test exception message"


def test_set_pynenc_exceptions(
    invocation: "DistributedInvocation[Params, Result]",
) -> None:
    """Test that can store and retrieve different types of exceptions"""
    app = invocation.app

    def get_init_var_names(cls: type) -> Optional[set[str]]:
        for base in cls.mro():
            if "__init__" in base.__dict__:
                init_method = base.__init__  # type: ignore
                if hasattr(init_method, "__code__"):
                    return set(init_method.__code__.co_varnames)
        return None

    for exception_cls in get_all_subclasses(PynencError):
        # Generate fake data for the exception
        fake_data = {
            "invocation_id": "fake_invocation_id",
            "task_id": "fake_task_id",
            "message": "fake_message",
            "existing_invocation_id": "fake_existing_invocation_id",
            "new_call_id": "fake_new_call_id",
            "diff": "fake_diff",
            "call_ids": ["fake_call_id_1", "fake_call_id_2"],
            # Add more fake data for other fields if needed
        }
        # Filter out the keys that are not in the __init__ of the exception class
        if init_params := get_init_var_names(exception_cls):
            filtered_fake_data = {
                k: v for k, v in fake_data.items() if k in init_params
            }
        else:
            # not necessary to test an Empty exception subclass
            # (we will call the parent class instead)
            continue

        # Create an instance of the exception
        exception_instance = exception_cls(**filtered_fake_data)

        # Store the exception using set_exception
        app.state_backend.set_exception(invocation.invocation_id, exception_instance)

        # Retrieve the stored exception using get_exception
        retrieved_exception = app.state_backend.get_exception(invocation.invocation_id)

        # Validate that the retrieved exception is the same as the stored one
        assert isinstance(retrieved_exception, exception_cls)
        for key, value in filtered_fake_data.items():
            assert getattr(retrieved_exception, key) == value
