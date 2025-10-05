from datetime import datetime
from time import sleep
from typing import TYPE_CHECKING

import pytest

from pynenc.app import AppInfo
from pynenc.exceptions import InvocationNotFoundError
from pynenc.state_backend.base_state_backend import InvocationHistory, InvocationStatus
from pynenc.workflow import WorkflowIdentity
from pynenc_tests.conftest import MockPynenc

if TYPE_CHECKING:
    from pynenc import Pynenc
    from pynenc.invocation import DistributedInvocation

mock_app = MockPynenc()


@mock_app.task
def dummy_task() -> None:
    pass


@pytest.fixture
def invocation(app_instance: "Pynenc") -> "DistributedInvocation":
    """Helper to create a dummy invocation."""
    dummy_task.app = app_instance
    invocation: "DistributedInvocation" = dummy_task()  # type: ignore
    return invocation


def test_history_records_are_stored_and_ordered(app_instance: "Pynenc") -> None:
    """
    Test that invocation history entries are stored with timestamp and status
    and are returned in timestamp order.
    """
    backend = app_instance.state_backend

    # Use the actual InvocationHistory class and set timestamps explicitly
    # This keeps the test simple and ensures type compatibility.
    hist1 = InvocationHistory(InvocationStatus.REGISTERED)
    hist1._timestamp = datetime.fromtimestamp(1.0)

    hist2 = InvocationHistory(InvocationStatus.RUNNING)
    hist2._timestamp = datetime.fromtimestamp(2.0)

    hist3 = InvocationHistory(InvocationStatus.FAILED)
    hist3._timestamp = datetime.fromtimestamp(3.0)

    backend._add_histories(["inv-1"], hist2)
    backend._add_histories(["inv-1"], hist1)
    backend._add_histories(["inv-1"], hist3)

    histories = backend.get_history("inv-1")
    assert [hist.status for hist in histories] == [
        hist1.status,
        hist2.status,
        hist3.status,
    ]


def test_upsert_and_get_invocation(invocation: "DistributedInvocation") -> None:
    """
    Test that upserting an invocation works correctly.
    """
    backend = invocation.app.state_backend

    backend._upsert_invocations([invocation])
    backend._upsert_invocations([invocation])
    backend._upsert_invocations([invocation])
    backend._upsert_invocations([invocation])
    fetched = backend.get_invocation(invocation.invocation_id)
    assert fetched == invocation


def test_get_invocation_raises_when_not_found(app_instance: "Pynenc") -> None:
    """
    Ensure get_invocation raises KeyError for unknown invocation IDs.

    :param "Pynenc" app_instance: Test application instance providing state_backend
    :return: None
    """
    backend = app_instance.state_backend
    invocation_id = "inv-unknown"

    with pytest.raises(InvocationNotFoundError):
        backend.get_invocation(invocation_id)


def test_add_and_get_ordered_histories(app_instance: "Pynenc") -> None:
    """
    Test that adding and getting (ordered by ts) invocation histories works correctly.
    """
    backend = app_instance.state_backend
    invocation_id = "inv-xyz"

    hist1 = InvocationHistory(InvocationStatus.REGISTERED)
    sleep(0.01)
    hist2 = InvocationHistory(InvocationStatus.RUNNING)
    sleep(0.01)
    hist3 = InvocationHistory(InvocationStatus.FAILED)

    backend._add_histories([invocation_id], hist1)
    backend._add_histories([invocation_id], hist3)
    backend._add_histories([invocation_id], hist2)

    histories = backend.get_history(invocation_id)
    if len(histories) != 3:
        sleep(0.1)  # add history run async in a thread
        histories = backend.get_history(invocation_id)
        assert len(histories) == 3
    assert [hist.status for hist in histories] == [
        hist1.status,
        hist2.status,
        hist3.status,
    ]


def test_get_history_retrurns_empty_list_when_no_history(
    app_instance: "Pynenc",
) -> None:
    """
    Ensure get_history returns an empty list for invocation IDs with no history.

    :param "Pynenc" app_instance: Test application instance providing state_backend
    :return: None
    """
    backend = app_instance.state_backend
    invocation_id = "inv-no-history"

    histories = backend.get_history(invocation_id)
    assert histories == []


def test_set_and_get_result(app_instance: "Pynenc") -> None:
    """
    Test that setting and getting invocation result works correctly.
    """
    backend = app_instance.state_backend
    invocation_id = "inv-abc"
    result = "some result"

    backend._set_result(invocation_id, result)
    fetched = backend.get_result(invocation_id)
    assert fetched == result


def test_get_result_raises_when_not_set(app_instance: "Pynenc") -> None:
    """
    Ensure get_result raises KeyError for invocation IDs with no stored result.

    :param "Pynenc" app_instance: Test application instance providing state_backend
    :return: None
    """
    backend = app_instance.state_backend
    invocation_id = "inv-no-result"

    with pytest.raises(KeyError):
        backend.get_result(invocation_id)


def test_set_and_get_exception(app_instance: "Pynenc") -> None:
    """
    Test that setting and getting invocation exception works correctly.
    """
    backend = app_instance.state_backend
    invocation_id = "inv-exc"
    original_exc = KeyError("some key error")

    backend._set_exception(invocation_id, original_exc)
    fetched = backend.get_exception(invocation_id)
    assert isinstance(fetched, type(original_exc))
    assert getattr(fetched, "args", None) == original_exc.args


def test_get_exception_raises_when_not_set(app_instance: "Pynenc") -> None:
    """
    Ensure get_exception returns None for invocation IDs with no stored exception.

    :param "Pynenc" app_instance: Test application instance providing state_backend
    :return: None
    """
    backend = app_instance.state_backend
    invocation_id = "inv-no-exc"

    with pytest.raises(KeyError):
        backend.get_exception(invocation_id)


def test_get_and_set_workflow_data(app_instance: "Pynenc") -> None:
    """
    Test that setting and getting workflow data works correctly.
    """
    backend = app_instance.state_backend
    workflow = WorkflowIdentity("task-1", "inv-1")
    key = "key"
    data = "some data"

    backend.set_workflow_data(workflow, key, data)
    fetched = backend.get_workflow_data(workflow, key, None)
    assert fetched == data

    # for a missing key, the default is returned
    default = "default"
    fetched = backend.get_workflow_data(workflow, "missing-key", default)
    assert fetched == default

    # for a missing workflow, the default is returned
    fetched = backend.get_workflow_data(
        WorkflowIdentity("task-x", "inv-x"), key, default
    )
    assert fetched == default


def test_store_and_get_app_info(app_instance: "Pynenc") -> None:
    """
    Test that storing and retrieving AppInfo works correctly.
    """
    backend = app_instance.state_backend
    app_info = AppInfo.from_app(app_instance)

    backend.store_app_info(app_info)
    fetched = backend.get_app_info()
    assert fetched == app_info


def test_store_and_get_workflow_runs(app_instance: "Pynenc") -> None:
    """
    Test that storing and retrieving workflow run works correctly.
    """
    backend = app_instance.state_backend
    workflow_id = WorkflowIdentity("task-1", "inv-1")

    backend.store_workflow_run(workflow_id)
    backend.store_workflow_run(workflow_id)
    backend.store_workflow_run(workflow_id)

    wf_runs = list(backend.get_workflow_runs(workflow_type=workflow_id.workflow_type))
    assert len(wf_runs) == 1

    assert wf_runs[0] == workflow_id


def test_get_all_workflow_types_and_runs(app_instance: "Pynenc") -> None:
    """
    Test that getting all workflow types and runs works correctly.
    """
    backend = app_instance.state_backend
    wf1 = WorkflowIdentity("task-1", "inv-1")
    wf2 = WorkflowIdentity("task-1", "inv-2")
    wf3 = WorkflowIdentity("task-2", "inv-3")

    backend.store_workflow_run(wf1)
    backend.store_workflow_run(wf2)
    backend.store_workflow_run(wf3)

    wf_types = backend.get_all_workflow_types()
    assert set(wf_types) == {"task-1", "task-2"}

    wf_runs = backend.get_all_workflow_runs()
    assert set(wf_runs) == {wf1, wf2, wf3}

    runs_task_1 = list(backend.get_workflow_runs("task-1"))
    assert set(runs_task_1) == {wf1, wf2}

    runs_task_2 = list(backend.get_workflow_runs("task-2"))
    assert runs_task_2 == [wf3]


def test_store_and_get_workflow_subinvocations(app_instance: "Pynenc") -> None:
    """
    Test that setting and getting workflow sub-invocations works correctly.
    """
    parent_workflow_id = "parent_inv_id"
    sub_invocation1 = "sub_inv_id_1"
    sub_invocation2 = "sub_inv_id_2"
    backend = app_instance.state_backend

    backend.store_workflow_sub_invocation(parent_workflow_id, sub_invocation1)
    backend.store_workflow_sub_invocation(parent_workflow_id, sub_invocation2)
    sub_invocations = backend.get_workflow_sub_invocations(parent_workflow_id)
    assert set(sub_invocations) == {sub_invocation1, sub_invocation2}
