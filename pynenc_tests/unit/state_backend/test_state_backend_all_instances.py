from datetime import datetime, timedelta, UTC
from typing import TYPE_CHECKING

import pytest

from pynenc.app import AppInfo
from pynenc.exceptions import InvocationNotFoundError
from pynenc.identifiers.invocation_id import InvocationId
from pynenc.invocation.status import InvocationStatus
from pynenc.runner import RunnerContext
from pynenc.state_backend.base_state_backend import (
    InvocationHistory,
    InvocationStatusRecord,
)
from pynenc.identifiers.task_id import TaskId
from pynenc.workflow import WorkflowIdentity
from pynenc_tests.conftest import MockPynenc

if TYPE_CHECKING:
    from pynenc import Pynenc
    from pynenc.invocation import DistributedInvocation

mock_app = MockPynenc()


@mock_app.task
def dummy_task() -> None:
    pass


@mock_app.task
def dummy_task_with_args(a: int, b: int) -> int:
    return a + b


@pytest.fixture
def invocation(app_instance: "Pynenc") -> "DistributedInvocation":
    """Helper to create a dummy invocation."""
    dummy_task.app = app_instance
    invocation: DistributedInvocation = dummy_task()  # type: ignore
    return invocation


def test_history_records_are_stored_and_ordered(app_instance: "Pynenc") -> None:
    """
    Test that invocation history entries are stored with timestamp and status
    and are returned in timestamp order.
    """
    backend = app_instance.state_backend
    runner_ctx_id = "test-runner-id"
    inv_1_id = InvocationId("inv-1")

    # Use the actual InvocationHistory class and set timestamps explicitly
    # This keeps the test simple and ensures type compatibility.
    hist1 = InvocationHistory(
        invocation_id="inv-1",
        status_record=InvocationStatusRecord(status=InvocationStatus.REGISTERED),
        runner_context_id=runner_ctx_id,
    )
    hist1._timestamp = datetime.fromtimestamp(1.0)

    hist2 = InvocationHistory(
        invocation_id=inv_1_id,
        status_record=InvocationStatusRecord(status=InvocationStatus.RUNNING),
        runner_context_id=runner_ctx_id,
    )
    hist2._timestamp = datetime.fromtimestamp(2.0)

    hist3 = InvocationHistory(
        invocation_id=inv_1_id,
        status_record=InvocationStatusRecord(status=InvocationStatus.FAILED),
        runner_context_id=runner_ctx_id,
    )
    hist3._timestamp = datetime.fromtimestamp(3.0)

    backend._add_histories([inv_1_id], hist2)
    backend._add_histories([inv_1_id], hist1)
    backend._add_histories([inv_1_id], hist3)

    histories = backend.get_history(inv_1_id)
    assert [hist.status_record.status for hist in histories] == [
        hist1.status_record.status,
        hist2.status_record.status,
        hist3.status_record.status,
    ]


def test_upsert_and_get_invocation(invocation: "DistributedInvocation") -> None:
    """
    Test that upserting an invocation works correctly.
    """
    backend = invocation.app.state_backend

    backend.upsert_invocations([invocation])
    backend.upsert_invocations([invocation])
    backend.upsert_invocations([invocation])
    backend.upsert_invocations([invocation])
    fetched = backend.get_invocation(invocation.invocation_id)
    assert fetched == invocation


def test_upsert_and_get_invocation_with_no_arguments_preserves_empty_serialized_arguments(
    app_instance: "Pynenc",
) -> None:
    """Plugins must not raise KeyError when retrieving an invocation with no arguments.

    Regression guard: a plugin that omits an empty 'arguments' field from the
    stored document and then reads it with ``doc['arguments']`` instead of
    ``doc.get('arguments', {})`` will raise ``KeyError: 'arguments'`` here.
    """
    from pynenc.call import Call
    from pynenc.invocation import DistributedInvocation

    dummy_task.app = app_instance
    inv = DistributedInvocation.isolated(Call(dummy_task))
    app_instance.state_backend.upsert_invocations([inv])

    retrieved = app_instance.state_backend.get_invocation(inv.invocation_id)

    # serialized_arguments must be an empty dict, not raise on access
    assert retrieved.call.serialized_arguments == {}


def test_upsert_and_get_invocation_with_arguments_preserves_serialized_arguments(
    app_instance: "Pynenc",
) -> None:
    """Plugins must persist and restore all named arguments without data loss.

    Paired with the no-arguments regression test above: together they ensure
    the plugin handles both the empty and non-empty argument cases correctly.
    """
    from pynenc.arguments import Arguments
    from pynenc.call import Call
    from pynenc.invocation import DistributedInvocation

    dummy_task_with_args.app = app_instance
    args = Arguments(kwargs={"a": 10, "b": 20})
    inv = DistributedInvocation.isolated(Call(dummy_task_with_args, arguments=args))
    app_instance.state_backend.upsert_invocations([inv])

    retrieved = app_instance.state_backend.get_invocation(inv.invocation_id)

    # Both argument keys must survive the round-trip
    assert set(retrieved.call.serialized_arguments.keys()) == {"a", "b"}


def test_get_invocation_raises_when_not_found(app_instance: "Pynenc") -> None:
    """
    Ensure get_invocation raises KeyError for unknown invocation IDs.

    :param "Pynenc" app_instance: Test application instance providing state_backend
    :return: None
    """
    backend = app_instance.state_backend
    invocation_id = InvocationId("inv-unknown")

    with pytest.raises(InvocationNotFoundError):
        backend.get_invocation(invocation_id)


def test_add_and_get_ordered_histories(app_instance: "Pynenc") -> None:
    """
    Test that adding and getting (ordered by ts) invocation histories works correctly.
    """
    backend = app_instance.state_backend
    invocation_id = InvocationId("inv-xyz")
    runner_context_id = "test-runner-id"

    hist1 = InvocationHistory(
        invocation_id=invocation_id,
        status_record=InvocationStatusRecord(status=InvocationStatus.REGISTERED),
        runner_context_id=runner_context_id,
    )
    hist2 = InvocationHistory(
        invocation_id=invocation_id,
        status_record=InvocationStatusRecord(status=InvocationStatus.RUNNING),
        runner_context_id=runner_context_id,
    )
    hist3 = InvocationHistory(
        invocation_id=invocation_id,
        status_record=InvocationStatusRecord(status=InvocationStatus.FAILED),
        runner_context_id=runner_context_id,
    )

    backend._add_histories([invocation_id], hist1)
    backend._add_histories([invocation_id], hist3)
    backend._add_histories([invocation_id], hist2)

    histories = backend.get_history(invocation_id)
    if len(histories) != 3:
        histories = backend.get_history(invocation_id)
        assert len(histories) == 3
    assert [hist.status_record for hist in histories] == [
        hist1.status_record,
        hist2.status_record,
        hist3.status_record,
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
    invocation_id = InvocationId("inv-no-history")

    histories = backend.get_history(invocation_id)
    assert histories == []


def test_set_and_get_result(app_instance: "Pynenc") -> None:
    """
    Test that setting and getting invocation result works correctly.
    """
    backend = app_instance.state_backend
    invocation_id = InvocationId("inv-abc")
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
    invocation_id = InvocationId("inv-no-result")

    with pytest.raises(KeyError):
        backend.get_result(invocation_id)


def test_set_and_get_exception(app_instance: "Pynenc") -> None:
    """
    Test that setting and getting invocation exception works correctly.
    """
    backend = app_instance.state_backend
    invocation_id = InvocationId("inv-exc")
    original_exc = KeyError("some key error")

    backend.set_exception(invocation_id, original_exc)
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
    invocation_id = InvocationId("inv-no-exc")

    with pytest.raises(KeyError):
        backend.get_exception(invocation_id)


def get_workflow_identity(
    inv_suffix: str, task_suffix: str | None = None
) -> WorkflowIdentity:
    return WorkflowIdentity(
        InvocationId(f"inv-{inv_suffix}"),
        TaskId("module", f"func_{task_suffix or inv_suffix}"),
    )


def test_get_and_set_workflow_data(app_instance: "Pynenc") -> None:
    """
    Test that setting and getting workflow data works correctly.
    """
    backend = app_instance.state_backend
    workflow = get_workflow_identity("1")
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
    fetched = backend.get_workflow_data(get_workflow_identity("x"), key, default)
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
    workflow_id = get_workflow_identity("1")

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
    wf1 = get_workflow_identity(task_suffix="1", inv_suffix="1")
    wf2 = get_workflow_identity(task_suffix="1", inv_suffix="2")
    wf3 = get_workflow_identity(task_suffix="2", inv_suffix="3")

    backend.store_workflow_run(wf1)
    backend.store_workflow_run(wf2)
    backend.store_workflow_run(wf3)

    wf_types = set(backend.get_all_workflow_types())
    assert len(wf_types) == 2
    assert set(wf_types) == {wf1.workflow_type, wf3.workflow_type}

    wf_runs = backend.get_all_workflow_runs()
    assert set(wf_runs) == {wf1, wf2, wf3}

    runs_task_1 = list(backend.get_workflow_runs(wf1.workflow_type))
    assert set(runs_task_1) == {wf1, wf2}

    runs_task_2 = list(backend.get_workflow_runs(wf3.workflow_type))
    assert runs_task_2 == [wf3]


def test_store_and_get_workflow_subinvocations(app_instance: "Pynenc") -> None:
    """
    Test that setting and getting workflow sub-invocations works correctly.
    """
    parent_workflow_id = InvocationId("parent_inv_id")
    sub_invocation1 = InvocationId("sub_inv_id_1")
    sub_invocation2 = InvocationId("sub_inv_id_2")
    backend = app_instance.state_backend

    backend.store_workflow_sub_invocation(parent_workflow_id, sub_invocation1)
    backend.store_workflow_sub_invocation(parent_workflow_id, sub_invocation2)
    sub_invocations = backend.get_workflow_sub_invocations(parent_workflow_id)
    assert set(sub_invocations) == {sub_invocation1, sub_invocation2}


def test_iter_history_in_timerange_returns_entries_within_range(
    app_instance: "Pynenc",
) -> None:
    """
    Test that iter_history_in_timerange returns only history entries
    whose timestamps fall within the specified time range.
    """
    backend = app_instance.state_backend
    runner_context_id = "test-runner-id"

    # Create a base time and define the query range
    base_time = datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC)
    query_start = base_time
    query_end = base_time + timedelta(hours=1)

    # Create history entries: before range, within range, after range
    hist_before = InvocationHistory(
        invocation_id="inv-before",
        status_record=InvocationStatusRecord(status=InvocationStatus.RUNNING),
        runner_context_id=runner_context_id,
    )
    hist_before._timestamp = base_time - timedelta(minutes=30)

    hist_within_1 = InvocationHistory(
        invocation_id="inv-within-1",
        status_record=InvocationStatusRecord(status=InvocationStatus.RUNNING),
        runner_context_id=runner_context_id,
    )
    hist_within_1._timestamp = base_time + timedelta(minutes=10)

    hist_within_2 = InvocationHistory(
        invocation_id="inv-within-2",
        status_record=InvocationStatusRecord(status=InvocationStatus.SUCCESS),
        runner_context_id=runner_context_id,
    )
    hist_within_2._timestamp = base_time + timedelta(minutes=30)

    hist_after = InvocationHistory(
        invocation_id="inv-after",
        status_record=InvocationStatusRecord(status=InvocationStatus.SUCCESS),
        runner_context_id=runner_context_id,
    )
    hist_after._timestamp = base_time + timedelta(hours=2)

    # Add all histories
    backend._add_histories([InvocationId("inv-before")], hist_before)
    backend._add_histories([InvocationId("inv-within-1")], hist_within_1)
    backend._add_histories([InvocationId("inv-within-2")], hist_within_2)
    backend._add_histories([InvocationId("inv-after")], hist_after)

    # Query the time range
    all_entries: list[InvocationHistory] = []
    for batch in backend.iter_history_in_timerange(query_start, query_end):
        all_entries.extend(batch)

    # Should only return entries within the range
    inv_ids = {entry.invocation_id for entry in all_entries}
    assert inv_ids == {"inv-within-1", "inv-within-2"}
    assert "inv-before" not in inv_ids
    assert "inv-after" not in inv_ids


def test_iter_history_in_timerange_returns_all_statuses_for_invocation(
    app_instance: "Pynenc",
) -> None:
    """
    Test that iter_history_in_timerange returns all status transitions
    for an invocation when they fall within the time range.

    This tests the scenario where an invocation has multiple status changes
    (e.g., RUNNING -> SUCCESS) and both should be returned.
    """
    backend = app_instance.state_backend
    runner_context_id = "test-runner-id"

    base_time = datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC)
    query_start = base_time
    query_end = base_time + timedelta(hours=1)

    # Create an invocation with RUNNING and SUCCESS within the range
    hist_running = InvocationHistory(
        invocation_id="inv-complete",
        status_record=InvocationStatusRecord(status=InvocationStatus.RUNNING),
        runner_context_id=runner_context_id,
    )
    hist_running._timestamp = base_time + timedelta(minutes=10)

    hist_success = InvocationHistory(
        invocation_id="inv-complete",
        status_record=InvocationStatusRecord(status=InvocationStatus.SUCCESS),
        runner_context_id=runner_context_id,
    )
    hist_success._timestamp = base_time + timedelta(minutes=20)

    backend._add_histories([InvocationId("inv-complete")], hist_running)
    backend._add_histories([InvocationId("inv-complete")], hist_success)

    # Query the time range
    all_entries: list[InvocationHistory] = []
    for batch in backend.iter_history_in_timerange(query_start, query_end):
        all_entries.extend(batch)

    # Both entries should be returned
    assert len(all_entries) == 2
    statuses = {entry.status_record.status for entry in all_entries}
    assert statuses == {InvocationStatus.RUNNING, InvocationStatus.SUCCESS}


def test_iter_history_in_timerange_ongoing_invocation_within_range(
    app_instance: "Pynenc",
) -> None:
    """
    Test that an invocation that started RUNNING within the range but has
    no subsequent status (still ongoing) is included in the results.

    This is the key scenario for the SVG timeline - ongoing invocations
    should be visible if they started within the queried range.
    """
    backend = app_instance.state_backend
    runner_context_id = "test-runner-id"
    inv_ongoing_id = InvocationId("inv-ongoing")

    base_time = datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC)
    query_start = base_time
    query_end = base_time + timedelta(hours=1)

    # Create an invocation that started RUNNING within the range but never completed
    hist_running = InvocationHistory(
        invocation_id=inv_ongoing_id,
        status_record=InvocationStatusRecord(status=InvocationStatus.RUNNING),
        runner_context_id=runner_context_id,
    )
    hist_running._timestamp = base_time + timedelta(minutes=15)

    backend._add_histories([inv_ongoing_id], hist_running)

    # Query the time range
    all_entries: list[InvocationHistory] = []
    for batch in backend.iter_history_in_timerange(query_start, query_end):
        all_entries.extend(batch)

    # The ongoing invocation should be included since its RUNNING started within range
    assert len(all_entries) == 1
    assert all_entries[0].invocation_id == inv_ongoing_id
    assert all_entries[0].status_record.status == InvocationStatus.RUNNING


def test_iter_history_in_timerange_entries_ordered_by_timestamp(
    app_instance: "Pynenc",
) -> None:
    """
    Test that iter_history_in_timerange returns entries ordered by timestamp.
    """
    backend = app_instance.state_backend
    runner_context_id = "test-runner-id"

    base_time = datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC)
    query_start = base_time
    query_end = base_time + timedelta(hours=1)

    # Create entries at different times, add them out of order
    times = [
        base_time + timedelta(minutes=30),
        base_time + timedelta(minutes=10),
        base_time + timedelta(minutes=50),
        base_time + timedelta(minutes=20),
    ]

    for i, ts in enumerate(times):
        inv_id = InvocationId(f"inv-{i}")
        hist = InvocationHistory(
            invocation_id=inv_id,
            status_record=InvocationStatusRecord(status=InvocationStatus.SUCCESS),
            runner_context_id=runner_context_id,
        )
        hist._timestamp = ts
        backend._add_histories([inv_id], hist)

    # Query the time range
    all_entries: list[InvocationHistory] = []
    for batch in backend.iter_history_in_timerange(query_start, query_end):
        all_entries.extend(batch)

    # Entries should be ordered by timestamp
    timestamps = [entry.timestamp for entry in all_entries]
    assert timestamps == sorted(timestamps)


def test_iter_invocations_in_timerange(app_instance: "Pynenc") -> None:
    """
    Test that iter_invocations_in_timerange returns distinct invocation IDs
    for invocations that have history entries within the time range.
    """
    backend = app_instance.state_backend
    runner_context_id = "test-runner-id"
    base_time = datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC)
    query_start = base_time
    query_end = base_time + timedelta(hours=1)

    # Create an invocation with multiple entries within range (should appear once)
    hist1 = InvocationHistory(
        invocation_id="inv-multi",
        status_record=InvocationStatusRecord(status=InvocationStatus.RUNNING),
        runner_context_id=runner_context_id,
    )
    hist1._timestamp = base_time + timedelta(minutes=10)

    hist2 = InvocationHistory(
        invocation_id="inv-multi",
        status_record=InvocationStatusRecord(status=InvocationStatus.SUCCESS),
        runner_context_id=runner_context_id,
    )
    hist2._timestamp = base_time + timedelta(minutes=20)

    # Create another invocation within range
    hist3 = InvocationHistory(
        invocation_id="inv-single",
        status_record=InvocationStatusRecord(status=InvocationStatus.RUNNING),
        runner_context_id=runner_context_id,
    )
    hist3._timestamp = base_time + timedelta(minutes=30)

    # Create an invocation outside range
    hist4 = InvocationHistory(
        invocation_id="inv-outside",
        status_record=InvocationStatusRecord(status=InvocationStatus.SUCCESS),
        runner_context_id=runner_context_id,
    )
    hist4._timestamp = base_time + timedelta(hours=2)

    backend._add_histories([InvocationId("inv-multi")], hist1)
    backend._add_histories([InvocationId("inv-multi")], hist2)
    backend._add_histories([InvocationId("inv-single")], hist3)
    backend._add_histories([InvocationId("inv-outside")], hist4)

    # Query the time range
    all_ids: list[str] = []
    for batch in backend.iter_invocations_in_timerange(query_start, query_end):
        all_ids.extend(batch)

    # Should return distinct IDs for invocations within range
    assert set(all_ids) == {"inv-multi", "inv-single"}
    assert "inv-outside" not in all_ids


def test_store_and_get_runner_context(app_instance: "Pynenc") -> None:
    """
    Test storing and retrieving a runner context.
    """
    backend = app_instance.state_backend
    runner_id = "test-runner-ctx"
    ctx = RunnerContext(runner_cls="TestRunner", runner_id=runner_id)
    # We get None if we did not save it
    assert backend.get_runner_context(runner_id) is None
    backend.store_runner_context(ctx)
    # We get back the same context we stored
    assert ctx == backend.get_runner_context(runner_id)


def test_get_matching_runner_contexts_should_return_matches(
    app_instance: "Pynenc",
) -> None:
    """Test that get_matching_runner_contexts returns contexts matching a partial ID."""
    backend = app_instance.state_backend
    ctx_a = RunnerContext(runner_cls="RunnerA", runner_id="runner-alpha-001")
    ctx_b = RunnerContext(runner_cls="RunnerB", runner_id="runner-beta-002")
    ctx_c = RunnerContext(runner_cls="RunnerA", runner_id="runner-alpha-003")

    backend.store_runner_context(ctx_a)
    backend.store_runner_context(ctx_b)
    backend.store_runner_context(ctx_c)

    # Match partial substring "alpha"
    matches = list(backend.get_matching_runner_contexts("alpha"))
    matched_ids = {m.runner_id for m in matches}
    assert matched_ids == {"runner-alpha-001", "runner-alpha-003"}

    # Match partial substring "beta"
    matches = list(backend.get_matching_runner_contexts("beta"))
    assert len(matches) == 1
    assert matches[0].runner_id == "runner-beta-002"

    # No match
    matches = list(backend.get_matching_runner_contexts("gamma"))
    assert matches == []


def test_get_invocation_ids_by_workflow_should_filter_by_workflow_id(
    app_instance: "Pynenc",
) -> None:
    """Test filtering invocations by workflow_id."""
    backend = app_instance.state_backend
    dummy_task.app = app_instance

    inv1: DistributedInvocation = dummy_task()  # type: ignore
    inv2: DistributedInvocation = dummy_task()  # type: ignore

    # Query by the first invocation's workflow_id
    wf_id_str = str(inv1.workflow.workflow_id)
    result = list(backend.get_invocation_ids_by_workflow(workflow_id=wf_id_str))
    result_strs = {str(r) for r in result}
    assert str(inv1.invocation_id) in result_strs
    # Second invocation has a different workflow, should not match
    assert str(inv2.invocation_id) not in result_strs


def test_get_invocation_ids_by_workflow_should_filter_by_type(
    app_instance: "Pynenc",
) -> None:
    """Test filtering invocations by workflow_type_key."""
    backend = app_instance.state_backend
    dummy_task.app = app_instance
    dummy_task_with_args.app = app_instance

    inv1: DistributedInvocation = dummy_task()  # type: ignore

    # Filter by dummy_task's task_id key as workflow_type
    wt_key = str(inv1.workflow.workflow_type)
    result = list(backend.get_invocation_ids_by_workflow(workflow_type_key=wt_key))
    result_strs = {str(r) for r in result}
    assert str(inv1.invocation_id) in result_strs


def test_get_invocation_ids_by_workflow_should_return_empty_when_no_match(
    app_instance: "Pynenc",
) -> None:
    """Test that non-matching filters return empty."""
    backend = app_instance.state_backend
    result = list(backend.get_invocation_ids_by_workflow(workflow_id="nonexistent-id"))
    assert result == []
