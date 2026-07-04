"""
Minimal tests for pynmon invocation view.

Tests basic functionality using real in-memory Pynenc app with actual tasks and invocations.
"""

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from pynenc.arguments import Arguments
from pynenc.call import Call
from pynenc.invocation import DistributedInvocation
from pynenc.invocation.status import InvocationStatus
from pynenc.orchestrator.atomic_service import AtomicServiceRun
from pynenc.runner.runner_context import RunnerContext
from pynenc.trigger.monitoring import TriggerRunParticipant, TriggerRunRecord
from pynenc_tests.conftest import MockPynenc
from pynmon.app import app as pynmon_app
from pynmon.app import setup_routes

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest

    from pynenc import Pynenc

# Module level app and task setup
mock_app = MockPynenc()


@mock_app.task
def add_task(x: int, y: int) -> int:
    """Simple addition task."""
    return x + y


@mock_app.task
def multiply_task(a: int, b: int) -> int:
    """Simple multiplication task."""
    return a * b


@mock_app.workflow
def shipping_workflow(order_id: str) -> str:
    """Workflow-root task for workflow visibility tests."""
    return order_id


@pytest.fixture
def app(request: "FixtureRequest", app_instance: "Pynenc") -> "Pynenc":
    app = app_instance
    app._tasks = mock_app._tasks
    add_task.app = app
    multiply_task.app = app
    shipping_workflow.app = app
    app.purge()
    request.addfinalizer(app.purge)
    return app


def test_invocations_list_shows_invocations(app: "Pynenc") -> None:
    """Test that invocations list displays invocations."""
    # Clear any existing invocations
    app.purge()

    # Create some invocations for testing
    call1: Call = Call(add_task, Arguments({"x": 5, "y": 3}))
    call2: Call = Call(multiply_task, Arguments({"a": 4, "b": 7}))

    invocation1: DistributedInvocation = DistributedInvocation.isolated(call1)
    invocation2: DistributedInvocation = DistributedInvocation.isolated(call2)

    # Store invocations in the orchestrator so they can be retrieved
    app.orchestrator.register_new_invocations([invocation1, invocation2])

    # Set their status to REGISTERED
    runner_ctx = RunnerContext.from_runner(app.runner)
    app.orchestrator.set_invocation_status(
        invocation2.invocation_id, InvocationStatus.PENDING, runner_ctx
    )
    app.orchestrator.set_invocation_status(
        invocation2.invocation_id, InvocationStatus.RUNNING, runner_ctx
    )
    app.orchestrator.set_invocation_status(
        invocation2.invocation_id, InvocationStatus.SUCCESS, runner_ctx
    )

    # Setup routes before creating test client
    setup_routes()

    # Patch pynmon to use our test app
    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        response = client.get("/invocations/")

        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

        content = response.text
        # Should show our invocations
        assert invocation1.invocation_id[:8] in content
        assert invocation2.invocation_id[:8] in content
        assert "add_task" in content
        assert "multiply_task" in content
        assert f"/invocations/timeline?selected={invocation1.invocation_id}" in content
        assert f"/invocations/timeline?selected={invocation2.invocation_id}" in content
        assert "window.openInvocationInTimeline = openInvocationInTimeline" in content
        assert 'params.set("selected", invocationId)' in content
        assert 'params.set("resolution", "100ms")' in content
        assert "Math.max(durationMs * 0.1, 1000)" not in content


def test_invocations_timeline_shows_reference_only_atomic_service_window(
    app: "Pynenc",
) -> None:
    """Trigger-run AS refs without execution history no longer produce a
    fallback window. The orchestrator's purge now protects referenced
    executions, so a trigger run whose execution has been dropped is
    treated as data loss rather than rendered with synthetic timestamps.
    """
    app.purge()
    setup_routes()
    now = datetime.now(UTC).replace(microsecond=123000)
    atomic_service_run_id = "as-run-from-trigger-ref"
    runner_id = "test-trigger-runner"
    app.trigger.store_trigger_run(
        TriggerRunRecord(
            trigger_run_id="trigger-run-as-ref",
            trigger_id="trigger-as-ref",
            task_id_key="tests.add_task",
            logic_value="AND",
            claimed_at=now,
            executed_at=now + timedelta(milliseconds=3),
            atomic_service_run_id=atomic_service_run_id,
            atomic_service_runner_id=runner_id,
        )
    )
    start_date = (now - timedelta(seconds=1)).replace(tzinfo=None).isoformat()
    end_date = (now + timedelta(seconds=1)).replace(tzinfo=None).isoformat()

    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        response = client.get(
            "/invocations/timeline",
            params={
                "time_range": "custom",
                "start_date": start_date,
                "end_date": end_date,
                "show_atomic_service": "1",
            },
        )

    assert response.status_code == 200
    assert f'data-atomic-service-run-id="{atomic_service_run_id}"' not in response.text


def _create_invocations_with_statuses(
    app: "Pynenc",
) -> tuple["DistributedInvocation", "DistributedInvocation", "DistributedInvocation"]:
    """Create three invocations with REGISTERED, SUCCESS, and FAILED statuses."""
    call1: Call = Call(add_task, Arguments({"x": 1, "y": 1}))
    call2: Call = Call(add_task, Arguments({"x": 2, "y": 2}))
    call3: Call = Call(add_task, Arguments({"x": 3, "y": 3}))

    inv1: DistributedInvocation = DistributedInvocation.isolated(call1)
    inv2: DistributedInvocation = DistributedInvocation.isolated(call2)
    inv3: DistributedInvocation = DistributedInvocation.isolated(call3)

    app.orchestrator.register_new_invocations([inv1, inv2, inv3])
    runner_ctx = RunnerContext.from_runner(app.runner)
    for status in (
        InvocationStatus.PENDING,
        InvocationStatus.RUNNING,
        InvocationStatus.SUCCESS,
    ):
        app.orchestrator.set_invocation_status(inv2.invocation_id, status, runner_ctx)
    for status in (
        InvocationStatus.PENDING,
        InvocationStatus.RUNNING,
        InvocationStatus.FAILED,
    ):
        app.orchestrator.set_invocation_status(inv3.invocation_id, status, runner_ctx)
    return inv1, inv2, inv3


def test_invocations_list_with_status_filter(app: "Pynenc") -> None:
    """Test that invocations list actually filters by status."""
    import re

    app.purge()
    _, success_invocation, failed_invocation = _create_invocations_with_statuses(app)

    setup_routes()

    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)

        response = client.get("/invocations/?status=success")
        assert response.status_code == 200
        content = response.text

        assert str(success_invocation.invocation_id) in content
        assert str(failed_invocation.invocation_id) not in content

        # Exactly one invocation (2 links: ID column + Details button)
        detail_links = re.findall(r"/invocations/[a-f0-9-]+", content)
        assert len(detail_links) == 2, (
            f"Expected 2 links (ID + Details) for 1 invocation, found {len(detail_links)}"
        )
        unique_invocation_ids = {link.split("/")[-1] for link in detail_links}
        assert len(unique_invocation_ids) == 1, (
            f"Expected links for 1 unique invocation, found {len(unique_invocation_ids)}"
        )


def test_invocations_list_with_task_filter(app: "Pynenc") -> None:
    """Test that invocations list actually filters by task."""
    # Clear any existing invocations
    app.purge()

    # Create invocations for different tasks
    call1: Call = Call(add_task, Arguments({"x": 1, "y": 1}))
    call2: Call = Call(multiply_task, Arguments({"a": 2, "b": 2}))

    invocation1: DistributedInvocation = DistributedInvocation.isolated(call1)
    invocation2: DistributedInvocation = DistributedInvocation.isolated(call2)

    # register invocations in the orchestrator
    app.orchestrator.register_new_invocations([invocation1, invocation2])

    # Setup routes before creating test client
    setup_routes()

    # Patch pynmon to use our test app
    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)

        # Filter by add_task - should only show invocation1
        task_id = add_task.task_id
        response = client.get(f"/invocations/?task_id={task_id.key}")

        assert response.status_code == 200
        content = response.text

        # Count the number of invocation detail links to verify filtering
        import re

        detail_links = re.findall(r"/invocations/[a-f0-9-]+", content)
        # Each invocation has 2 links: one in ID column, one in Details button
        assert len(detail_links) == 2, (
            f"Expected 2 links (ID + Details) for 1 invocation for {task_id}, found {len(detail_links)}"
        )

        # Verify both links point to the same invocation (invocation1)
        unique_invocation_ids = {link.split("/")[-1] for link in detail_links}
        assert len(unique_invocation_ids) == 1, (
            f"Expected links for 1 unique invocation, found {len(unique_invocation_ids)}"
        )

        # Verify the task ID key appears in the content (as a link to the task)
        assert task_id.key in content, (
            f"Task ID key {task_id.key} should appear in the filtered results"
        )

        # Verify the task module and function name appears in the content
        assert task_id.module in content
        assert task_id.func_name in content


def test_invocation_detail_shows_invocation_info(app: "Pynenc") -> None:
    """Test that invocation detail displays complete invocation information."""
    # Create an invocation for testing
    call: Call = Call(add_task, Arguments({"x": 10, "y": 20}))
    invocation: DistributedInvocation = DistributedInvocation.isolated(call)

    # register invocations in the orchestrator
    app.orchestrator.register_new_invocations([invocation])

    # Setup routes before creating test client
    setup_routes()

    # Patch pynmon to use our test app
    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        response = client.get(f"/invocations/{invocation.invocation_id}")

        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]
        content = response.text
        # Should show invocation details
        assert invocation.invocation_id in content
        assert "add_task" in content
        assert str(invocation.status.name) in content

        # Should show call information
        assert call.call_id.task_id.module in content
        assert call.call_id.task_id.func_name in content
        assert call.call_id.args_id in content

        # Should show arguments
        assert "10" in content  # x argument
        assert "20" in content  # y argument
        assert "window.openInvocationInTimeline = openInvocationInTimeline" in content
        assert f'data-invocation-id="{invocation.invocation_id}"' in content


def test_invocation_detail_shows_workflow_role_states(app: "Pynenc") -> None:
    """Invocation detail should distinguish root, member, and standalone invocations."""
    app.purge()

    root_invocation = DistributedInvocation.isolated(
        Call(shipping_workflow, Arguments({"order_id": "ORD-1"}))
    )
    workflow_member = DistributedInvocation.from_parent(
        Call(add_task, Arguments({"x": 2, "y": 3})),
        parent_invocation=root_invocation,
    )
    standalone = DistributedInvocation.isolated(
        Call(multiply_task, Arguments({"a": 4, "b": 5}))
    )
    app.orchestrator.register_new_invocations(
        [root_invocation, workflow_member, standalone]
    )

    setup_routes()

    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        root_response = client.get(f"/invocations/{root_invocation.invocation_id}")
        member_response = client.get(f"/invocations/{workflow_member.invocation_id}")
        standalone_response = client.get(f"/invocations/{standalone.invocation_id}")

    assert root_response.status_code == 200
    assert "Workflow root" in root_response.text
    assert "Defines the root workflow run." in root_response.text

    assert member_response.status_code == 200
    assert "Workflow member" in member_response.text
    assert "Runs inside an existing workflow." in member_response.text

    assert standalone_response.status_code == 200
    assert "No workflow" in standalone_response.text
    assert (
        "Standalone invocation with no workflow membership." in standalone_response.text
    )


def test_invocation_detail_nonexistent_invocation(app: "Pynenc") -> None:
    """Test that invocation detail handles nonexistent invocations."""
    # Setup routes before creating test client
    setup_routes()

    # Patch pynmon to use our test app
    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        response = client.get("/invocations/nonexistent-id")

        assert response.status_code == 404
        assert "text/html" in response.headers["content-type"]

        content = response.text
        assert "not found" in content.lower() or "error" in content.lower()


def test_invocations_timeline_basic(app: "Pynenc") -> None:
    """Test that invocations timeline loads without errors."""
    # Clear any existing invocations
    app.purge()

    # Setup routes before creating test client
    setup_routes()

    # Patch pynmon to use our test app
    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        response = client.get("/invocations/timeline")

        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]
        assert 'id="timeline-range-zoom"' in response.text
        assert 'aria-label="Select timeline range to zoom"' in response.text
        assert "zoom_in" in response.text


def test_invocations_timeline_scoped_workflow_list_renders_histories(
    app: "Pynenc",
) -> None:
    """Workflow-scoped inv_ids timeline should not filter out all history."""
    app.purge()
    root_invocation = DistributedInvocation.isolated(
        Call(shipping_workflow, Arguments({"order_id": "ORD-3"}))
    )
    workflow_member = DistributedInvocation.from_parent(
        Call(add_task, Arguments({"x": 11, "y": 12})),
        parent_invocation=root_invocation,
    )
    app.orchestrator.register_new_invocations([root_invocation, workflow_member])
    runner_ctx = RunnerContext.from_runner(app.runner)
    for invocation in (root_invocation, workflow_member):
        app.orchestrator.set_invocation_status(
            invocation.invocation_id, InvocationStatus.PENDING, runner_ctx
        )
        app.orchestrator.set_invocation_status(
            invocation.invocation_id, InvocationStatus.RUNNING, runner_ctx
        )
        app.orchestrator.set_invocation_status(
            invocation.invocation_id, InvocationStatus.SUCCESS, runner_ctx
        )

    setup_routes()

    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        response = client.get(
            "/invocations/timeline",
            params={
                "time_range": "1h",
                "workflow_id": str(root_invocation.invocation_id),
                "inv_ids": ",".join(
                    [
                        str(root_invocation.invocation_id),
                        str(workflow_member.invocation_id),
                    ]
                ),
                "resolution": "100ms",
            },
        )

    assert response.status_code == 200
    assert "No invocations found for the selected time range." not in response.text
    assert str(root_invocation.invocation_id) in response.text
    assert str(workflow_member.invocation_id) in response.text


def test_invocations_timeline_explicit_scope_backfills_segment_without_points(
    app: "Pynenc",
) -> None:
    """Explicit inv_ids should render an invocation active inside the window."""
    from pynenc.invocation.status import InvocationStatusRecord
    from pynenc.state_backend.base_state_backend import InvocationHistory
    from pynenc.trigger.monitoring import EventMarkerPage
    from pynmon.util.svg.models import TimelineConfig
    from pynmon.views.invocations import TimelineRequest, _build_svg_timeline

    app.purge()
    invocation = DistributedInvocation.isolated(
        Call(add_task, Arguments({"x": 5, "y": 6}))
    )
    app.orchestrator.register_new_invocations([invocation])
    runner_ctx = RunnerContext.from_runner(app.runner)
    app.state_backend.store_runner_context(runner_ctx)

    window_start = datetime(2026, 6, 29, 17, 58, 35, tzinfo=UTC)
    window_end = window_start + timedelta(seconds=1)
    running = InvocationHistory(
        invocation_id=str(invocation.invocation_id),
        status_record=InvocationStatusRecord(status=InvocationStatus.RUNNING),
        runner_context_id=runner_ctx.runner_id,
    )
    running._timestamp = window_start - timedelta(seconds=2)
    success = InvocationHistory(
        invocation_id=str(invocation.invocation_id),
        status_record=InvocationStatusRecord(status=InvocationStatus.SUCCESS),
        runner_context_id=runner_ctx.runner_id,
    )
    success._timestamp = window_end + timedelta(seconds=2)

    with (
        patch.object(
            app.state_backend,
            "iter_history_in_timerange",
            return_value=iter([]),
        ),
        patch.object(app.state_backend, "get_history", return_value=[running, success]),
        patch.object(
            app.trigger,
            "get_event_markers_in_timerange",
            return_value=EventMarkerPage(markers=[], total=0, truncated=False),
        ),
        patch.object(app.trigger, "get_trigger_runs_in_timerange", return_value=[]),
    ):
        svg = _build_svg_timeline(
            TimelineRequest(
                app=app,
                start_time=window_start,
                end_time=window_end,
                config=TimelineConfig(resolution_seconds=0.1),
                limit=500,
                inv_ids_filter={str(invocation.invocation_id)},
                show_atomic_service=False,
            )
        )

    assert 'class="status-segment"><rect' in svg
    assert (
        f'data-invocation-id="{invocation.invocation_id}" data-status="RUNNING"' in svg
    )


def test_invocations_list_exposes_list_timeline_action(app: "Pynenc") -> None:
    """The invocations list provides a timeline action for the whole visible list."""
    app.purge()
    invocation1 = DistributedInvocation.isolated(
        Call(add_task, Arguments({"x": 1, "y": 2}))
    )
    invocation2 = DistributedInvocation.isolated(
        Call(multiply_task, Arguments({"a": 3, "b": 4}))
    )
    app.orchestrator.register_new_invocations([invocation1, invocation2])

    setup_routes()

    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        response = client.get("/invocations/")

    assert response.status_code == 200
    assert 'id="view-list-in-timeline-btn"' in response.text
    assert "Timeline for List" in response.text
    assert (
        "window.openInvocationListInTimeline = openInvocationListInTimeline"
        in response.text
    )
    assert 'class="btn btn-sm btn-outline-secondary ms-1"' not in response.text


def test_invocations_timeline_exposes_scope_filter_and_date_examples(
    app: "Pynenc",
) -> None:
    """Hidden invocation scoping must be visible and editable in filters."""
    app.purge()
    setup_routes()

    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        response = client.get(
            "/invocations/timeline"
            "?time_range=custom"
            "&start_date=2026-05-18T17:50:49"
            "&end_date=2026-05-18T17:50:49.600"
            "&task_id=tasks.workflow_root"
            "&workflow_type=tasks.order_workflow"
            "&workflow_id=workflow-123"
            "&inv_ids=inv-a,inv-b"
        )

    assert response.status_code == 200
    content = response.text
    assert 'id="inv_ids" name="inv_ids"' in content
    assert 'value="inv-a,inv-b"' in content
    assert 'type="hidden" name="inv_ids"' not in content
    assert "Invocation Scope" in content
    assert "scope: 2" in content
    assert "timeline-filter-scope-clear" in content
    assert "Clear invocation scope" in content
    assert "Clear task filter" in content
    assert "Clear workflow type filter" in content
    assert "Clear workflow id filter" in content
    import re

    clear_link = re.search(
        r'href="([^"]+)" class="timeline-filter-scope-clear"\s+title="Clear invocation scope"',
        content,
    )
    assert clear_link is not None
    assert "inv_ids" not in clear_link.group(1)
    assert "time_range=custom" in clear_link.group(1)
    workflow_clear_link = re.search(
        r'href="([^"]+)" class="timeline-filter-scope-clear"\s+title="Clear workflow id filter"',
        content,
    )
    assert workflow_clear_link is not None
    assert "workflow_id" not in workflow_clear_link.group(1)
    assert 'placeholder="2026-05-16T19:09:12.348"' in content
    assert "ISO 8601 with ms/us" not in content


def test_invocations_timeline_renders_event_markers(app: "Pynenc") -> None:
    """Timeline SVG includes a clickable marker for each event in the window."""
    from datetime import UTC, datetime

    from pynenc.trigger.monitoring import EventRecord

    app.purge()
    # Create a real invocation with a RUNNING segment so the marker has
    # a visible emitter bar to anchor on (strict anchor regime skips
    # orphan markers that have no segment in the timeline).
    call: Call = Call(add_task, Arguments({"x": 7, "y": 8}))
    emitter: DistributedInvocation = DistributedInvocation.isolated(call)
    app.orchestrator.register_new_invocations([emitter])
    runner_ctx = RunnerContext.from_runner(app.runner)
    app.orchestrator.set_invocation_status(
        emitter.invocation_id, InvocationStatus.PENDING, runner_ctx
    )
    app.orchestrator.set_invocation_status(
        emitter.invocation_id, InvocationStatus.RUNNING, runner_ctx
    )

    record = EventRecord(
        event_id="evt-marker",
        event_code="alpha",
        timestamp=datetime.now(UTC),
        emitted_by_invocation_id=emitter.invocation_id,
        triggered_invocation_ids=[emitter.invocation_id],
    )
    app.trigger.store_event(record)

    setup_routes()
    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        response = client.get("/invocations/timeline?time_range=1h")

    assert response.status_code == 200
    assert "/events/evt-marker" in response.text
    assert "event-markers" in response.text


def test_invocations_timeline_places_external_event_on_external_runner(
    app: "Pynenc",
) -> None:
    """Events emitted by clients render on an ExternalRunner lane."""
    from datetime import UTC, datetime

    from pynenc.trigger.monitoring import EventRecord

    app.purge()
    external_context = RunnerContext(
        runner_cls="ExternalRunner",
        runner_id="ExternalRunner@host-123",
        hostname="host",
        pid=123,
    )
    app.state_backend.store_runner_context(external_context)
    app.trigger.store_event(
        EventRecord(
            event_id="evt-external",
            event_code="feed_updated",
            timestamp=datetime.now(UTC),
            matched_condition_ids=["event:feed_updated"],
            triggered_invocation_ids=["inv-triggered"],
            emitted_by_runner_context_id=external_context.runner_id,
        )
    )

    setup_routes()
    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        response = client.get("/invocations/timeline?time_range=1h")

    assert response.status_code == 200
    assert 'data-event-id="evt-external"' in response.text
    assert 'data-runner-id="__collapsed_external_runners__"' in response.text
    assert "ExternalRunner" in response.text


def test_invocations_timeline_renders_ghost_bar_for_offscreen_emitter(
    app: "Pynenc",
) -> None:
    """Off-window emitter referenced by a visible event gets a partial bar.

    When the time window contains only the event marker but the emitter
    invocation's status history lives entirely before window.start, the
    timeline must backfill a clipped "ghost" segment so the marker has an
    anchor (no floating dot) and the rect remains clickable via the
    existing ``rect[data-invocation-id]`` JS delegation.
    """
    from datetime import UTC, datetime, timedelta

    from pynenc.invocation.status import InvocationStatusRecord
    from pynenc.state_backend.base_state_backend import InvocationHistory
    from pynenc.trigger.monitoring import EventRecord

    app.purge()
    call: Call = Call(add_task, Arguments({"x": 1, "y": 2}))
    emitter: DistributedInvocation = DistributedInvocation.isolated(call)
    app.orchestrator.register_new_invocations([emitter])

    event_time = datetime.now(UTC)
    window_start = event_time - timedelta(milliseconds=50)
    window_end = event_time + timedelta(milliseconds=50)
    backdate = event_time - timedelta(seconds=10)

    runner_ctx = RunnerContext.from_runner(app.runner)
    app.state_backend.store_runner_context(runner_ctx)
    backdated_running = InvocationHistory(
        invocation_id=str(emitter.invocation_id),
        status_record=InvocationStatusRecord(status=InvocationStatus.RUNNING),
        runner_context_id=runner_ctx.runner_id,
    )
    backdated_running._timestamp = backdate
    # The emitter's only history record is backdated well before the
    # visible window. Stub get_history so the test is backend-agnostic.
    with patch.object(
        app.state_backend, "get_history", return_value=[backdated_running]
    ):
        app.trigger.store_event(
            EventRecord(
                event_id="evt-ghost",
                event_code="payment.captured",
                timestamp=event_time,
                emitted_by_invocation_id=str(emitter.invocation_id),
                triggered_invocation_ids=[str(emitter.invocation_id)],
            )
        )
        # Mark the event as "triggered" so the default state="triggered"
        # filter in _load_event_markers keeps it in the window.
        app.trigger.link_trigger_run_to_events(
            ["evt-ghost"],
            str(emitter.invocation_id),
            trigger_run_id="run-ghost",
        )

        setup_routes()
        with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
            client = TestClient(pynmon_app)
            response = client.get(
                "/invocations/timeline"
                "?time_range=custom"
                f"&start_date={window_start.isoformat()}"
                f"&end_date={window_end.isoformat()}"
                "&resolution=1ms"
            )

    assert response.status_code == 200
    # The ghost backfill must emit a clickable status segment for the
    # off-screen emitter so the event marker can anchor on top of it.
    assert f'data-invocation-id="{emitter.invocation_id}"' in response.text
    assert "evt-ghost" in response.text


def test_invocations_timeline_backfills_visible_status_left_boundary(
    app: "Pynenc",
) -> None:
    """A visible final status should keep its clipped running segment."""
    from pynenc.invocation.status import InvocationStatusRecord
    from pynenc.state_backend.base_state_backend import InvocationHistory
    from pynenc.trigger.monitoring import EventMarkerPage
    from pynmon.util.svg.models import TimelineConfig
    from pynmon.views.invocations import TimelineRequest, _build_svg_timeline

    app.purge()
    runner_ctx = RunnerContext.from_runner(app.runner)
    app.state_backend.store_runner_context(runner_ctx)
    invocation_id = "11111111-1111-4111-8111-111111111111"
    window_start = datetime(2026, 5, 26, 12, 25, 31, 668000, tzinfo=UTC)
    window_end = window_start + timedelta(milliseconds=300)

    running = InvocationHistory(
        invocation_id=invocation_id,
        status_record=InvocationStatusRecord(status=InvocationStatus.RUNNING),
        runner_context_id=runner_ctx.runner_id,
    )
    running._timestamp = window_start - timedelta(milliseconds=100)
    success = InvocationHistory(
        invocation_id=invocation_id,
        status_record=InvocationStatusRecord(status=InvocationStatus.SUCCESS),
        runner_context_id=runner_ctx.runner_id,
    )
    success._timestamp = window_start + timedelta(milliseconds=80)

    with (
        patch.object(
            app.state_backend,
            "iter_history_in_timerange",
            return_value=iter([[success]]),
        ),
        patch.object(app.state_backend, "get_history", return_value=[running, success]),
        patch.object(
            app.trigger,
            "get_event_markers_in_timerange",
            return_value=EventMarkerPage(markers=[], total=0, truncated=False),
        ),
        patch.object(app.trigger, "get_trigger_runs_in_timerange", return_value=[]),
    ):
        svg = _build_svg_timeline(
            TimelineRequest(
                app=app,
                start_time=window_start,
                end_time=window_end,
                config=TimelineConfig(resolution_seconds=0.001),
                limit=500,
                show_atomic_service=False,
            )
        )

    assert 'class="status-segment"><rect' in svg
    assert f'data-invocation-id="{invocation_id}" data-status="RUNNING"' in svg
    assert f'data-invocation-id="{invocation_id}" data-status="SUCCESS"' in svg


def test_collect_referenced_invocation_ids_picks_marker_and_trigger_run_refs() -> None:
    """Helper aggregates emitter, triggered, and trigger-run invocation IDs."""
    from types import SimpleNamespace

    from pynmon.views.invocations import _collect_referenced_invocation_ids

    marker = SimpleNamespace(
        emitted_by_invocation_id="emitter-1",
        triggered_invocation_ids=["child-1", "child-2"],
    )
    run = SimpleNamespace(
        source_invocation_ids=["src-1", "src-2"],
        triggered_invocation_id="child-3",
    )

    refs = _collect_referenced_invocation_ids([marker], [run])

    assert refs == {"emitter-1", "child-1", "child-2", "src-1", "src-2", "child-3"}


def test_clip_history_to_window_prepends_synthetic_segment_entry() -> None:
    """Clipping a pre-window RUNNING entry produces a synthetic start anchor."""
    from datetime import UTC, datetime, timedelta

    from pynenc.invocation.status import InvocationStatus, InvocationStatusRecord
    from pynenc.state_backend.base_state_backend import InvocationHistory
    from pynmon.views.invocations import _clip_history_to_window

    window_start = datetime(2026, 5, 25, 12, 31, 16, 203000, tzinfo=UTC)
    window_end = window_start + timedelta(milliseconds=300)
    pre = InvocationHistory(
        invocation_id="ghost-inv",
        status_record=InvocationStatusRecord(status=InvocationStatus.RUNNING),
        runner_context_id="runner-x",
    )
    pre._timestamp = window_start - timedelta(seconds=10)

    clipped = _clip_history_to_window([pre], window_start, window_end)

    assert len(clipped) == 1
    assert clipped[0].invocation_id == "ghost-inv"
    assert clipped[0].timestamp == window_start
    assert clipped[0].status_record.status == InvocationStatus.RUNNING


def test_clip_history_to_window_drops_lifecycle_entirely_before_window() -> None:
    """Already-finished invocations before the window produce no ghost entries."""
    from datetime import UTC, datetime, timedelta

    from pynenc.invocation.status import InvocationStatus, InvocationStatusRecord
    from pynenc.state_backend.base_state_backend import InvocationHistory
    from pynmon.views.invocations import _clip_history_to_window

    window_start = datetime(2026, 5, 25, 12, 31, 16, 203000, tzinfo=UTC)
    window_end = window_start + timedelta(milliseconds=300)

    def _hist(status: InvocationStatus, offset_ms: int) -> InvocationHistory:
        h = InvocationHistory(
            invocation_id="done-inv",
            status_record=InvocationStatusRecord(status=status),
            runner_context_id="runner-x",
        )
        h._timestamp = window_start + timedelta(milliseconds=offset_ms)
        return h

    history = [
        _hist(InvocationStatus.RUNNING, -5000),
        _hist(InvocationStatus.SUCCESS, -4000),
    ]

    assert _clip_history_to_window(history, window_start, window_end) == []


def test_invocations_timeline_renders_atomic_service_with_default_filters(
    app: "Pynenc",
) -> None:
    """Blank default filters must not hide recorded atomic-service windows."""
    from datetime import UTC, datetime, timedelta

    app.purge()
    start = datetime(2026, 5, 24, 11, 53, 0, 444000, tzinfo=UTC)
    end = start + timedelta(milliseconds=2)
    app.orchestrator.register_runner_heartbeats(
        ["runner-atomic"], can_run_atomic_service=True
    )
    atomic_service_run = AtomicServiceRun(
        runner_id="runner-atomic",
        atomic_service_run_id="as-timeline-default",
        started_at=start,
    )
    app.orchestrator.record_atomic_service_execution_start(
        atomic_service_run,
        start,
    )
    from pynenc.orchestrator.atomic_service import AtomicServiceExecutionStatus

    app.orchestrator.finalize_atomic_service_execution(
        atomic_service_run,
        end,
        AtomicServiceExecutionStatus.COMPLETED,
    )

    setup_routes()
    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        response = client.get(
            "/invocations/timeline"
            "?time_range=custom"
            "&start_date=2026-05-24T11:53:00.394000"
            "&end_date=2026-05-24T11:53:00.496000"
            "&task_id="
            "&limit=500"
            "&resolution=100ms"
            "&workflow_type="
            "&workflow_id="
            "&inv_ids="
            "&collapse_external=0"
            "&show_system=1"
            "&show_atomic_service=1"
        )

    assert response.status_code == 200
    assert "atomic-service-window" in response.text
    assert "atomic service run" in response.text


def test_family_tree_shows_result_trigger_source_parent(app: "Pynenc") -> None:
    """Result-trigger source invocations appear as indirect parents."""
    from datetime import UTC, datetime

    from pynenc.trigger.monitoring import TriggerRunParticipant, TriggerRunRecord

    app.purge()
    source = DistributedInvocation.isolated(Call(add_task, Arguments({"x": 1, "y": 2})))
    child = DistributedInvocation.isolated(
        Call(multiply_task, Arguments({"a": 3, "b": 4}))
    )
    app.orchestrator.register_new_invocations([source, child])
    now = datetime.now(UTC)
    app.trigger.store_trigger_run(
        TriggerRunRecord(
            trigger_run_id="run-result-parent",
            trigger_id="trg-result-parent",
            task_id_key=multiply_task.task_id.key,
            logic_value="and",
            valid_condition_ids=["vc-result"],
            condition_ids=["c-result"],
            source_invocation_ids=[str(source.invocation_id)],
            triggered_invocation_id=str(child.invocation_id),
            claimed_at=now,
            executed_at=now,
            participants=[
                TriggerRunParticipant(
                    context_type="ResultContext",
                    condition_id="c-result",
                    valid_condition_id="vc-result",
                    source_invocation_id=str(source.invocation_id),
                    context_timestamp=now,
                    context_summary="result:any",
                )
            ],
        )
    )
    setup_routes()

    with patch("pynmon.views.family_tree.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        response = client.get(f"/invocations/{child.invocation_id}/family-tree?bare=1")

    assert response.status_code == 200
    assert str(source.invocation_id) in response.text
    assert str(child.invocation_id) in response.text
    assert "add_task" in response.text
    assert "multiply_task" in response.text
    assert 'class="ft-edge ft-edge-result_trigger"' in response.text
    assert 'data-relation-kind="result_trigger"' in response.text
    assert 'stroke="#15803d"' in response.text
    assert 'stroke-dasharray="6,3"' in response.text


def test_orchestrator_status_filtering_logic(app: "Pynenc") -> None:
    """Test that the orchestrator correctly filters invocations by status."""
    # Clear any existing invocations
    app.purge()

    # Create invocations with different statuses
    call1: Call = Call(add_task, Arguments({"x": 1, "y": 1}))
    call2: Call = Call(add_task, Arguments({"x": 2, "y": 2}))
    call3: Call = Call(add_task, Arguments({"x": 3, "y": 3}))

    invocation1: DistributedInvocation = DistributedInvocation.isolated(call1)
    invocation2: DistributedInvocation = DistributedInvocation.isolated(call2)
    invocation3: DistributedInvocation = DistributedInvocation.isolated(call3)

    # register invocations in the orchestrator
    app.orchestrator.register_new_invocations([invocation1, invocation2, invocation3])

    # Set different statuses (to registered)
    runner_ctx = RunnerContext.from_runner(app.runner)
    app.orchestrator.set_invocation_status(
        invocation2.invocation_id, InvocationStatus.PENDING, runner_ctx
    )
    app.orchestrator.set_invocation_status(
        invocation2.invocation_id, InvocationStatus.RUNNING, runner_ctx
    )
    app.orchestrator.set_invocation_status(
        invocation2.invocation_id, InvocationStatus.SUCCESS, runner_ctx
    )
    app.orchestrator.set_invocation_status(
        invocation3.invocation_id, InvocationStatus.PENDING, runner_ctx
    )
    app.orchestrator.set_invocation_status(
        invocation3.invocation_id, InvocationStatus.RUNNING, runner_ctx
    )
    app.orchestrator.set_invocation_status(
        invocation3.invocation_id, InvocationStatus.FAILED, runner_ctx
    )

    # Test filtering for SUCCESS status only
    success_invocation_ids = list(
        app.orchestrator.get_existing_invocations(
            task=add_task, statuses=[InvocationStatus.SUCCESS]
        )
    )

    assert len(success_invocation_ids) == 1
    assert success_invocation_ids[0] == invocation2.invocation_id

    # Test filtering for multiple statuses
    multiple_status_invocation_ids = set(
        app.orchestrator.get_existing_invocations(
            task=add_task,
            statuses=[InvocationStatus.REGISTERED, InvocationStatus.FAILED],
        )
    )

    assert len(multiple_status_invocation_ids) == 2
    expected_ids = {invocation1.invocation_id, invocation3.invocation_id}
    assert multiple_status_invocation_ids == expected_ids

    # Test no filter (should return all)
    all_invocations = list(app.orchestrator.get_existing_invocations(task=add_task))
    assert len(all_invocations) == 3


def test_invocation_api_endpoint(app: "Pynenc") -> None:
    """Test that invocation API endpoint returns JSON data."""
    # Create an invocation for testing
    call: Call = Call(multiply_task, Arguments({"a": 3, "b": 4}))
    invocation: DistributedInvocation = DistributedInvocation.isolated(call)

    # register invocations in the orchestrator
    app.orchestrator.register_new_invocations([invocation])

    # Setup routes before creating test client
    setup_routes()

    # Patch pynmon to use our test app
    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        response = client.get(f"/invocations/{invocation.invocation_id}/api")

        assert response.status_code == 200
        assert "application/json" in response.headers["content-type"]

        data = response.json()
        # Should contain invocation data
        assert "invocation_id" in data
        assert data["invocation_id"] == invocation.invocation_id


def test_invocation_api_exposes_workflow_role(app: "Pynenc") -> None:
    """Timeline API should expose workflow role classification."""
    app.purge()

    root_invocation = DistributedInvocation.isolated(
        Call(shipping_workflow, Arguments({"order_id": "ORD-2"}))
    )
    workflow_member = DistributedInvocation.from_parent(
        Call(add_task, Arguments({"x": 5, "y": 6})),
        parent_invocation=root_invocation,
    )
    standalone = DistributedInvocation.isolated(
        Call(multiply_task, Arguments({"a": 7, "b": 8}))
    )
    app.orchestrator.register_new_invocations(
        [root_invocation, workflow_member, standalone]
    )

    setup_routes()

    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        root_data = client.get(
            f"/invocations/{root_invocation.invocation_id}/api"
        ).json()
        member_data = client.get(
            f"/invocations/{workflow_member.invocation_id}/api"
        ).json()
        standalone_data = client.get(
            f"/invocations/{standalone.invocation_id}/api"
        ).json()

    assert root_data["workflow_role"]["kind"] == "root"
    assert root_data["workflow_role"]["label"] == "Workflow root"
    assert member_data["workflow_role"]["kind"] == "member"
    assert member_data["workflow_role"]["label"] == "Workflow member"
    assert standalone_data["workflow_role"]["kind"] == "none"
    assert standalone_data["workflow_role"]["label"] == "No workflow"


def test_invocation_history_endpoint(app: "Pynenc") -> None:
    """Test that invocation history endpoint returns JSON data."""
    # Create an invocation for testing
    call: Call = Call(add_task, Arguments({"x": 1, "y": 2}))
    invocation: DistributedInvocation = DistributedInvocation.isolated(call)

    # register invocations in the orchestrator
    app.orchestrator.register_new_invocations([invocation])

    # Setup routes before creating test client
    setup_routes()

    # Patch pynmon to use our test app
    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        response = client.get(f"/invocations/{invocation.invocation_id}/history")

        assert response.status_code == 200
        assert "application/json" in response.headers["content-type"]

        data = response.json()
        # Should be a list (even if empty)
        assert isinstance(data, list)


def test_invocation_detail_status_timeline_renders_status_badges(
    app: "Pynenc",
) -> None:
    """The full detail status timeline should show visible status labels."""
    app.purge()
    call: Call = Call(add_task, Arguments({"x": 1, "y": 2}))
    invocation: DistributedInvocation = DistributedInvocation.isolated(call)
    app.orchestrator.register_new_invocations([invocation])
    runner_ctx = RunnerContext.from_runner(app.runner)
    for status in (
        InvocationStatus.PENDING,
        InvocationStatus.RUNNING,
        InvocationStatus.PAUSED,
    ):
        app.orchestrator.set_invocation_status(
            invocation.invocation_id, status, runner_ctx
        )

    setup_routes()

    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        detail_response = client.get(f"/invocations/{invocation.invocation_id}")
        history_response = client.get(
            f"/invocations/{invocation.invocation_id}/history"
        )

    assert detail_response.status_code == 200
    content = detail_response.text
    assert "Status timeline" in content
    assert "pynmon-status-badge" in content
    assert "background-color:" in content
    assert "color: #ffffff;" in content
    assert ">PENDING</span" in content
    assert ">RUNNING</span" in content
    assert ">PAUSED</span" in content

    assert history_response.status_code == 200
    history = history_response.json()
    assert history
    assert all(entry.get("status") for entry in history)


# ################################################################################### #
# PARAMETRIZED ERROR-CASE TESTS
# ################################################################################### #


@pytest.mark.parametrize(
    "status_value",
    ["nonexistent", "INVALID_STATUS", "12345", "success; DROP TABLE"],
)
def test_invocations_list_should_return_empty_for_invalid_status(
    app: "Pynenc", status_value: str
) -> None:
    """Test that invalid status filter values produce a valid (empty) response."""
    app.purge()
    _create_invocations_with_statuses(app)
    setup_routes()

    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        response = client.get(f"/invocations/?status={status_value}")

        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]


@pytest.mark.parametrize(
    "limit,page,expected_status",
    [
        (0, 1, 200),  # zero limit → clamped to 1
        (-5, 1, 200),  # negative limit → clamped to 1
        (50, 0, 200),  # zero page → clamped to 1
        (50, -1, 200),  # negative page → clamped to 1
        (9999, 1, 200),  # very large limit → clamped to 1000
    ],
)
def test_invocations_list_should_handle_pagination_edge_cases(
    app: "Pynenc", limit: int, page: int, expected_status: int
) -> None:
    """Test that pagination edge cases are handled gracefully."""
    app.purge()
    setup_routes()

    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        response = client.get(f"/invocations/?limit={limit}&page={page}")
        assert response.status_code == expected_status


def test_invocations_list_should_handle_missing_task_id_key(app: "Pynenc") -> None:
    """Test that a non-existent task_id key returns a valid empty page."""
    app.purge()
    _create_invocations_with_statuses(app)
    setup_routes()

    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        response = client.get("/invocations/?task_id=nonexistent.module:no_func")

        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]


# ################################################################################### #
# TRIGGER ORIGIN TESTS (Step 2.2)
# ################################################################################### #


def _store_trigger_run_for(app: "Pynenc", invocation_id: str) -> None:
    """Persist a trigger run that produced ``invocation_id``."""
    from datetime import UTC, datetime

    from pynenc.trigger.monitoring import TriggerRunRecord

    now = datetime.now(UTC)
    app.trigger.store_trigger_run(
        TriggerRunRecord(
            trigger_run_id="run-xyz",
            trigger_id="trg-xyz",
            task_id_key=add_task.task_id.key,
            logic_value="EVENT",
            valid_condition_ids=["cond-1"],
            condition_ids=["cond-1"],
            event_ids=["evt-origin"],
            source_invocation_ids=[],
            triggered_invocation_id=invocation_id,
            claimed_at=now,
            executed_at=now,
        )
    )


def test_invocation_detail_shows_trigger_origin(app: "Pynenc") -> None:
    """Invocation detail renders trigger source context when a run exists."""
    call: Call = Call(add_task, Arguments({"x": 1, "y": 2}))
    invocation: DistributedInvocation = DistributedInvocation.isolated(call)
    app.orchestrator.register_new_invocations([invocation])
    _store_trigger_run_for(app, invocation.invocation_id)
    setup_routes()

    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        response = client.get(f"/invocations/{invocation.invocation_id}")

    assert response.status_code == 200
    assert "Triggering information" in response.text
    assert "Trigger origin" in response.text
    assert "trg-xyz" in response.text
    assert "evt-origin"[:8] in response.text


def test_invocation_detail_no_trigger_origin(app: "Pynenc") -> None:
    """Invocations with no trigger run do not show the trigger origin block."""
    call: Call = Call(add_task, Arguments({"x": 1, "y": 2}))
    invocation: DistributedInvocation = DistributedInvocation.isolated(call)
    app.orchestrator.register_new_invocations([invocation])
    setup_routes()

    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        response = client.get(f"/invocations/{invocation.invocation_id}")

    assert response.status_code == 200
    assert "Trigger origin" not in response.text


def test_invocation_api_includes_triggered_by(app: "Pynenc") -> None:
    """The /api endpoint always exposes a triggered_by key (may be null)."""
    call: Call = Call(add_task, Arguments({"x": 1, "y": 2}))
    invocation: DistributedInvocation = DistributedInvocation.isolated(call)
    app.orchestrator.register_new_invocations([invocation])
    setup_routes()

    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        response = client.get(f"/invocations/{invocation.invocation_id}/api")

    assert response.status_code == 200
    data = response.json()
    assert "triggered_by" in data
    assert data["triggered_by"] is None


def test_invocation_api_triggered_by_populated(app: "Pynenc") -> None:
    """When a trigger run exists, /api returns the linked metadata."""
    call: Call = Call(add_task, Arguments({"x": 1, "y": 2}))
    invocation: DistributedInvocation = DistributedInvocation.isolated(call)
    app.orchestrator.register_new_invocations([invocation])
    _store_trigger_run_for(app, invocation.invocation_id)
    setup_routes()

    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        response = client.get(f"/invocations/{invocation.invocation_id}/api")

    assert response.status_code == 200
    data = response.json()
    assert data["triggered_by"] is not None
    assert data["triggered_by"]["trigger_id"] == "trg-xyz"
    assert data["triggered_by"]["event_ids"] == ["evt-origin"]


def test_invocation_api_participants_include_context_timestamp(app: "Pynenc") -> None:
    """Timeline zoom needs participant timestamps to include trigger origins."""
    call: Call = Call(add_task, Arguments({"x": 1, "y": 2}))
    invocation: DistributedInvocation = DistributedInvocation.isolated(call)
    app.orchestrator.register_new_invocations([invocation])
    _store_trigger_run_with_participants(app, invocation.invocation_id)
    setup_routes()

    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        response = client.get(f"/invocations/{invocation.invocation_id}/api")

    assert response.status_code == 200
    participant = response.json()["triggered_by"]["participants"][0]
    assert participant["context_timestamp"] is not None
    assert participant["context_summary"] == "status:SUCCESS"


def test_invocation_views_hide_stale_composite_participants(app: "Pynenc") -> None:
    """Existing runs project only the source that completed an AND trigger."""
    call: Call = Call(add_task, Arguments({"x": 1, "y": 2}))
    invocation: DistributedInvocation = DistributedInvocation.isolated(call)
    app.orchestrator.register_new_invocations([invocation])
    now = datetime.now(UTC)
    app.trigger.store_trigger_run(
        TriggerRunRecord(
            trigger_run_id="run-composite",
            trigger_id="trigger-composite",
            task_id_key=add_task.task_id.key,
            logic_value="and",
            valid_condition_ids=["old-status", "matched-status", "matched-result"],
            condition_ids=["status-condition", "status-condition", "result-condition"],
            source_invocation_ids=["old-source", "matching-source", "matching-source"],
            triggered_invocation_id=str(invocation.invocation_id),
            claimed_at=now,
            executed_at=now,
            participants=[
                TriggerRunParticipant(
                    context_type="StatusContext",
                    condition_id="status-condition",
                    valid_condition_id="old-status",
                    source_invocation_id="old-source",
                    context_timestamp=now - timedelta(seconds=1),
                ),
                TriggerRunParticipant(
                    context_type="StatusContext",
                    condition_id="status-condition",
                    valid_condition_id="matched-status",
                    source_invocation_id="matching-source",
                    context_timestamp=now,
                ),
                TriggerRunParticipant(
                    context_type="ResultContext",
                    condition_id="result-condition",
                    valid_condition_id="matched-result",
                    source_invocation_id="matching-source",
                    context_timestamp=now,
                ),
            ],
        )
    )
    setup_routes()

    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        api_response = client.get(f"/invocations/{invocation.invocation_id}/api")
        detail_response = client.get(f"/invocations/{invocation.invocation_id}")

    assert api_response.status_code == 200
    participants = api_response.json()["triggered_by"]["participants"]
    assert [p["source_invocation_id"] for p in participants] == [
        "matching-source",
        "matching-source",
    ]
    assert detail_response.status_code == 200
    assert "matching-source" in detail_response.text
    assert 'href="/invocations/old-source"' not in detail_response.text
    assert 'data-source-invocation-id="old-source"' not in detail_response.text


# --------------------------------------------------------------------------- #
# Phase 6: participant tables + reverse trigger effects
# --------------------------------------------------------------------------- #


def _store_trigger_run_with_participants(app: "Pynenc", invocation_id: str) -> None:
    """Store a trigger run with one StatusContext participant for tests."""
    from datetime import UTC, datetime

    from pynenc.trigger.monitoring import TriggerRunParticipant, TriggerRunRecord

    now = datetime.now(UTC)
    app.trigger.store_trigger_run(
        TriggerRunRecord(
            trigger_run_id="run-p6",
            trigger_id="trg-p6",
            task_id_key=add_task.task_id.key,
            logic_value="and",
            valid_condition_ids=["cond-1"],
            condition_ids=["cond-1"],
            event_ids=[],
            source_invocation_ids=["src-inv-p6"],
            triggered_invocation_id=invocation_id,
            claimed_at=now,
            executed_at=now,
            participants=[
                TriggerRunParticipant(
                    context_type="StatusContext",
                    condition_id="cond-1",
                    valid_condition_id="vc-1",
                    source_invocation_id="src-inv-p6",
                    context_timestamp=now,
                    context_summary="status:SUCCESS",
                )
            ],
        )
    )


def _store_trigger_run_sourced_by(app: "Pynenc", source_invocation_id: str) -> str:
    """Store a trigger run whose participant points at *source_invocation_id*."""
    from datetime import UTC, datetime

    from pynenc.trigger.monitoring import TriggerRunParticipant, TriggerRunRecord

    now = datetime.now(UTC)
    child_invocation_id = "child-inv-p6"
    app.trigger.store_trigger_run(
        TriggerRunRecord(
            trigger_run_id="run-rev",
            trigger_id="trg-rev",
            task_id_key="pkg.task.child",
            logic_value="and",
            valid_condition_ids=["vc-status-rev", "vc-result-rev"],
            condition_ids=["c-status-rev", "c-result-rev"],
            event_ids=[],
            source_invocation_ids=[source_invocation_id],
            triggered_invocation_id=child_invocation_id,
            claimed_at=now,
            executed_at=now,
            participants=[
                TriggerRunParticipant(
                    context_type="StatusContext",
                    condition_id="c-status-rev",
                    valid_condition_id="vc-status-rev",
                    source_invocation_id=source_invocation_id,
                    context_timestamp=now,
                    context_summary="status:SUCCESS",
                ),
                TriggerRunParticipant(
                    context_type="ResultContext",
                    condition_id="c-result-rev",
                    valid_condition_id="vc-result-rev",
                    source_invocation_id=source_invocation_id,
                    context_timestamp=now,
                    context_summary="result:{'accepted': true}",
                ),
            ],
        )
    )
    return child_invocation_id


def test_invocation_detail_renders_participants_table(app: "Pynenc") -> None:
    """The trigger origin section renders one row per participant."""
    call: Call = Call(add_task, Arguments({"x": 1, "y": 2}))
    invocation: DistributedInvocation = DistributedInvocation.isolated(call)
    app.orchestrator.register_new_invocations([invocation])
    _store_trigger_run_with_participants(app, invocation.invocation_id)
    setup_routes()

    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        response = client.get(f"/invocations/{invocation.invocation_id}")

    assert response.status_code == 200
    assert 'data-trigger-run-id="run-p6"' in response.text
    assert 'data-context-type="StatusContext"' in response.text
    assert 'data-source-invocation-id="src-inv-p6"' in response.text
    assert "status:SUCCESS" in response.text


def test_invocation_detail_shows_reverse_trigger_effects(app: "Pynenc") -> None:
    """Triggers caused by this invocation render as a dedicated section."""
    call: Call = Call(add_task, Arguments({"x": 1, "y": 2}))
    invocation: DistributedInvocation = DistributedInvocation.isolated(call)
    app.orchestrator.register_new_invocations([invocation])
    child_id = _store_trigger_run_sourced_by(app, str(invocation.invocation_id))
    setup_routes()

    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        response = client.get(f"/invocations/{invocation.invocation_id}")

    assert response.status_code == 200
    assert "Triggers caused by this invocation" in response.text
    assert 'data-trigger-run-id="run-rev"' in response.text
    assert f"/invocations/{child_id}" in response.text
    assert "Matched conditions" in response.text
    assert 'data-context-type="StatusContext"' in response.text
    assert 'data-context-type="ResultContext"' in response.text
    assert "c-status-rev" in response.text
    assert "c-result-rev" in response.text
    assert "status:SUCCESS" in response.text
    assert "result:{&#39;accepted&#39;: true}" in response.text


def test_invocation_api_includes_reverse_trigger_effects(app: "Pynenc") -> None:
    """Timeline detail API exposes trigger runs caused by the selected source."""
    call: Call = Call(add_task, Arguments({"x": 1, "y": 2}))
    invocation: DistributedInvocation = DistributedInvocation.isolated(call)
    app.orchestrator.register_new_invocations([invocation])
    child_id = _store_trigger_run_sourced_by(app, str(invocation.invocation_id))
    setup_routes()

    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        response = client.get(f"/invocations/{invocation.invocation_id}/api")

    assert response.status_code == 200
    data = response.json()
    assert data["triggered_runs_caused"][0]["trigger_run_id"] == "run-rev"
    assert data["triggered_runs_caused"][0]["triggered_invocation_id"] == child_id
    assert child_id in data["source_inv_summaries"]


def test_invocation_detail_no_reverse_trigger_effects(app: "Pynenc") -> None:
    """When no trigger run is sourced by this invocation, the section is hidden."""
    call: Call = Call(add_task, Arguments({"x": 1, "y": 2}))
    invocation: DistributedInvocation = DistributedInvocation.isolated(call)
    app.orchestrator.register_new_invocations([invocation])
    # Store the inbound trigger so the related-events panel is rendered but
    # the outbound triggers section should remain hidden.
    _store_trigger_run_with_participants(app, invocation.invocation_id)
    setup_routes()

    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        response = client.get(f"/invocations/{invocation.invocation_id}")

    assert response.status_code == 200
    # The "Triggers caused" section is collapsed entirely when there are none.
    assert "Triggers caused by this invocation" not in response.text
    # The inbound trigger section is still visible.
    assert "Trigger origin" in response.text


def test_condition_types_by_event_deduplicates_participant_contexts() -> None:
    """Event marker overlays should show unique condition context colours."""
    from pynenc.trigger.monitoring import TriggerRunParticipant, TriggerRunRecord
    from pynmon.views.invocations import _condition_types_by_event

    runs = [
        TriggerRunRecord(
            trigger_run_id="run-contexts",
            trigger_id="trg-contexts",
            task_id_key="pkg.task",
            logic_value="and",
            participants=[
                TriggerRunParticipant(
                    context_type="EventContext",
                    event_id="event-1",
                ),
                TriggerRunParticipant(
                    context_type="ResultContext",
                    event_id="event-1",
                ),
                TriggerRunParticipant(
                    context_type="EventContext",
                    event_id="event-1",
                ),
                TriggerRunParticipant(
                    context_type="StatusContext",
                    event_id="event-2",
                ),
            ],
        )
    ]

    assert _condition_types_by_event(runs) == {
        "event-1": ["EventContext", "ResultContext"],
        "event-2": ["StatusContext"],
    }
