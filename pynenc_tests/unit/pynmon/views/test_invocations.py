"""
Minimal tests for pynmon invocation view.

Tests basic functionality using real in-memory Pynenc app with actual tasks and invocations.
"""

# Skip all pynmon tests if monitor dependencies are not available
import pytest

pytest.importorskip("fastapi", reason="pynmon tests require monitor dependencies")
pytest.importorskip("jinja2", reason="pynmon tests require monitor dependencies")

# All imports below must come after pytest.importorskip calls
# ruff: noqa: E402

from typing import TYPE_CHECKING
from unittest.mock import patch

from fastapi.testclient import TestClient

from pynenc.arguments import Arguments
from pynenc.call import Call
from pynenc.invocation import DistributedInvocation
from pynenc.invocation.status import InvocationStatus
from pynenc.runner.runner_context import RunnerContext
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


@pytest.fixture
def app(request: "FixtureRequest", app_instance: "Pynenc") -> "Pynenc":
    app = app_instance
    app._tasks = mock_app._tasks
    add_task.app = app
    multiply_task.app = app
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
    _create_invocations_with_statuses(app)

    setup_routes()

    with patch("pynmon.views.invocations.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)

        response = client.get("/invocations/?status=success")
        assert response.status_code == 200
        content = response.text

        # Should contain only the SUCCESS status badge
        assert re.search(r"bg-success[^>]*>[^<]*SUCCESS[^<]*</span>", content), (
            "Expected SUCCESS badge not found"
        )
        assert not re.search(r"bg-dark[^>]*>[^<]*REGISTERED[^<]*</span>", content), (
            "REGISTERED badge should not be present"
        )
        assert not re.search(r"bg-danger[^>]*>[^<]*FAILED[^<]*</span>", content), (
            "FAILED badge should not be present"
        )

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
