"""
Minimal tests for pynmon broker view.

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

from pynenc_tests.conftest import MockPynenc
from pynmon.app import app as pynmon_app
from pynmon.app import setup_routes

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest

    from pynenc import Pynenc

# Module level app and task setup
mock_app = MockPynenc(app_id="test-broker-app")


@mock_app.task
def task_one(x: int) -> int:
    """Simple test task that returns double the input."""
    return x * 2


@mock_app.task
def task_two(duration: int) -> str:
    """Task that simulates work but doesn't actually sleep in tests."""
    return f"completed after {duration}s"


@pytest.fixture
def app(request: "FixtureRequest", app_instance: "Pynenc") -> "Pynenc":
    app = app_instance
    app._app_id = mock_app.app_id
    app._tasks = mock_app._tasks
    task_one.app = app
    task_two.app = app
    app.purge()
    request.addfinalizer(app.purge)
    return app


def test_broker_overview_shows_broker_info(app: "Pynenc") -> None:
    """Test that broker overview displays broker information."""
    # Setup routes before creating test client
    setup_routes()

    # Patch pynmon to use our test app
    with patch("pynmon.views.broker.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        response = client.get("/broker/")

        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

        # Check that broker info is in the response
        content = response.text
        assert "test-broker-app" in content
        assert "BaseBroker" not in content
        assert app.broker.__class__.__name__ in content


def test_broker_queue_shows_pending_invocations(app: "Pynenc") -> None:
    """Test that broker queue displays pending invocations."""
    # Manually create and route invocations to ensure they stay in the queue
    from pynenc.arguments import Arguments
    from pynenc.call import Call
    from pynenc.invocation import DistributedInvocation

    # Debug: Check initial state
    print(f"Initial queue count: {app.broker.count_invocations()}")

    # Create calls for our tasks
    call1: Call = Call(task_one, Arguments({"x": 1}))
    call2: Call = Call(task_two, Arguments({"duration": 5}))

    # Create invocations
    invocation1: DistributedInvocation = DistributedInvocation(call1)
    invocation2: DistributedInvocation = DistributedInvocation(call2)

    print(
        f"Created invocations: {invocation1.invocation_id[:8]}, {invocation2.invocation_id[:8]}"
    )

    # Manually route them to the broker to ensure they are queued
    app.broker.route_invocation(invocation1)
    app.broker.route_invocation(invocation2)

    print(f"Queue count after routing: {app.broker.count_invocations()}")

    # Test retrieve directly
    retrieved = app.broker.retrieve_invocation()
    print(f"Retrieved: {retrieved.invocation_id[:8] if retrieved else None}")

    # Re-route for the actual test
    if retrieved:
        app.broker.route_invocation(retrieved)

    # Setup routes before creating test client
    setup_routes()

    # Patch pynmon to use our test app
    with patch("pynmon.views.broker.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)
        response = client.get("/broker/queue")

        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

        content = response.text

        # Should show our queued invocations (first 8 chars of invocation_id)
        assert invocation1.invocation_id[:8] in content
        assert invocation2.invocation_id[:8] in content
        assert "task_one" in content
        assert "task_two" in content


def test_broker_purge_clears_queue(app: "Pynenc") -> None:
    """Test that purging broker clears the queue."""
    # Create some invocations first
    task_one(10)
    task_one(20)

    # Setup routes before creating test client
    setup_routes()

    # Patch pynmon to use our test app
    with patch("pynmon.views.broker.get_pynenc_instance", return_value=app):
        client = TestClient(pynmon_app)

        # Verify queue has items
        response = client.get("/broker/queue")
        assert "task_one" in response.text

        # Purge the queue
        purge_response = client.post("/broker/purge")
        assert purge_response.status_code == 200
        assert purge_response.json()["success"] is True

        # Verify queue is now empty
        response = client.get("/broker/queue")
        # After purge, should have no pending invocations
        assert app.broker.count_invocations() == 0
