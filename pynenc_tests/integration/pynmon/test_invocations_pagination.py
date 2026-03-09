"""Integration tests for pynmon invocations pagination."""

import re
import threading
from typing import TYPE_CHECKING

from pynenc.builder import PynencBuilder

if TYPE_CHECKING:
    from pynenc_tests.integration.pynmon.conftest import PynmonClient

app = (
    PynencBuilder()
    .memory()
    .thread_runner()
    .app_id("test-pynmon-pagination-app")
    .build()
)


@app.task
def simple_task(n: int) -> int:
    """Simple task for pagination testing."""
    return n * 2


def _extract_invocation_ids(html: str) -> set[str]:
    """Extract invocation IDs from HTML response."""
    # Match UUIDs in the format: /invocations/UUID
    pattern = r'/invocations/([a-f0-9-]{36})"'
    return set(re.findall(pattern, html))


def test_invocations_pagination(pynmon_client: "PynmonClient") -> None:
    """Test that pagination returns different invocations on each page."""
    app.purge()

    runner_thread = threading.Thread(target=app.runner.run, daemon=True)
    runner_thread.start()
    try:
        # Create 6 invocations
        invocations = [simple_task(i) for i in range(6)]
        for inv in invocations:
            _ = inv.result  # Wait for completion

        # Get page 1 (limit=3)
        response1 = pynmon_client.get("/invocations/?limit=3&page=1")
        assert response1.status_code == 200

        # Get page 2 (limit=3)
        response2 = pynmon_client.get("/invocations/?limit=3&page=2")
        assert response2.status_code == 200

        # Extract invocation IDs from responses
        page1_ids = _extract_invocation_ids(response1.text)
        page2_ids = _extract_invocation_ids(response2.text)

        # Verify each page has 3 invocations
        assert len(page1_ids) == 3
        assert len(page2_ids) == 3

        # Verify pages have different invocations (no overlap)
        assert page1_ids.isdisjoint(page2_ids)

    finally:
        app.runner.stop_runner_loop()
