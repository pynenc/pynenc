"""
Shared fixtures for deterministic workflow tests.

Provides common test setup including app configuration, workflow identity,
and deterministic executor instances used across all deterministic test modules.
"""

from typing import TYPE_CHECKING

import pytest

from pynenc import Pynenc
from pynenc.workflow.workflow_deterministic import DeterministicExecutor
from pynenc.workflow.workflow_identity import WorkflowIdentity

if TYPE_CHECKING:
    from pynenc.task import Task


def add_task_func(a: int, b: int) -> int:
    """Simple addition function for task testing."""
    return a + b


@pytest.fixture
def app() -> Pynenc:
    """Create a test app with real memory state backend and thread runner."""
    config = {
        "runner_cls": "ThreadRunner",
        "state_backend_cls": "MemStateBackend",
        "broker_cls": "MemBroker",
        "orchestrator_cls": "MemOrchestrator",
        "client_data_store_cls": "MemClientDataStore",
    }
    app = Pynenc(app_id="test_deterministic", config_values=config)
    app.purge()
    return app


@pytest.fixture
def app_with_task(app: Pynenc) -> "Task":
    """Create a test app with a simple task for execute_task testing."""
    task = app.task(add_task_func)
    return task


@pytest.fixture
def deterministic_executor(
    app: Pynenc, workflow_identity: WorkflowIdentity
) -> DeterministicExecutor:
    """Create a deterministic executor for testing."""
    return DeterministicExecutor(workflow_identity, app)
