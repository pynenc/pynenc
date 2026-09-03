"""Unit tests for pynmon workflow views."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest
from fastapi.responses import HTMLResponse
from fastapi.testclient import TestClient

from pynenc.identifiers.invocation_id import InvocationId
from pynenc.identifiers.task_id import TaskId
from pynenc.invocation.status import InvocationStatus
from pynenc.workflow.workflow_identity import WorkflowIdentity
from pynmon.app import app as pynmon_app
from pynmon.app import setup_routes
from pynmon.views import workflows as workflow_views

setup_routes()


class _TemplateRecorder:
    """Capture template name and context while returning a valid response."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def __call__(
        self,
        request: Any,
        name: str,
        context: dict[str, Any] | None = None,
        status_code: int = 200,
        **_: Any,
    ) -> HTMLResponse:
        self.calls.append(
            {
                "name": name,
                "context": context or {},
                "status_code": status_code,
            }
        )
        return HTMLResponse(f"template:{name}", status_code=status_code)

    @property
    def last_call(self) -> dict[str, Any]:
        assert self.calls, "expected a template to be rendered"
        return self.calls[-1]


class _StateBackend:
    def __init__(
        self,
        *,
        workflow_types: list[TaskId] | None = None,
        runs_by_type: dict[str, list[Any]] | None = None,
        all_runs: list[Any] | None = None,
        parent_invocations: dict[str, Any] | None = None,
        workflow_invocations: dict[str, list[str]] | None = None,
        histories: dict[str, list[Any]] | None = None,
    ) -> None:
        self.workflow_types = workflow_types or []
        self.runs_by_type = runs_by_type or {}
        self.all_runs = all_runs or []
        self.parent_invocations = parent_invocations or {}
        self.workflow_invocations = workflow_invocations or {}
        self.histories = histories or {}

    def get_all_workflow_types(self) -> Any:
        return iter(self.workflow_types)

    def get_workflow_runs(self, workflow_type: TaskId) -> Any:
        return iter(self.runs_by_type.get(workflow_type.key, []))

    def get_all_workflow_runs(self) -> Any:
        return iter(self.all_runs)

    def get_invocation(self, invocation_id: InvocationId) -> Any:
        try:
            return self.parent_invocations[str(invocation_id)]
        except KeyError as exc:
            raise LookupError(invocation_id) from exc

    def get_invocation_ids_by_workflow(self, *, workflow_id: str) -> Any:
        return iter(self.workflow_invocations.get(workflow_id, []))

    def get_history(self, invocation_id: InvocationId) -> list[Any]:
        return self.histories.get(str(invocation_id), [])


@pytest.fixture
def template_recorder(monkeypatch: pytest.MonkeyPatch) -> _TemplateRecorder:
    recorder = _TemplateRecorder()
    monkeypatch.setattr(
        workflow_views,
        "templates",
        SimpleNamespace(TemplateResponse=recorder),
    )
    return recorder


def _run(
    workflow_id: str,
    workflow_type: TaskId,
    *,
    parent_workflow_id: str | None = None,
    created_at: str | None = None,
) -> Any:
    run = SimpleNamespace(
        workflow_id=InvocationId(workflow_id),
        workflow_type=workflow_type,
        parent_workflow_id=(
            InvocationId(parent_workflow_id) if parent_workflow_id else None
        ),
    )
    if created_at is not None:
        run.created_at = created_at
    return run


def _app(
    state_backend: _StateBackend, *, tasks: dict[TaskId, Any] | None = None
) -> Any:
    return SimpleNamespace(
        app_id="workflow-view-test",
        state_backend=state_backend,
        tasks=tasks or {},
    )


def test_workflows_list_builds_counts_and_latest_run(
    template_recorder: _TemplateRecorder,
) -> None:
    workflow_type = TaskId("tests.workflows", "daily_import")
    other_type = TaskId("tests.workflows", "cleanup")
    first_run = _run("wf-first", workflow_type)
    second_run = _run("wf-second", workflow_type)
    cleanup_run = _run("wf-cleanup", other_type)
    app = _app(
        _StateBackend(
            workflow_types=[workflow_type, other_type],
            runs_by_type={
                workflow_type.key: [first_run, second_run],
                other_type.key: [cleanup_run],
            },
        )
    )

    with patch("pynmon.views.workflows.get_pynenc_instance", return_value=app):
        response = TestClient(pynmon_app).get("/workflows/")

    assert response.status_code == 200
    call = template_recorder.last_call
    assert call["name"] == "workflows/list.html"
    assert call["context"]["app_id"] == "workflow-view-test"
    assert call["context"]["workflows"] == [
        {
            "workflow_type": workflow_type,
            "run_count": 2,
            "latest_run": first_run,
        },
        {
            "workflow_type": other_type,
            "run_count": 1,
            "latest_run": cleanup_run,
        },
    ]


def test_workflow_run_rows_resolve_parent_workflow_and_tolerate_missing_parent() -> (
    None
):
    workflow_type = TaskId("tests.workflows", "child")
    parent_workflow = WorkflowIdentity(
        workflow_id=InvocationId("parent-wf"),
        workflow_type=TaskId("tests.workflows", "parent"),
    )
    child_run = _run("child-wf", workflow_type, parent_workflow_id="parent-wf")
    orphan_run = _run("orphan-wf", workflow_type, parent_workflow_id="missing-wf")
    app = _app(
        _StateBackend(
            parent_invocations={
                "parent-wf": SimpleNamespace(workflow=parent_workflow),
            }
        )
    )

    rows = workflow_views._workflow_run_rows(app, [child_run, orphan_run])

    assert rows[0]["workflow_id"] == InvocationId("child-wf")
    assert rows[0]["parent_workflow_id"] == InvocationId("parent-wf")
    assert rows[0]["parent_workflow"] == parent_workflow
    assert rows[1]["workflow_id"] == InvocationId("orphan-wf")
    assert rows[1]["parent_workflow_id"] == InvocationId("missing-wf")
    assert rows[1]["parent_workflow"] is None


def test_workflow_runs_list_sorts_newest_first(
    template_recorder: _TemplateRecorder,
) -> None:
    workflow_type = TaskId("tests.workflows", "sort_me")
    older_run = _run("older-wf", workflow_type, created_at="2026-06-01T12:00:00")
    newer_run = _run("newer-wf", workflow_type, created_at="2026-06-02T12:00:00")
    app = _app(_StateBackend(all_runs=[older_run, newer_run]))

    with patch("pynmon.views.workflows.get_pynenc_instance", return_value=app):
        response = TestClient(pynmon_app).get("/workflows/runs")

    assert response.status_code == 200
    call = template_recorder.last_call
    assert call["name"] == "workflows/runs.html"
    rendered_runs = call["context"]["workflow_runs"]
    assert [row["workflow_id"] for row in rendered_runs] == [
        InvocationId("newer-wf"),
        InvocationId("older-wf"),
    ]


def test_workflow_detail_invalid_key_renders_not_found(
    template_recorder: _TemplateRecorder,
) -> None:
    app = _app(_StateBackend())

    with (
        patch("pynmon.views.workflows.get_pynenc_instance", return_value=app),
        patch.object(workflow_views.logger, "exception") as log_exception,
    ):
        response = TestClient(pynmon_app).get("/workflows/not-a-task-id")

    assert response.status_code == 404
    log_exception.assert_called_once()
    call = template_recorder.last_call
    assert call["name"] == "shared/error.html"
    assert call["status_code"] == 404
    assert call["context"]["error_title"] == "Workflow Not Found"


def test_workflow_detail_builds_selected_histograms_with_shared_scale(
    template_recorder: _TemplateRecorder,
) -> None:
    workflow_type = TaskId("tests.workflows", "daily_import")
    first_run = _run("wf-first", workflow_type, created_at="2026-09-02T12:00:00")
    second_run = _run("wf-second", workflow_type, created_at="2026-09-02T11:00:00")
    start = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)

    def history(invocation_id: str) -> list[Any]:
        return [
            SimpleNamespace(
                timestamp=start,
                status_record=SimpleNamespace(status=InvocationStatus.RUNNING),
            ),
            SimpleNamespace(
                timestamp=start + timedelta(seconds=5),
                status_record=SimpleNamespace(status=InvocationStatus.SUCCESS),
            ),
        ]

    invocations = {
        invocation_id: SimpleNamespace(
            task=SimpleNamespace(task_id=TaskId("tests.tasks", invocation_id))
        )
        for invocation_id in ("wf-first", "wf-second")
    }
    app = _app(
        _StateBackend(
            runs_by_type={workflow_type.key: [first_run, second_run]},
            parent_invocations=invocations,
            workflow_invocations={"wf-first": [], "wf-second": []},
            histories={
                "wf-first": history("wf-first"),
                "wf-second": history("wf-second"),
            },
        )
    )

    with patch("pynmon.views.workflows.get_pynenc_instance", return_value=app):
        response = TestClient(pynmon_app).get(
            f"/workflows/{workflow_type.key}?histogram_status=running"
        )

    assert response.status_code == 200
    context = template_recorder.last_call["context"]
    assert context["histogram_status"] == "running"
    assert len(context["workflow_histograms"]) == 2
    assert all(
        'data-statuses="running"' in item["histogram"]["svg"]
        for item in context["workflow_histograms"]
    )
