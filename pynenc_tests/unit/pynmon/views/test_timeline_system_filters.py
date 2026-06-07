"""Unit tests for timeline system-task filtering helpers."""

from datetime import UTC, datetime

from pynenc.identifiers.task_id import TaskId
from pynenc_tests.conftest import MockPynenc
from pynmon.util.svg.models import TimelineConfig
from pynmon.views.invocations import TimelineRequest, _load_system_task_invocation_ids


def _request(show_system_tasks: bool) -> tuple[TimelineRequest, MockPynenc]:
    app = MockPynenc()
    sys_task = TaskId.from_key("pynenc.core_tasks.recover_pending_invocations")
    app._tasks[sys_task] = object()  # type: ignore[assignment]
    app._tasks[TaskId.from_key("orders.capture")] = object()  # type: ignore[assignment]
    app.orchestrator.get_task_invocation_ids.return_value = ["sys-inv-1", "sys-inv-2"]
    app.state_backend.iter_history_in_timerange.return_value = iter([])
    return TimelineRequest(
        app=app,
        start_time=datetime(2026, 5, 24, 10, 0, tzinfo=UTC),
        end_time=datetime(2026, 5, 24, 10, 5, tzinfo=UTC),
        config=TimelineConfig(),
        limit=None,
        show_system_tasks=show_system_tasks,
    ), app


def test_system_task_filter_collects_core_task_invocations() -> None:
    req, app = _request(show_system_tasks=False)

    ids = _load_system_task_invocation_ids(req)

    assert ids == {"sys-inv-1", "sys-inv-2"}
    assert app.orchestrator.get_task_invocation_ids.call_count == 1


def test_system_task_filter_noops_when_system_tasks_visible() -> None:
    req, app = _request(show_system_tasks=True)

    ids = _load_system_task_invocation_ids(req)

    assert ids == set()
    assert app.orchestrator.get_task_invocation_ids.call_count == 0
