"""Shared helpers for the pynmon atomic-service timeline + detail pages."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from math import ceil
from typing import TYPE_CHECKING
from urllib.parse import quote, urlencode

from fastapi import Request

from pynmon.util.timeline_zoom import calculate_timeline_zoom_window

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.orchestrator.atomic_service import AtomicServiceExecution


ATOMIC_TIMELINE_PAGE_SIZE_CHOICES: tuple[int, ...] = (100, 1000)
ATOMIC_TIMELINE_MIN_DURATION_CHOICES: tuple[tuple[str, float], ...] = (
    ("Any", 0.0),
    ("\u2265 0.01s", 0.01),
    ("\u2265 0.1s", 0.1),
    ("\u2265 1s", 1.0),
    ("\u2265 5s", 5.0),
)
ATOMIC_TIMELINE_STATUS_CHOICES: tuple[str, ...] = (
    "",
    "running",
    "completed",
    "abandoned",
    "blocked",
)
ATOMIC_TIMELINE_MIN_PERIOD_WIDTH_PERCENT = 0.12


@dataclass(frozen=True)
class AtomicTimelineFilters:
    """URL filters for the atomic-service timeline."""

    page: int
    page_size: int
    runner_id: str | None
    atomic_service_run_id: str
    selected_atomic_service_run_id: str
    status: str
    min_duration_seconds: float
    page_was_requested: bool


def atomic_service_invocations_timeline_url(
    start_time: datetime, end_time: datetime
) -> str:
    """Return an invocations timeline URL focused on one service execution."""
    window = calculate_timeline_zoom_window([start_time, end_time])
    if window is None:
        window = (start_time, end_time)
    start, end = window
    params = {
        "time_range": "custom",
        "start_date": start.replace(tzinfo=None).isoformat(timespec="microseconds"),
        "end_date": end.replace(tzinfo=None).isoformat(timespec="microseconds"),
        "resolution": "100ms",
        "collapse_external": "0",
        "show_atomic_service": "1",
    }
    return "/invocations/timeline?" + urlencode(params)


def atomic_service_run_detail_url(atomic_service_run_id: str) -> str:
    """Return the Pynmon detail URL for an atomic-service run id."""
    return f"/runners/atomic-service/runs/{quote(atomic_service_run_id, safe='')}"


def atomic_service_execution_data(
    runner_id: str,
    start_time: datetime,
    end_time: datetime | None,
    duration: float | None,
    atomic_service_run_id: str,
    *,
    status: str = "running",
    reason: str = "",
    runner_alive: bool | None = None,
) -> dict:
    """Build template data for one atomic-service execution."""
    timeline_end = end_time or datetime.now(UTC)
    return {
        "runner_id": runner_id,
        "start": start_time,
        "end": end_time,
        "duration": duration or 0.0,
        "atomic_service_run_id": atomic_service_run_id,
        "status": status,
        "reason": reason,
        "runner_alive": runner_alive,
        "is_active": end_time is None,
        "timeline_url": atomic_service_invocations_timeline_url(
            start_time, timeline_end
        ),
        "detail_url": atomic_service_run_detail_url(atomic_service_run_id),
    }


def load_atomic_service_timeline_data(
    app: Pynenc,
    *,
    limit: int,
    runner_id: str | None,
    min_duration_seconds: float,
) -> list[dict]:
    """Load recorded atomic-service executions for the runners page."""
    start = datetime.min.replace(tzinfo=UTC)
    end = datetime.now(UTC)
    executions = app.orchestrator.get_atomic_service_executions_in_timerange(
        start,
        end,
        limit=limit,
        runner_id=runner_id,
        min_duration_seconds=min_duration_seconds,
    )
    active_runner_ids = {
        runner.runner_id for runner in app.orchestrator.get_active_runners()
    }
    return [
        atomic_service_execution_data(
            execution.runner_id,
            execution.start_time,
            execution.end_time,
            execution.duration_seconds,
            execution.atomic_service_run_id,
            status=execution.status.value,
            reason=execution.reason,
            runner_alive=execution.runner_id in active_runner_ids,
        )
        for execution in executions
    ]


def find_atomic_service_execution(
    app: Pynenc, atomic_service_run_id: str
) -> AtomicServiceExecution | None:
    """Find a retained atomic-service execution by service-run id."""
    limit = max(int(app.conf.atomic_service_execution_max_records), 1000)
    executions = app.orchestrator.get_atomic_service_executions_in_timerange(
        datetime.min.replace(tzinfo=UTC),
        datetime.now(UTC),
        limit=limit,
    )
    return next(
        (
            execution
            for execution in executions
            if execution.atomic_service_run_id == atomic_service_run_id
        ),
        None,
    )


def _parse_int(value: str | None, default: int) -> int:
    """Parse a positive integer query parameter."""
    try:
        parsed = int(value or str(default))
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def _atomic_timeline_page_size(raw_value: str | None) -> int:
    """Normalize timeline page size to a supported window size."""
    requested = _parse_int(raw_value, 100)
    if requested <= 100:
        return 100
    return 1000


def parse_atomic_timeline_filters(request: Request) -> AtomicTimelineFilters:
    """Read URL filters for the atomic-service timeline view."""
    page_size = _atomic_timeline_page_size(
        request.query_params.get("page_size") or request.query_params.get("limit")
    )
    page_was_requested = "page" in request.query_params
    page = _parse_int(request.query_params.get("page"), 1)

    runner_id_raw = request.query_params.get("runner_id", "").strip()
    runner_id = runner_id_raw or None

    atomic_service_run_id = request.query_params.get(
        "atomic_service_run_id", ""
    ).strip()
    selected_atomic_service_run_id = request.query_params.get(
        "selected_atomic_service_run_id", ""
    ).strip()

    status = request.query_params.get("status", "").strip().lower()
    if status not in ATOMIC_TIMELINE_STATUS_CHOICES:
        status = ""

    try:
        min_duration_seconds = float(
            request.query_params.get("min_duration_seconds", "0")
        )
    except ValueError:
        min_duration_seconds = 0.0
    if min_duration_seconds < 0.0:
        min_duration_seconds = 0.0

    return AtomicTimelineFilters(
        page=page,
        page_size=page_size,
        runner_id=runner_id,
        atomic_service_run_id=atomic_service_run_id,
        selected_atomic_service_run_id=selected_atomic_service_run_id,
        status=status,
        min_duration_seconds=min_duration_seconds,
        page_was_requested=page_was_requested,
    )


def load_atomic_service_timeline_executions(
    app: Pynenc,
    filters: AtomicTimelineFilters,
) -> list[AtomicServiceExecution]:
    """Load retained atomic-service executions before view-level filters."""
    start = datetime.min.replace(tzinfo=UTC)
    end = datetime.now(UTC)
    max_records = max(int(app.conf.atomic_service_execution_max_records), 1000)
    return app.orchestrator.get_atomic_service_executions_in_timerange(
        start,
        end,
        limit=max_records,
        runner_id=filters.runner_id,
        min_duration_seconds=filters.min_duration_seconds,
    )


def filter_atomic_service_timeline_executions(
    executions: list[AtomicServiceExecution],
    filters: AtomicTimelineFilters,
) -> list[AtomicServiceExecution]:
    """Apply filters not supported directly by orchestrator backends."""
    filtered = executions
    if filters.atomic_service_run_id:
        filtered = [
            execution
            for execution in filtered
            if filters.atomic_service_run_id in execution.atomic_service_run_id
        ]
    if filters.status:
        filtered = [
            execution
            for execution in filtered
            if execution.status.value == filters.status
        ]
    filtered.sort(key=lambda execution: execution.start_time, reverse=True)
    return filtered


def selected_atomic_service_run_id(
    executions: list[AtomicServiceExecution],
    filters: AtomicTimelineFilters,
) -> str:
    """Return the exact run id that should be highlighted, if any."""
    if filters.selected_atomic_service_run_id:
        return filters.selected_atomic_service_run_id
    if filters.atomic_service_run_id:
        for execution in executions:
            if execution.atomic_service_run_id == filters.atomic_service_run_id:
                return filters.atomic_service_run_id
    return ""


def atomic_timeline_query(
    filters: AtomicTimelineFilters,
    **updates: str | int | float | None,
) -> str:
    """Build a timeline URL query string from filters plus overrides."""
    params: dict[str, str | int | float] = {
        "page_size": filters.page_size,
        "page": filters.page,
    }
    if filters.runner_id:
        params["runner_id"] = filters.runner_id
    if filters.atomic_service_run_id:
        params["atomic_service_run_id"] = filters.atomic_service_run_id
    if filters.selected_atomic_service_run_id:
        params["selected_atomic_service_run_id"] = (
            filters.selected_atomic_service_run_id
        )
    if filters.status:
        params["status"] = filters.status
    if filters.min_duration_seconds > 0.0:
        params["min_duration_seconds"] = filters.min_duration_seconds
    for key, value in updates.items():
        if value is None or value == "":
            params.pop(key, None)
        else:
            params[key] = value
    return "/runners/atomic-service/timeline?" + urlencode(params)


def paginate_atomic_service_timeline(
    executions: list[AtomicServiceExecution],
    filters: AtomicTimelineFilters,
    selected_run_id: str,
) -> tuple[list[AtomicServiceExecution], dict]:
    """Select the visible execution page and build pagination metadata."""
    total_items = len(executions)
    total_pages = max(ceil(total_items / filters.page_size), 1)
    page = min(filters.page, total_pages)
    selected_index = next(
        (
            index
            for index, execution in enumerate(executions)
            if execution.atomic_service_run_id == selected_run_id
        ),
        None,
    )
    if selected_index is not None and not filters.page_was_requested:
        page = selected_index // filters.page_size + 1

    start_index = (page - 1) * filters.page_size
    end_index = min(start_index + filters.page_size, total_items)
    page_items = executions[start_index:end_index]
    current_filters = AtomicTimelineFilters(
        page=page,
        page_size=filters.page_size,
        runner_id=filters.runner_id,
        atomic_service_run_id=filters.atomic_service_run_id,
        selected_atomic_service_run_id=filters.selected_atomic_service_run_id,
        status=filters.status,
        min_duration_seconds=filters.min_duration_seconds,
        page_was_requested=filters.page_was_requested,
    )
    pagination = {
        "page": page,
        "page_size": filters.page_size,
        "total_items": total_items,
        "total_pages": total_pages,
        "start_item": start_index + 1 if total_items else 0,
        "end_item": end_index,
        "has_previous": page > 1,
        "has_next": page < total_pages,
        "previous_url": atomic_timeline_query(current_filters, page=page - 1)
        if page > 1
        else "",
        "next_url": atomic_timeline_query(current_filters, page=page + 1)
        if page < total_pages
        else "",
        "first_url": atomic_timeline_query(current_filters, page=1),
        "last_url": atomic_timeline_query(current_filters, page=total_pages),
    }
    return page_items, pagination


def add_atomic_service_period_offsets(timeline_data: list[dict]) -> None:
    """Annotate rows with visible period offsets for the table background."""
    if not timeline_data:
        return
    earliest_start = min(row["start"] for row in timeline_data)
    latest_end = max((row["end"] or row["start"]) for row in timeline_data)
    total_time_span = (latest_end - earliest_start).total_seconds()
    for row in timeline_data:
        if total_time_span <= 0:
            period_start = 0.0
            period_end = 100.0
        else:
            period_start = (
                (row["start"] - earliest_start).total_seconds() / total_time_span * 100
            )
            period_width = row["duration"] / total_time_span * 100
            visible_width = max(period_width, ATOMIC_TIMELINE_MIN_PERIOD_WIDTH_PERCENT)
            period_end = period_start + visible_width
            if period_end > 100.0:
                period_end = 100.0
                period_start = max(0.0, 100.0 - visible_width)
        row["period_start_percent"] = round(period_start, 2)
        row["period_end_percent"] = round(period_end, 2)
