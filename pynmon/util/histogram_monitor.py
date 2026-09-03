"""Backend adapters for rendering Pynmon occupancy histograms."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from collections.abc import Iterable

from pynenc.identifiers.invocation_id import InvocationId
from pynmon.util.histogram import (
    DEFAULT_CATEGORIES,
    HistogramCategory,
    HistogramData,
    HistogramEntry,
    build_histogram,
)
from pynmon.util.histogram_svg import render_histogram_svg

if TYPE_CHECKING:
    from pynenc.app import Pynenc


def history_entries_for_invocations(
    app: Pynenc, invocation_ids: Iterable[str]
) -> list[HistogramEntry]:
    """Load complete histories and task identities for a bounded invocation set."""
    entries: list[HistogramEntry] = []
    for invocation_id in dict.fromkeys(str(value) for value in invocation_ids):
        typed_id = InvocationId(invocation_id)
        try:
            invocation = app.state_backend.get_invocation(typed_id)
            history = app.state_backend.get_history(typed_id)
        except Exception:
            continue
        task_id = str(invocation.task.task_id)
        entries.extend(
            HistogramEntry(
                invocation_id=invocation_id,
                task_id=task_id,
                status=item.status_record.status,
                timestamp=item.timestamp,
            )
            for item in history
        )
    return entries


def histogram_window_for_entries(
    entries: list[HistogramEntry], *, now: datetime | None = None
) -> tuple[datetime, datetime] | None:
    """Return a workflow-sized window, extending active histories to now."""
    if not entries:
        return None
    now = now or datetime.now(UTC)
    start = min(item.timestamp for item in entries)
    latest_by_invocation: dict[str, HistogramEntry] = {}
    for item in sorted(entries, key=lambda value: value.timestamp):
        latest_by_invocation[item.invocation_id] = item
    has_active = any(
        not item.status.is_final() for item in latest_by_invocation.values()
    )
    end = now if has_active else max(item.timestamp for item in entries)
    if end <= start:
        end = start + timedelta(seconds=1)
    return start, end


def histogram_context(
    app: Pynenc,
    invocation_ids: Iterable[str],
    start: datetime,
    end: datetime,
    selected_categories: frozenset[HistogramCategory] = DEFAULT_CATEGORIES,
    *,
    common_params: dict[str, str] | None = None,
    link_path: str = "/invocations",
    compact: bool = False,
    left_margin: int | None = None,
    right_margin: int | None = None,
) -> dict:
    """Build the template context for one histogram panel."""
    entries = history_entries_for_invocations(app, invocation_ids)
    return histogram_context_from_entries(
        entries,
        start,
        end,
        selected_categories,
        common_params=common_params,
        link_path=link_path,
        compact=compact,
        left_margin=left_margin,
        right_margin=right_margin,
    )


def histogram_context_from_entries(
    entries: list[HistogramEntry],
    start: datetime,
    end: datetime,
    selected_categories: frozenset[HistogramCategory] = DEFAULT_CATEGORIES,
    *,
    common_params: dict[str, str] | None = None,
    link_path: str = "/invocations",
    compact: bool = False,
    y_axis_max: int | None = None,
    left_margin: int | None = None,
    right_margin: int | None = None,
) -> dict:
    """Build template context from histories already loaded by a monitor view."""
    data = build_histogram(entries, start, end, selected_categories)
    return histogram_context_from_data(
        data,
        common_params=common_params,
        link_path=link_path,
        compact=compact,
        y_axis_max=y_axis_max,
        left_margin=left_margin,
        right_margin=right_margin,
    )


def histogram_context_from_data(
    data: HistogramData,
    *,
    common_params: dict[str, str] | None = None,
    link_path: str = "/invocations",
    compact: bool = False,
    y_axis_max: int | None = None,
    left_margin: int | None = None,
    right_margin: int | None = None,
) -> dict:
    """Build template context from a precomputed histogram model."""
    svg = render_histogram_svg(
        data,
        common_params=common_params,
        link_path=link_path,
        height=98 if compact else 118,
        y_axis_max=y_axis_max,
        left_margin=320 if left_margin is None else left_margin,
        right_margin=0 if right_margin is None else right_margin,
    )
    return {
        "svg": svg,
        "empty_reason": data.empty_reason or "",
        "categories": [
            {
                "value": category.value,
                "label": category.label,
                "selected": category in data.selected_categories,
            }
            for category in HistogramCategory
        ],
        "compact": compact,
    }
