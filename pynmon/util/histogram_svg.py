"""Small dependency-free SVG renderer for occupancy histograms."""

from __future__ import annotations

import math
from datetime import datetime
from html import escape
from urllib.parse import urlencode

from pynmon.util.histogram import (
    MAX_BAR_WIDTH_PX,
    MIN_BAR_WIDTH_PX,
    HistogramCategory,
    HistogramData,
    exact_status_values,
)

_TASK_PALETTE = (
    "#4e79a7",
    "#f28e2b",
    "#e15759",
    "#76b7b2",
    "#59a14f",
    "#edc948",
    "#b07aa1",
    "#ff9da7",
    "#9c755f",
    "#bab0ac",
    "#86bcb6",
    "#8cd17d",
    "#b6992d",
    "#499894",
    "#d37295",
)
_MAX_TASK_LEGEND = 12


def _task_color(task_id: str) -> str:
    """Return a stable cross-process color using FNV-1a and the shared palette."""
    if task_id == "__other__":
        return "#cccccc"
    value = 2166136261
    for byte in task_id.encode("utf-8"):
        value = ((value ^ byte) * 16777619) & 0xFFFFFFFF
    return _TASK_PALETTE[value % len(_TASK_PALETTE)]


def _task_series(data: HistogramData) -> tuple[list[str], bool]:
    totals: dict[str, int] = {}
    for bucket in data.buckets:
        for task_id, count in bucket.counts_by_task.items():
            totals[task_id] = totals.get(task_id, 0) + count
    ordered = sorted(totals, key=lambda task_id: (-totals[task_id], task_id))
    return ordered[:_MAX_TASK_LEGEND], len(ordered) > _MAX_TASK_LEGEND


def _bucket_task_counts(
    counts_by_task: dict[str, int], visible_tasks: set[str]
) -> list[tuple[str, int]]:
    counts = [
        (task_id, count)
        for task_id, count in counts_by_task.items()
        if task_id in visible_tasks and count
    ]
    other = sum(
        count
        for task_id, count in counts_by_task.items()
        if task_id not in visible_tasks
    )
    if other:
        counts.append(("__other__", other))
    return counts


def _iso(value: datetime) -> str:
    return value.isoformat()


def render_histogram_svg(
    data: HistogramData,
    *,
    common_params: dict[str, str] | None = None,
    link_path: str = "/invocations",
    width: int = 2000,
    left_margin: int = 320,
    right_margin: int = 0,
    height: int = 118,
    y_axis_max: int | None = None,
) -> str:
    """Render histogram data with timeline-aligned coordinates and links."""
    if not data.buckets or data.max_count == 0:
        return ""
    plot_top = 8.0
    plot_bottom = 82.0
    plot_height = plot_bottom - plot_top
    plot_width = width - left_margin - right_margin
    duration = (data.end - data.start).total_seconds()
    categories = [
        category
        for category in HistogramCategory
        if category in data.selected_categories
    ]
    visible_tasks, has_other_tasks = _task_series(data)
    visible_task_set = set(visible_tasks)
    legend_items = [
        (task_id, task_id, _task_color(task_id)) for task_id in visible_tasks
    ]
    if has_other_tasks:
        legend_items.append(("__other__", "Other", _task_color("__other__")))
    legend_rows: list[list[tuple[str, str, str]]] = [[]]
    legend_width = 0
    for item in legend_items:
        item_width = max(110, len(item[1]) * 7 + 34)
        if legend_width and legend_width + item_width > plot_width:
            legend_rows.append([])
            legend_width = 0
        legend_rows[-1].append(item)
        legend_width += item_width
    legend_start_y = 96
    svg_height = max(height, legend_start_y + len(legend_rows) * 16 + 4)
    scale_max = max(data.max_count, y_axis_max or 0)
    status_values = ",".join(exact_status_values(data.selected_categories))
    selected_names = ",".join(category.value for category in categories)

    out = [
        (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="100%" '
            f'viewBox="0 0 {width} {svg_height}" preserveAspectRatio="xMinYMin meet" '
            f'data-histogram-start="{escape(_iso(data.start))}" '
            f'data-histogram-end="{escape(_iso(data.end))}" '
            f'data-histogram-left="{left_margin}" '
            f'data-histogram-right="{width - right_margin}" '
            f'data-statuses="{escape(selected_names)}" role="img" '
            f'aria-label="Task occupancy histogram">'
        ),
        f'<rect width="{width}" height="{svg_height}" fill="#fff"/>',
        '<text x="10" y="22" font-size="12" font-weight="600" fill="#495057">Task occupancy</text>',
        f'<text x="10" y="39" font-size="10" fill="#6c757d">max {scale_max}</text>',
    ]
    tick_step = max(1, math.ceil(scale_max / 4))
    tick_values = list(range(0, scale_max + 1, tick_step))
    if tick_values[-1] != scale_max:
        tick_values.append(scale_max)
    for tick in tick_values:
        y = plot_bottom - (tick / scale_max) * plot_height
        out.append(
            f'<line x1="{left_margin}" y1="{y:.2f}" x2="{width - right_margin}" '
            f'y2="{y:.2f}" stroke="#e9ecef" stroke-width="1"/>'
            f'<text x="{left_margin - 8}" y="{y + 3:.2f}" text-anchor="end" '
            f'font-size="10" fill="#6c757d">{tick}</text>'
        )
    params_base = dict(common_params or {})
    params_base.update({"time_range": "custom", "status": status_values})
    has_common_invocation_scope = "inv_ids" in params_base

    for bucket in data.buckets:
        start_ratio = (bucket.start - data.start).total_seconds() / duration
        end_ratio = (bucket.end - data.start).total_seconds() / duration
        bucket_center = left_margin + ((start_ratio + end_ratio) / 2) * plot_width
        bucket_width = min(
            MAX_BAR_WIDTH_PX,
            max((end_ratio - start_ratio) * plot_width - 0.7, MIN_BAR_WIDTH_PX),
        )
        x = max(
            left_margin,
            min(width - right_margin - bucket_width, bucket_center - bucket_width / 2),
        )
        y = plot_bottom
        if not bucket.total_count:
            continue
        top_tasks = sorted(
            bucket.counts_by_task.items(), key=lambda item: (-item[1], item[0])
        )[:7]
        counts_text = ", ".join(
            f"{category.label}: {bucket.counts_by_category.get(category, 0)}"
            for category in categories
        )
        tasks_text = ", ".join(f"{task}: {count}" for task, count in top_tasks)
        tooltip = f"{_iso(bucket.start)} to {_iso(bucket.end)} | {counts_text}" + (
            f" | Tasks: {tasks_text}" if tasks_text else ""
        )
        params = dict(params_base)
        params.update({"start_date": _iso(bucket.start), "end_date": _iso(bucket.end)})
        if link_path == "/invocations":
            params["status_mode"] = "history"
        else:
            params.pop("status", None)
            params["histogram_status"] = selected_names
        if (
            not has_common_invocation_scope
            and bucket.invocation_ids
            and len(bucket.invocation_ids) <= 50
        ):
            params["inv_ids"] = ",".join(bucket.invocation_ids)
        href = f"{link_path}?{urlencode(params)}"
        out.append(
            f'<a href="{escape(href, quote=True)}" class="histogram-bucket-link">'
            f'<g class="histogram-bucket" tabindex="0" '
            f'data-bucket-start="{escape(_iso(bucket.start))}" '
            f'data-bucket-end="{escape(_iso(bucket.end))}" '
            f'data-invocation-ids="{escape(" ".join(bucket.invocation_ids))}" '
            f'data-statuses="{escape(selected_names)}" '
            f'data-tooltip="{escape(tooltip, quote=True)}">'
        )
        for task_id, count in _bucket_task_counts(
            bucket.counts_by_task, visible_task_set
        ):
            if not count:
                continue
            segment_height = (count / scale_max) * plot_height
            y -= segment_height
            label = "Other" if task_id == "__other__" else task_id
            out.append(
                f'<rect x="{x:.2f}" y="{y:.2f}" width="{bucket_width:.2f}" '
                f'height="{segment_height:.2f}" fill="{_task_color(task_id)}" '
                f'data-task="{escape(label, quote=True)}"/>'
            )
        out.append(f"<title>{escape(tooltip)}</title></g></a>")

    for row_index, row in enumerate(legend_rows):
        legend_x = left_margin
        legend_y = legend_start_y + row_index * 16
        for _task_id, label, color in row:
            out.append(
                f'<rect x="{legend_x}" y="{legend_y - 9}" width="10" height="10" rx="1" fill="{color}"/>'
                f'<text x="{legend_x + 15}" y="{legend_y}" font-size="10" fill="#495057">{escape(label)}</text>'
            )
            legend_x += max(110, len(label) * 7 + 34)
    out.append("</svg>")
    return "".join(out)
