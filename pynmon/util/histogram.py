"""Pure occupancy histogram model shared by Pynmon monitoring views."""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum

from pynenc.invocation.status import InvocationStatus

MAX_BUCKETS = 240
HISTOGRAM_PLOT_WIDTH_PX = 1680
MIN_BAR_WIDTH_PX = 3
# The timeline renders status points with a 5px radius; occupancy bars target
# that same 10px visual diameter instead of becoming wider than a timeline mark.
TIMELINE_POINT_DIAMETER_PX = 10
MAX_BAR_WIDTH_PX = TIMELINE_POINT_DIAMETER_PX


class HistogramCategory(StrEnum):
    """User-facing groups of invocation lifecycle statuses."""

    REGISTERED = "registered"
    PENDING = "pending"
    RUNNING = "running"

    @property
    def label(self) -> str:
        return self.value.title()


DEFAULT_CATEGORIES = frozenset({HistogramCategory.PENDING, HistogramCategory.RUNNING})

CATEGORY_STATUSES: dict[HistogramCategory, frozenset[InvocationStatus]] = {
    HistogramCategory.REGISTERED: frozenset(
        {
            InvocationStatus.REGISTERED,
            InvocationStatus.REROUTED,
            InvocationStatus.CONCURRENCY_CONTROLLED,
        }
    ),
    HistogramCategory.PENDING: frozenset(
        {
            InvocationStatus.PENDING,
            InvocationStatus.PENDING_RECOVERY,
            InvocationStatus.RETRY,
        }
    ),
    HistogramCategory.RUNNING: frozenset(
        {InvocationStatus.RUNNING, InvocationStatus.RUNNING_RECOVERY}
    ),
}

_STATUS_CATEGORY = {
    status: category
    for category, statuses in CATEGORY_STATUSES.items()
    for status in statuses
}


@dataclass(frozen=True)
class HistogramEntry:
    invocation_id: str
    task_id: str
    status: InvocationStatus
    timestamp: datetime


@dataclass(frozen=True)
class HistogramBucket:
    start: datetime
    end: datetime
    counts_by_category: dict[HistogramCategory, int]
    counts_by_task: dict[str, int]
    invocation_ids_by_category: dict[HistogramCategory, tuple[str, ...]]
    invocation_ids: tuple[str, ...]

    @property
    def total_count(self) -> int:
        """Return unique invocations, avoiding double-counting transitions."""
        return len(self.invocation_ids)


@dataclass(frozen=True)
class HistogramData:
    start: datetime
    end: datetime
    bucket_size: timedelta
    buckets: tuple[HistogramBucket, ...]
    selected_categories: frozenset[HistogramCategory]
    max_count: int
    empty_reason: str | None = None


def parse_histogram_categories(value: str | None) -> frozenset[HistogramCategory]:
    """Parse a comma-separated selector while preserving an explicit empty set."""
    if value is None:
        return DEFAULT_CATEGORIES
    selected: set[HistogramCategory] = set()
    for item in value.split(","):
        try:
            selected.add(HistogramCategory(item.strip().lower()))
        except ValueError:
            continue
    return frozenset(selected)


def serialize_histogram_categories(
    categories: frozenset[HistogramCategory],
) -> str:
    """Return the stable query-string representation of a category selection."""
    return ",".join(
        category.value for category in HistogramCategory if category in categories
    )


def bucket_size_for_window(duration: timedelta) -> timedelta:
    """Return a resolution that respects both bucket and pixel-size limits."""
    seconds = max(duration.total_seconds(), 0.001)
    if seconds <= 5:
        candidate = 0.25
    elif seconds <= 30:
        candidate = 0.5
    elif seconds <= 60:
        candidate = 1
    elif seconds <= 15 * 60:
        candidate = 5
    elif seconds <= 60 * 60:
        candidate = 15
    elif seconds <= 3 * 60 * 60:
        candidate = 60
    elif seconds <= 12 * 60 * 60:
        candidate = 5 * 60
    elif seconds <= 3 * 24 * 60 * 60:
        candidate = 30 * 60
    else:
        candidate = 60 * 60
    minimum_for_bucket_cap = seconds / MAX_BUCKETS
    minimum_buckets_for_bar_width = math.ceil(
        HISTOGRAM_PLOT_WIDTH_PX / MAX_BAR_WIDTH_PX
    )
    maximum_for_bar_width = seconds / minimum_buckets_for_bar_width
    return timedelta(
        seconds=max(
            minimum_for_bucket_cap,
            min(candidate, maximum_for_bar_width),
        )
    )


def _empty_data(
    start: datetime,
    end: datetime,
    categories: frozenset[HistogramCategory],
    reason: str,
) -> HistogramData:
    return HistogramData(
        start=start,
        end=end,
        bucket_size=bucket_size_for_window(end - start),
        buckets=(),
        selected_categories=categories,
        max_count=0,
        empty_reason=reason,
    )


def build_histogram(
    entries: list[HistogramEntry],
    start: datetime,
    end: datetime,
    selected_categories: frozenset[HistogramCategory] = DEFAULT_CATEGORIES,
    *,
    bucket_size: timedelta | None = None,
) -> HistogramData:
    """Build half-open occupancy buckets from complete invocation histories."""
    if end <= start:
        return _empty_data(start, end, selected_categories, "Invalid time range.")
    if not selected_categories:
        return _empty_data(
            start, end, selected_categories, "Select at least one status."
        )
    if not entries:
        return _empty_data(
            start,
            end,
            selected_categories,
            "No invocation history in this time range.",
        )

    size = bucket_size or bucket_size_for_window(end - start)
    size_seconds = size.total_seconds()
    if size_seconds <= 0:
        raise ValueError("bucket_size must be positive")
    bucket_count = math.ceil((end - start).total_seconds() / size_seconds)
    bucket_ranges = [
        (
            start + (size * index),
            min(start + (size * (index + 1)), end),
        )
        for index in range(bucket_count)
    ]
    category_ids: list[defaultdict[HistogramCategory, set[str]]] = [
        defaultdict(set) for _ in bucket_ranges
    ]
    task_ids: list[defaultdict[str, set[str]]] = [
        defaultdict(set) for _ in bucket_ranges
    ]

    histories: dict[str, list[HistogramEntry]] = defaultdict(list)
    for item in entries:
        histories[item.invocation_id].append(item)

    for invocation_id, history in histories.items():
        history.sort(key=lambda item: item.timestamp)
        task_id = next((item.task_id for item in history if item.task_id), "unknown")
        for index, item in enumerate(history):
            category = _STATUS_CATEGORY.get(item.status)
            if category not in selected_categories or item.status.is_final():
                continue
            interval_start = max(item.timestamp, start)
            next_timestamp = (
                history[index + 1].timestamp if index + 1 < len(history) else end
            )
            interval_end = min(next_timestamp, end)
            if interval_end <= interval_start:
                continue

            first_bucket = max(
                0,
                int((interval_start - start).total_seconds() // size_seconds),
            )
            last_bucket = min(
                bucket_count - 1,
                int(
                    math.ceil((interval_end - start).total_seconds() / size_seconds) - 1
                ),
            )
            for bucket_index in range(first_bucket, last_bucket + 1):
                bucket_start, bucket_end = bucket_ranges[bucket_index]
                if interval_start < bucket_end and interval_end > bucket_start:
                    category_ids[bucket_index][category].add(invocation_id)
                    task_ids[bucket_index][task_id].add(invocation_id)

    buckets: list[HistogramBucket] = []
    for index, (bucket_start, bucket_end) in enumerate(bucket_ranges):
        by_category = {
            category: len(category_ids[index].get(category, set()))
            for category in HistogramCategory
            if category in selected_categories
        }
        ids_by_category = {
            category: tuple(sorted(category_ids[index].get(category, set())))
            for category in HistogramCategory
            if category in selected_categories
        }
        all_ids = tuple(
            sorted(
                {
                    invocation_id
                    for ids in category_ids[index].values()
                    for invocation_id in ids
                }
            )
        )
        buckets.append(
            HistogramBucket(
                start=bucket_start,
                end=bucket_end,
                counts_by_category=by_category,
                counts_by_task={
                    task: len(ids) for task, ids in sorted(task_ids[index].items())
                },
                invocation_ids_by_category=ids_by_category,
                invocation_ids=all_ids,
            )
        )

    max_count = max((bucket.total_count for bucket in buckets), default=0)
    empty_reason = None
    if max_count == 0:
        empty_reason = "No selected statuses occupied this time range."
    return HistogramData(
        start=start,
        end=end,
        bucket_size=size,
        buckets=tuple(buckets),
        selected_categories=selected_categories,
        max_count=max_count,
        empty_reason=empty_reason,
    )


def exact_status_values(
    categories: frozenset[HistogramCategory],
) -> tuple[str, ...]:
    """Return exact lowercase status names accepted by invocation-list filters."""
    return tuple(
        status.value
        for category in HistogramCategory
        if category in categories
        for status in InvocationStatus
        if status in CATEGORY_STATUSES[category]
    )
