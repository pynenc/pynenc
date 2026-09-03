from datetime import UTC, datetime, timedelta

from pynenc.invocation.status import InvocationStatus
from pynmon.util.histogram import (
    MAX_BAR_WIDTH_PX,
    MIN_BAR_WIDTH_PX,
    DEFAULT_CATEGORIES,
    HistogramCategory,
    HistogramEntry,
    bucket_size_for_window,
    build_histogram,
    parse_histogram_categories,
    serialize_histogram_categories,
)
from pynmon.util.histogram_svg import _task_color, render_histogram_svg

START = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)


def entry(
    invocation_id: str,
    seconds: float,
    status: InvocationStatus,
    task_id: str = "tests.task",
) -> HistogramEntry:
    return HistogramEntry(
        invocation_id,
        task_id,
        status,
        START + timedelta(seconds=seconds),
    )


def test_long_running_invocation_occupies_every_overlapping_bucket() -> None:
    data = build_histogram(
        [
            entry("inv-1", 0, InvocationStatus.RUNNING),
            entry("inv-1", 12, InvocationStatus.SUCCESS),
        ],
        START,
        START + timedelta(seconds=15),
        frozenset({HistogramCategory.RUNNING}),
        bucket_size=timedelta(seconds=5),
    )

    assert [bucket.total_count for bucket in data.buckets] == [1, 1, 1]
    assert all(bucket.invocation_ids == ("inv-1",) for bucket in data.buckets)


def test_categories_are_counted_independently_and_once_per_bucket() -> None:
    data = build_histogram(
        [
            entry("inv-1", 0, InvocationStatus.REGISTERED),
            entry("inv-1", 2, InvocationStatus.REROUTED),
            entry("inv-1", 4, InvocationStatus.PENDING),
            entry("inv-1", 7, InvocationStatus.RUNNING),
            entry("inv-1", 10, InvocationStatus.SUCCESS),
        ],
        START,
        START + timedelta(seconds=10),
        frozenset(HistogramCategory),
        bucket_size=timedelta(seconds=5),
    )

    first, second = data.buckets
    assert first.counts_by_category == {
        HistogramCategory.REGISTERED: 1,
        HistogramCategory.PENDING: 1,
        HistogramCategory.RUNNING: 0,
    }
    assert second.counts_by_category == {
        HistogramCategory.REGISTERED: 0,
        HistogramCategory.PENDING: 1,
        HistogramCategory.RUNNING: 1,
    }


def test_final_status_does_not_extend_to_window_end() -> None:
    data = build_histogram(
        [entry("inv-1", 1, InvocationStatus.SUCCESS)],
        START,
        START + timedelta(seconds=10),
        DEFAULT_CATEGORIES,
        bucket_size=timedelta(seconds=5),
    )

    assert [bucket.total_count for bucket in data.buckets] == [0, 0]


def test_interval_is_clipped_and_boundary_transition_is_not_double_counted() -> None:
    data = build_histogram(
        [
            entry("inv-1", -5, InvocationStatus.PENDING),
            entry("inv-1", 5, InvocationStatus.RUNNING),
        ],
        START,
        START + timedelta(seconds=10),
        DEFAULT_CATEGORIES,
        bucket_size=timedelta(seconds=5),
    )

    assert data.buckets[0].counts_by_category[HistogramCategory.PENDING] == 1
    assert data.buckets[0].counts_by_category[HistogramCategory.RUNNING] == 0
    assert data.buckets[1].counts_by_category[HistogramCategory.PENDING] == 0
    assert data.buckets[1].counts_by_category[HistogramCategory.RUNNING] == 1


def test_status_selection_filters_ids_and_task_counts() -> None:
    data = build_histogram(
        [
            entry("inv-1", 0, InvocationStatus.PENDING, "tests.alpha"),
            entry("inv-2", 0, InvocationStatus.RUNNING, "tests.beta"),
        ],
        START,
        START + timedelta(seconds=5),
        frozenset({HistogramCategory.RUNNING}),
        bucket_size=timedelta(seconds=5),
    )

    bucket = data.buckets[0]
    assert bucket.invocation_ids == ("inv-2",)
    assert bucket.counts_by_task == {"tests.beta": 1}


def test_empty_histories_and_empty_selection_have_explicit_reasons() -> None:
    no_data = build_histogram(
        [], START, START + timedelta(seconds=5), DEFAULT_CATEGORIES
    )
    no_selection = build_histogram(
        [entry("inv-1", 0, InvocationStatus.RUNNING)],
        START,
        START + timedelta(seconds=5),
        frozenset(),
    )

    assert no_data.empty_reason == "No invocation history in this time range."
    assert no_selection.empty_reason == "Select at least one status."


def test_resolution_table_and_bucket_cap() -> None:
    assert bucket_size_for_window(timedelta(seconds=5)) <= timedelta(seconds=0.25)
    assert bucket_size_for_window(timedelta(seconds=10)) <= timedelta(seconds=0.5)
    assert bucket_size_for_window(timedelta(seconds=60)) <= timedelta(seconds=1)
    assert bucket_size_for_window(timedelta(minutes=15)) == timedelta(seconds=5)
    assert bucket_size_for_window(timedelta(hours=1)) == timedelta(seconds=15)
    assert bucket_size_for_window(timedelta(days=30)) >= timedelta(hours=3)
    zoomed = bucket_size_for_window(timedelta(milliseconds=474))
    assert zoomed < timedelta(milliseconds=20)
    assert (timedelta(milliseconds=474) / zoomed) >= 26


def test_category_parser_distinguishes_missing_from_empty() -> None:
    assert parse_histogram_categories(None) == DEFAULT_CATEGORIES
    assert DEFAULT_CATEGORIES == frozenset(
        {HistogramCategory.PENDING, HistogramCategory.RUNNING}
    )
    assert parse_histogram_categories("") == frozenset()
    assert parse_histogram_categories("running, pending,unknown") == frozenset(
        {HistogramCategory.RUNNING, HistogramCategory.PENDING}
    )


def test_category_serializer_is_stable() -> None:
    assert (
        serialize_histogram_categories(
            frozenset({HistogramCategory.RUNNING, HistogramCategory.REGISTERED})
        )
        == "registered,running"
    )


def test_other_uses_neutral_colour() -> None:
    assert _task_color("__other__") == "#cccccc"


def test_svg_stacks_tasks_and_keeps_task_colors_stable() -> None:
    one_task = build_histogram(
        [entry("inv-1", 0, InvocationStatus.RUNNING, "alpha.task")],
        START,
        START + timedelta(seconds=5),
        frozenset({HistogramCategory.RUNNING}),
        bucket_size=timedelta(seconds=1),
    )
    two_tasks = build_histogram(
        [
            entry("inv-1", 0, InvocationStatus.RUNNING, "alpha.task"),
            entry("inv-2", 0, InvocationStatus.RUNNING, "beta.task"),
        ],
        START,
        START + timedelta(seconds=5),
        frozenset({HistogramCategory.RUNNING}),
        bucket_size=timedelta(seconds=1),
    )

    one_svg = render_histogram_svg(one_task)
    two_svg = render_histogram_svg(two_tasks)
    assert 'data-task="alpha.task"' in one_svg
    assert 'data-task="beta.task"' in two_svg
    alpha_color = (
        one_svg.split('data-task="alpha.task"')[0]
        .rsplit('fill="', 1)[1]
        .split('"', 1)[0]
    )
    assert f'fill="{alpha_color}" data-task="alpha.task"' in two_svg


def test_zoomed_svg_caps_bar_width_and_skips_empty_buckets() -> None:
    data = build_histogram(
        [entry("inv-1", 0, InvocationStatus.RUNNING, "alpha.task")],
        START,
        START + timedelta(milliseconds=474),
        frozenset({HistogramCategory.RUNNING}),
    )
    svg = render_histogram_svg(data)
    widths = [
        float(fragment.split('width="', 1)[1].split('"', 1)[0])
        for fragment in svg.split("<rect ")[1:]
        if 'data-task="' in fragment
    ]
    assert widths
    assert max(widths) <= MAX_BAR_WIDTH_PX
    assert min(widths) >= MIN_BAR_WIDTH_PX

    sparse_data = build_histogram(
        [
            entry("inv-1", 0.2, InvocationStatus.RUNNING, "alpha.task"),
            entry("inv-1", 0.25, InvocationStatus.SUCCESS, "alpha.task"),
        ],
        START,
        START + timedelta(milliseconds=474),
        frozenset({HistogramCategory.RUNNING}),
    )
    sparse_svg = render_histogram_svg(sparse_data)
    sparse_widths = [
        fragment
        for fragment in sparse_svg.split("<rect ")[1:]
        if 'data-task="' in fragment
    ]
    assert sparse_widths
    assert len(sparse_widths) < len(sparse_data.buckets)


def test_svg_renders_left_scale_and_horizontal_grid() -> None:
    data = build_histogram(
        [entry("inv-1", 0, InvocationStatus.RUNNING, "alpha.task")],
        START,
        START + timedelta(seconds=5),
        DEFAULT_CATEGORIES,
        bucket_size=timedelta(seconds=1),
    )

    svg = render_histogram_svg(data)

    assert 'text-anchor="end"' in svg
    assert 'fill="#6c757d">0</text>' in svg
    assert 'stroke="#e9ecef"' in svg


def test_svg_links_use_the_matching_status_scope_without_duplicate_ids() -> None:
    data = build_histogram(
        [entry("inv-1", 0, InvocationStatus.RUNNING, "alpha.task")],
        START,
        START + timedelta(seconds=5),
        frozenset({HistogramCategory.RUNNING}),
        bucket_size=timedelta(seconds=1),
    )

    list_svg = render_histogram_svg(data, link_path="/invocations")
    timeline_svg = render_histogram_svg(
        data,
        link_path="/invocations/timeline",
        common_params={"inv_ids": "scope-1"},
    )

    assert "status_mode=history" in list_svg
    assert "status=running%2Crunning_recovery" in list_svg
    assert "histogram_status=running" in timeline_svg
    hrefs = [fragment.split('"', 1)[0] for fragment in timeline_svg.split('href="')[1:]]
    assert hrefs
    assert all(href.count("inv_ids=") == 1 for href in hrefs)
