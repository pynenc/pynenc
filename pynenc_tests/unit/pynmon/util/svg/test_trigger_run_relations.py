"""Unit tests for Phase 5 trigger-run participant relation rendering.

Covers ``_render_trigger_run_relations`` inside the timeline SVG output:
status/result/exception/cron participants render as colour-coded lines
from the source invocation's nearest status point to the triggered
invocation's first point, all tagged with ``data-trigger-run-id`` and
``data-context-type`` for the cross-highlight overlay.
"""

from datetime import UTC, datetime, timedelta

from pynenc.trigger.monitoring import TriggerRunParticipant, TriggerRunRecord
from pynmon.util.svg.builder import RunnerInfo
from pynmon.util.svg.atomic_service import (
    AtomicServiceWindow,
    assign_atomic_service_sub_lanes,
    render_atomic_service_windows,
)
from pynmon.util.svg.event_markers import (
    EventMarker,
    cron_event_markers_from_trigger_runs,
    render_event_markers,
    render_timeline_relations,
)
from pynmon.util.svg.models import (
    TimelineBounds,
    TimelineConfig,
    TimelineData,
)
from pynmon.util.svg.render_axis import render_legend
from pynmon.util.svg.renderer import SVGStyle
from pynmon.util.svg.status_elements import StatusLine, StatusPoint, StatusSegment


def _empty_timeline() -> TimelineData:
    start = datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC)
    end = datetime(2025, 1, 1, 1, 0, 0, tzinfo=UTC)
    config = TimelineConfig()
    bounds = TimelineBounds(start_time=start, end_time=end, config=config)
    return TimelineData(bounds=bounds, config=config)


def _seed_two_lanes(td: TimelineData) -> None:
    """Add source-invocation and triggered-invocation status points."""
    src_lane = td.get_or_create_lane(
        "runner-A", RunnerInfo("Runner", "runner-A", "h1", 1001), "#aaa"
    )
    tgt_lane = td.get_or_create_lane(
        "runner-B", RunnerInfo("Runner", "runner-B", "h2", 1002), "#bbb"
    )
    src_lane.add_point(
        StatusPoint(
            invocation_id="src-inv",
            timestamp=td.bounds.start_time + timedelta(minutes=10),
            status="SUCCESS",
            color="#28a745",
        )
    )
    tgt_lane.add_point(
        StatusPoint(
            invocation_id="trg-inv",
            timestamp=td.bounds.start_time + timedelta(minutes=12),
            status="REGISTERED",
            color="#0d6efd",
        )
    )


def _make_run(
    *,
    context_type: str,
    source_invocation_id: str | None,
    ctx_offset_min: int = 11,
) -> TriggerRunRecord:
    ctx_ts = datetime(2025, 1, 1, 0, ctx_offset_min, 0, tzinfo=UTC)
    return TriggerRunRecord(
        trigger_run_id="run-1",
        trigger_id="trig-1",
        task_id_key="pkg.task",
        logic_value="and",
        valid_condition_ids=["vc-1"],
        condition_ids=["c-1"],
        triggered_invocation_id="trg-inv",
        participants=[
            TriggerRunParticipant(
                context_type=context_type,
                condition_id="c-1",
                valid_condition_id="vc-1",
                source_invocation_id=source_invocation_id,
                context_timestamp=ctx_ts,
                context_summary="status:SUCCESS",
            )
        ],
        claimed_at=ctx_ts,
        executed_at=ctx_ts,
    )


def test_status_trigger_relation_line_emitted() -> None:
    td = _empty_timeline()
    _seed_two_lanes(td)
    td.trigger_runs = [
        _make_run(context_type="StatusContext", source_invocation_id="src-inv")
    ]

    svg = render_timeline_relations(td, style=None)

    assert "status-trigger-relation-line" in svg
    assert 'data-trigger-run-id="run-1"' in svg
    assert 'data-source-invocation-id="src-inv"' in svg
    assert 'data-child-invocation-id="trg-inv"' in svg
    assert 'data-context-type="StatusContext"' in svg


def test_result_and_exception_lines_use_distinct_kinds() -> None:
    td = _empty_timeline()
    _seed_two_lanes(td)
    td.trigger_runs = [
        TriggerRunRecord(
            trigger_run_id="run-multi",
            trigger_id="trig-1",
            task_id_key="pkg.task",
            logic_value="and",
            valid_condition_ids=["vc-1", "vc-2"],
            condition_ids=["c-1", "c-2"],
            triggered_invocation_id="trg-inv",
            participants=[
                TriggerRunParticipant(
                    context_type="ResultContext",
                    condition_id="c-1",
                    valid_condition_id="vc-1",
                    source_invocation_id="src-inv",
                    context_timestamp=td.bounds.start_time + timedelta(minutes=11),
                    context_summary="result:ok",
                ),
                TriggerRunParticipant(
                    context_type="ExceptionContext",
                    condition_id="c-2",
                    valid_condition_id="vc-2",
                    source_invocation_id="src-inv",
                    context_timestamp=td.bounds.start_time + timedelta(minutes=11),
                    context_summary="exc:Boom",
                ),
            ],
            claimed_at=td.bounds.start_time + timedelta(minutes=11),
            executed_at=td.bounds.start_time + timedelta(minutes=12),
        )
    ]

    svg = render_timeline_relations(td, style=None)

    assert "result-trigger-relation-line" in svg
    assert "exception-trigger-relation-line" in svg
    # Lines and source condition markers share the same trigger-run id so JS can
    # cross-highlight siblings.
    assert svg.count('data-trigger-run-id="run-multi"') == 4


def test_result_trigger_renders_source_condition_marker() -> None:
    td = _empty_timeline()
    _seed_two_lanes(td)
    td.trigger_runs = [
        _make_run(context_type="ResultContext", source_invocation_id="src-inv")
    ]

    svg = render_timeline_relations(td, style=None)

    assert "result-trigger-relation-line" in svg
    assert "trigger-condition-marker" in svg
    assert 'data-context-type="ResultContext"' in svg
    assert 'data-condition-id="c-1"' in svg
    assert 'width="15" height="15"' in svg
    assert 'rx="2"' in svg
    assert "#15803d" in svg


def test_event_context_participants_are_skipped_here() -> None:
    """Event participants stay with the event-marker lines, not duplicated."""
    td = _empty_timeline()
    _seed_two_lanes(td)
    td.trigger_runs = [
        _make_run(context_type="EventContext", source_invocation_id=None)
    ]

    svg = render_timeline_relations(td, style=None)

    assert "event-trigger-relation-line" not in svg  # no event markers seeded
    assert "status-trigger-relation-line" not in svg


def test_event_origin_relation_line_connects_emitter_to_event_marker() -> None:
    td = _empty_timeline()
    _seed_two_lanes(td)
    td.event_markers = [
        EventMarker(
            event_id="evt-1",
            event_code="result.any.captured",
            timestamp=td.bounds.start_time + timedelta(minutes=11),
            triggered=True,
            matched=True,
            triggered_invocation_ids=["trg-inv"],
            emitted_by_invocation_id="src-inv",
        )
    ]

    svg = render_timeline_relations(td, style=None)

    assert "event-origin-relation-line" in svg
    assert "event-trigger-relation-line" in svg
    assert 'data-event-id="evt-1"' in svg
    assert 'data-source-invocation-id="src-inv"' in svg
    assert 'data-child-invocation-id="trg-inv"' in svg


def test_cron_context_renders_without_source_invocation() -> None:
    """Cron participants have no source invocation but still draw a line."""
    td = _empty_timeline()
    _seed_two_lanes(td)
    td.trigger_runs = [_make_run(context_type="CronContext", source_invocation_id=None)]

    svg = render_timeline_relations(td, style=None)

    assert "cron-trigger-relation-line" in svg
    assert 'data-trigger-run-id="run-1"' in svg


def test_cron_trigger_run_creates_synthetic_event_marker() -> None:
    run = _make_run(context_type="CronContext", source_invocation_id=None)

    markers = cron_event_markers_from_trigger_runs([run])

    assert len(markers) == 1
    marker = markers[0]
    assert marker.event_id == "cron:run-1:c-1"
    assert marker.event_code == "cron.tick"
    assert marker.trigger_run_id == "run-1"
    assert marker.trigger_run_ids == ["run-1"]
    assert marker.context_type == "CronContext"
    assert marker.href == "/trigger-runs/run-1"
    assert marker.triggered_invocation_ids == ["trg-inv"]


def test_cron_runs_from_same_atomic_service_tick_share_one_marker() -> None:
    first = _make_run(context_type="CronContext", source_invocation_id=None)
    first.atomic_service_run_id = "atomic-run-1"
    first.atomic_service_runner_id = "runner-1"
    second = _make_run(context_type="CronContext", source_invocation_id=None)
    second.trigger_run_id = "run-2"
    second.trigger_id = "trig-2"
    second.triggered_invocation_id = "trg-inv-2"
    second.atomic_service_run_id = "atomic-run-1"
    second.atomic_service_runner_id = "runner-1"
    second.participants[0] = TriggerRunParticipant(
        context_type="CronContext",
        condition_id="c-2",
        valid_condition_id="vc-2",
        context_timestamp=first.participants[0].context_timestamp,
        context_summary="timestamp:shared",
    )

    markers = cron_event_markers_from_trigger_runs([first, second])

    assert len(markers) == 1
    marker = markers[0]
    assert marker.trigger_run_ids == ["run-1", "run-2"]
    assert marker.triggered_invocation_ids == ["trg-inv", "trg-inv-2"]
    assert marker.atomic_service_run_id == "atomic-run-1"
    assert marker.href == "/runners/atomic-service/runs/atomic-run-1"
    assert marker.payload_excerpt.endswith("trigger runs:2")


def test_cron_event_marker_renders_trigger_run_highlight_attrs() -> None:
    td = _empty_timeline()
    # Anchor the cron tick on the triggered invocation's lane — the
    # marker now refuses to float so it needs at least a triggered
    # invocation segment in view.
    tgt_lane = td.get_or_create_lane(
        "runner-B", RunnerInfo("Runner", "runner-B", "h2", 1002), "#bbb"
    )
    tgt_lane.add_segment(
        StatusSegment(
            invocation_id="trg-inv",
            start_time=td.bounds.start_time + timedelta(minutes=10),
            end_time=td.bounds.start_time + timedelta(minutes=15),
            status="RUNNING",
            color="#0dcaf0",
        )
    )
    td.event_markers = cron_event_markers_from_trigger_runs(
        [_make_run(context_type="CronContext", source_invocation_id=None)]
    )

    svg = render_event_markers(td, style=None)

    assert 'href="/trigger-runs/run-1"' in svg
    assert 'data-event-code="cron.tick"' in svg
    assert 'data-trigger-run-id="run-1"' in svg
    assert 'data-trigger-run-ids="run-1"' in svg
    assert 'data-context-type="CronContext"' in svg
    assert "cron.tick" in svg
    assert "condition-type-square" not in svg


def test_cron_marker_does_not_render_normal_event_trigger_line() -> None:
    td = _empty_timeline()
    _seed_two_lanes(td)
    run = _make_run(context_type="CronContext", source_invocation_id=None)
    td.trigger_runs = [run]
    td.event_markers = cron_event_markers_from_trigger_runs([run])

    svg = render_timeline_relations(td, style=None)

    assert "cron-trigger-relation-line" in svg
    assert "event-trigger-relation-line" not in svg


def test_cron_relation_starts_at_grouped_atomic_service_marker() -> None:
    td = _empty_timeline()
    _seed_two_lanes(td)
    first = _make_run(context_type="CronContext", source_invocation_id=None)
    first.atomic_service_run_id = "atomic-run-1"
    first.atomic_service_runner_id = "runner-A"
    timestamp = first.participants[0].context_timestamp
    assert timestamp is not None
    second = _make_run(context_type="CronContext", source_invocation_id=None)
    second.trigger_run_id = "run-2"
    second.trigger_id = "trig-2"
    second.triggered_invocation_id = "trg-inv-2"
    second.atomic_service_run_id = "atomic-run-1"
    second.atomic_service_runner_id = "runner-A"
    second.participants[0] = TriggerRunParticipant(
        context_type="CronContext",
        condition_id="c-2",
        valid_condition_id="vc-2",
        context_timestamp=timestamp,
        context_summary="timestamp:shared",
    )
    td.lanes["runner-B"].add_point(
        StatusPoint(
            invocation_id="trg-inv-2",
            timestamp=timestamp + timedelta(minutes=2),
            status="REGISTERED",
            color="#0d6efd",
        )
    )
    td.atomic_service_windows = [
        AtomicServiceWindow(
            runner_id="runner-A",
            start_time=timestamp - timedelta(seconds=1),
            end_time=timestamp + timedelta(seconds=1),
            atomic_service_run_id="atomic-run-1",
            sub_lane=1,
        )
    ]
    td.reserve_auxiliary_sub_lane("runner-A", 1)
    td.trigger_runs = [first, second]
    td.event_markers = cron_event_markers_from_trigger_runs([first, second])

    svg = render_timeline_relations(td, style=None)

    lane = td.lanes["runner-A"]
    expected_x = td.bounds.time_to_x(timestamp)
    expected_y = (
        td.lane_y_position(lane)
        + td.config.bar_y_offset
        + td.config.bar_height
        + 2
        + td.config.bar_height / 2
    )
    assert svg.count(f'd="M{expected_x:.1f},{expected_y:.1f} ') == 4
    assert svg.count('data-relation-kind="cron-trigger"') == 2
    assert 'data-trigger-run-id="run-1"' in svg
    assert 'data-trigger-run-id="run-2"' in svg


def test_non_cron_missing_source_invocation_skips_relation() -> None:
    td = _empty_timeline()
    # Only target lane exists; the result participant source is filtered out.
    target_lane = td.get_or_create_lane(
        "runner-B", RunnerInfo("Runner", "runner-B", "h2", 1002), "#bbb"
    )
    target_lane.add_point(
        StatusPoint(
            invocation_id="trg-inv",
            timestamp=td.bounds.start_time + timedelta(minutes=12),
            status="REGISTERED",
            color="#0d6efd",
        )
    )
    td.trigger_runs = [
        _make_run(context_type="ExceptionContext", source_invocation_id="src-inv")
    ]

    svg = render_timeline_relations(td, style=None)

    assert "exception-trigger-relation-line" not in svg
    assert "trigger-condition-marker" not in svg


def test_event_marker_renders_condition_type_squares() -> None:
    td = _empty_timeline()
    # Anchor the marker on an emitter segment so it actually renders.
    src_lane = td.get_or_create_lane(
        "runner-A", RunnerInfo("Runner", "runner-A", "h1", 1001), "#aaa"
    )
    src_lane.add_segment(
        StatusSegment(
            invocation_id="emitter-inv",
            start_time=td.bounds.start_time + timedelta(minutes=8),
            end_time=td.bounds.start_time + timedelta(minutes=14),
            status="RUNNING",
            color="#0dcaf0",
        )
    )
    td.event_markers = [
        EventMarker(
            event_id="evt-typed",
            event_code="result.any.captured",
            timestamp=td.bounds.start_time + timedelta(minutes=10),
            triggered=True,
            matched=True,
            emitted_by_invocation_id="emitter-inv",
            condition_types=["ResultContext", "ExceptionContext"],
        )
    ]

    svg = render_event_markers(td, style=None)

    assert 'class="condition-type-square"' in svg
    assert 'data-condition-type="ResultContext"' in svg
    assert 'data-condition-type="ExceptionContext"' in svg
    assert "#15803d" in svg
    assert "#dc2626" in svg


def test_legend_names_all_timeline_relation_colours() -> None:
    td = _empty_timeline()

    legend = render_legend(td, SVGStyle())

    assert "atomic service run" in legend
    assert "workflow root" in legend
    assert "Runner" in legend
    assert "event origin" in legend
    assert "event trigger" in legend
    assert "status trigger" in legend
    assert "result trigger" in legend
    assert "exception trigger" in legend
    assert "cron trigger" in legend
    assert "Condition" in legend
    assert "result" in legend
    assert "cron" in legend


def test_missing_target_invocation_skips_line() -> None:
    """No SVG point for the triggered invocation → no line is emitted."""
    td = _empty_timeline()
    # Only the source lane exists; the triggered invocation is missing.
    td.get_or_create_lane(
        "runner-A", RunnerInfo("Runner", "runner-A", "h1", 1001), "#aaa"
    ).add_point(
        StatusPoint(
            invocation_id="src-inv",
            timestamp=td.bounds.start_time + timedelta(minutes=10),
            status="SUCCESS",
            color="#28a745",
        )
    )
    td.trigger_runs = [
        _make_run(context_type="StatusContext", source_invocation_id="src-inv")
    ]

    svg = render_timeline_relations(td, style=None)

    assert "status-trigger-relation-line" not in svg


def test_empty_trigger_runs_does_not_break_existing_relations() -> None:
    """Backward compatibility: no trigger_runs means no Phase 5 lines."""
    td = _empty_timeline()
    _seed_two_lanes(td)
    td.trigger_runs = []

    svg = render_timeline_relations(td, style=None)

    assert "status-trigger-relation-line" not in svg
    # The relations group still renders for direct-call processing.
    assert 'class="timeline-relations"' in svg


def test_atomic_service_window_renders_on_runner_lane() -> None:
    td = _empty_timeline()
    _seed_two_lanes(td)
    td.atomic_service_windows = [
        AtomicServiceWindow(
            runner_id="runner-A",
            start_time=td.bounds.start_time + timedelta(minutes=5),
            end_time=td.bounds.start_time + timedelta(minutes=6),
            atomic_service_run_id="as-run-A",
            duration_seconds=60.0,
        )
    ]

    svg = render_atomic_service_windows(td, SVGStyle())

    assert "atomic-service-window" in svg
    assert 'data-runner-id="runner-A"' in svg
    assert 'data-atomic-service="1"' in svg
    assert (
        'data-atomic-service-id="atomic-service:runner-A:'
        '2025-01-01T00:05:00.000000+00:00"' in svg
    )
    assert 'data-start-time="2025-01-01T00:05:00.000000+00:00"' in svg
    assert 'data-end-time="2025-01-01T00:06:00.000000+00:00"' in svg
    assert 'data-duration-seconds="60.0"' in svg
    assert "Atomic service on runner-A" in svg
    assert 'fill="#f97316"' in svg


def test_atomic_service_window_uses_free_sub_lane_during_invocation() -> None:
    """Service registrations move with the service instead of floating."""
    td = _empty_timeline()
    lane = td.get_or_create_lane(
        "runner-A", RunnerInfo("Runner", "runner-A", "h1", 1001), "#aaa"
    )
    lane.add_segment(
        StatusSegment(
            invocation_id="long-invocation",
            start_time=td.bounds.start_time + timedelta(minutes=5),
            end_time=td.bounds.start_time + timedelta(minutes=15),
            status="RUNNING",
            color="#0ea5e9",
        )
    )
    registered_at = td.bounds.start_time + timedelta(minutes=7)
    lane.add_point(
        StatusPoint(
            invocation_id="triggered-invocation",
            timestamp=registered_at,
            status="REGISTERED",
            color="#95a5a6",
        )
    )
    td.add_global_line(
        StatusLine(
            invocation_id="triggered-invocation",
            start_time=registered_at,
            end_time=registered_at + timedelta(minutes=1),
            from_status="REGISTERED",
            to_status="PENDING",
            color="#95a5a6",
            from_runner_id="runner-A",
            to_runner_id="runner-A",
        )
    )
    windows = [
        AtomicServiceWindow(
            runner_id="runner-A",
            start_time=registered_at - timedelta(seconds=1),
            end_time=td.bounds.start_time + timedelta(minutes=8),
            atomic_service_run_id="as-overlap",
        )
    ]
    trigger_run = _make_run(
        context_type="CronContext",
        source_invocation_id=None,
    )
    trigger_run.atomic_service_run_id = "as-overlap"
    trigger_run.atomic_service_runner_id = "runner-A"
    trigger_run.triggered_invocation_id = "triggered-invocation"

    td.atomic_service_windows = assign_atomic_service_sub_lanes(
        td, windows, [trigger_run]
    )
    svg = render_atomic_service_windows(td, SVGStyle())

    assert td.atomic_service_windows[0].sub_lane == 1
    assert lane.max_sub_lane == 1
    registered_point = next(
        point for point in lane.points if point.invocation_id == "triggered-invocation"
    )
    assert registered_point.sub_lane == 1
    assert td.global_lines[0].from_sub_lane == 1
    expected_y = (
        td.lane_y_position(lane) + td.config.bar_y_offset + td.config.bar_height + 2
    )
    assert f'y="{expected_y:.1f}"' in svg


def test_external_event_marker_anchors_to_its_runner_not_atomic_service() -> None:
    td = _empty_timeline()
    thread_lane = td.get_or_create_lane(
        "runner-A", RunnerInfo("ThreadRunner", "runner-A", "h1", 1001), "#aaa"
    )
    external_lane = td.get_or_create_lane(
        "external-A",
        RunnerInfo("ExternalRunner", "external-A", "h2", 1002),
        "#bbb",
    )
    timestamp = td.bounds.start_time + timedelta(minutes=10)
    td.atomic_service_windows = [
        AtomicServiceWindow(
            runner_id=thread_lane.runner_id,
            start_time=timestamp - timedelta(seconds=1),
            end_time=timestamp + timedelta(seconds=1),
            duration_seconds=2,
            atomic_service_run_id="atomic-run",
        )
    ]
    td.event_markers = [
        EventMarker(
            event_id="external-event",
            event_code="feed_updated",
            timestamp=timestamp,
            triggered=True,
            matched=True,
            emitted_by_runner_context_id=external_lane.runner_id,
        )
    ]

    svg = render_event_markers(td, SVGStyle())

    expected_y = (
        td.lane_y_position(external_lane)
        + td.config.bar_y_offset
        + td.config.bar_height / 2
        - 4
    )
    atomic_y = (
        td.lane_y_position(thread_lane)
        + td.config.bar_y_offset
        + td.config.bar_height / 2
        - 4
    )
    assert 'data-event-id="external-event"' in svg
    assert f'y="{expected_y:.1f}"' in svg
    assert f'y="{atomic_y:.1f}"' not in svg


def test_event_without_emitter_does_not_invent_atomic_service_origin() -> None:
    td = _empty_timeline()
    _seed_two_lanes(td)
    event_time = td.bounds.start_time + timedelta(minutes=11)
    td.atomic_service_windows = [
        AtomicServiceWindow(
            runner_id="runner-A",
            start_time=td.bounds.start_time + timedelta(minutes=10),
            end_time=td.bounds.start_time + timedelta(minutes=12),
            atomic_service_run_id="as-run-A",
        )
    ]
    td.event_markers = [
        EventMarker(
            event_id="evt-atomic",
            event_code="result.captured",
            timestamp=event_time,
            triggered=True,
            matched=True,
            triggered_invocation_ids=["trg-inv"],
        )
    ]

    svg = render_timeline_relations(td, style=None)

    assert "atomic-service-origin-relation-line" not in svg
    assert 'data-runner-id="runner-A"' not in svg
    assert 'data-atomic-service="1"' not in svg
