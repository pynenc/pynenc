import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from pynenc.invocation.status import InvocationStatus, InvocationStatusRecord
from pynenc.runner.runner_context import RunnerContext
from pynenc.state_backend.base_state_backend import InvocationHistory
from pynenc.trigger.monitoring import (
    EventRecord,
    TriggerRunParticipant,
    TriggerRunRecord,
)
from pynenc_tests.conftest import MockPynenc
from pynmon.util.log_parser import EntityRef
from pynmon.views.log_explorer_svg import (
    LogSvgParams,
    _compute_time_range,
    build_log_svg,
    compute_log_svg_time_range,
)


def _history(
    invocation_id: str, status: InvocationStatus, timestamp: datetime
) -> InvocationHistory:
    item = InvocationHistory(
        invocation_id=invocation_id,
        status_record=InvocationStatusRecord(status=status),
        runner_context_id="runner-1",
    )
    item._timestamp = timestamp
    return item


def _make_app(
    event: EventRecord,
    run: TriggerRunRecord | None,
    history: list[InvocationHistory],
) -> MockPynenc:
    """Return a MockPynenc with trigger and state_backend configured for SVG tests."""
    context = RunnerContext(
        runner_cls="ThreadRunner",
        runner_id="runner-1",
        parent_ctx=None,
        hostname="local",
        pid=123,
        thread_id=1,
    )
    app = MockPynenc()
    # iter_history_in_timerange is abstract → auto-mocked
    app.state_backend.iter_history_in_timerange.side_effect = (
        lambda start, end, batch_size=100: [
            [item for item in history if start <= item.timestamp <= end]
        ]
    )
    # get_runner_contexts is a concrete wrapper around _get_runner_contexts (abstract)
    app.state_backend.get_runner_contexts = MagicMock(
        side_effect=lambda ids: [context for r in ids if r == context.runner_id]
    )
    app.state_backend.get_runner_context = MagicMock(
        side_effect=lambda runner_id: context
        if runner_id == context.runner_id
        else None
    )
    app.state_backend.get_history = MagicMock(
        side_effect=lambda invocation_id: [
            item for item in history if str(item.invocation_id) == str(invocation_id)
        ]
    )
    app.trigger.get_event.side_effect = (
        lambda event_id: event if event_id == event.event_id else None
    )
    app.trigger.get_trigger_run.side_effect = (
        lambda trigger_run_id: run
        if run and trigger_run_id == run.trigger_run_id
        else None
    )
    app.trigger.get_trigger_runs_for_event.side_effect = (
        lambda eid: [run] if run and eid == event.event_id else []
    )

    def _get_trigger_runs_in_timerange(
        start: datetime,
        end: datetime,
        *,
        event_code: str | None = None,
        task_id_key: str | None = None,
        limit: int | None = None,
    ) -> list[TriggerRunRecord]:
        del event_code, task_id_key, limit
        if run is None:
            return []
        claimed_at = run.claimed_at or event.timestamp
        return [run] if start <= claimed_at <= end else []

    app.trigger.get_trigger_runs_in_timerange.side_effect = (
        _get_trigger_runs_in_timerange
    )
    app.orchestrator.get_atomic_service_executions_in_timerange = MagicMock(
        return_value=[]
    )
    return app


def test_compute_time_range_should_use_subsecond_padding_for_single_log() -> None:
    timestamp = datetime(2026, 5, 18, 11, 3, 6, 831123, tzinfo=UTC)

    start, end = _compute_time_range([timestamp])

    assert (timestamp - start).total_seconds() == pytest.approx(0.25, abs=0.001)
    assert (end - timestamp).total_seconds() == pytest.approx(0.25, abs=0.001)


def test_compute_time_range_should_scale_padding_for_larger_blocks() -> None:
    start_log = datetime(2026, 5, 18, 11, 3, 0, tzinfo=UTC)
    end_log = datetime(2026, 5, 18, 11, 3, 10, tzinfo=UTC)

    start, end = _compute_time_range([start_log, end_log])

    assert (start_log - start).total_seconds() == pytest.approx(1.0, abs=0.001)
    assert (end - end_log).total_seconds() == pytest.approx(1.0, abs=0.001)


def test_log_svg_should_render_referenced_event_markers_and_relations() -> None:
    event_time = datetime(2026, 5, 18, 15, 28, 37, 800000, tzinfo=UTC)
    event = EventRecord(
        event_id="evt-1",
        event_code="result.any.captured",
        timestamp=event_time,
        emitted_by_invocation_id="source-inv",
        triggered_invocation_ids=["child-inv"],
    )
    run = TriggerRunRecord(
        trigger_run_id="run-1",
        trigger_id="trig-1",
        task_id_key="pkg.task",
        logic_value="and",
        valid_condition_ids=["vc-1"],
        condition_ids=["c-1"],
        event_ids=[event.event_id],
        source_invocation_ids=["source-inv"],
        triggered_invocation_id="child-inv",
        claimed_at=event_time,
        executed_at=event_time,
        participants=[
            TriggerRunParticipant(
                context_type="EventContext",
                condition_id="c-1",
                valid_condition_id="vc-1",
                event_id=event.event_id,
                context_timestamp=event_time,
                context_summary="event:result.any.captured",
            )
        ],
    )
    app = _make_app(
        event,
        run,
        [
            # Emitter has a RUNNING segment around the event timestamp so
            # the strict marker anchor can resolve to its visible bar.
            _history(
                "source-inv",
                InvocationStatus.RUNNING,
                event_time - timedelta(milliseconds=50),
            ),
            _history("source-inv", InvocationStatus.SUCCESS, event_time),
            _history("child-inv", InvocationStatus.REGISTERED, event_time),
        ],
    )
    svg = asyncio.run(
        build_log_svg(
            LogSvgParams(
                app=app,
                all_refs=[EntityRef(kind="event", value=event.event_id)],
                utc_timestamps=[event_time],
            )
        )
    )

    assert "event-marker-link" in svg
    assert 'data-event-id="evt-1"' in svg
    # The marker is anchored on the emitter's RUNNING segment (its own
    # bar), so the event-origin line is intentionally suppressed — it
    # would only repeat the bar placement and add visual noise.
    assert "event-origin-relation-line" not in svg
    assert "event-trigger-relation-line" in svg


def test_log_svg_should_backfill_referenced_invocations_outside_window() -> None:
    """Referenced invocations should render even when status history is off-window."""
    event_time = datetime(2026, 5, 18, 15, 28, 37, 800000, tzinfo=UTC)
    source_inv = "11111111-1111-4111-8111-111111111111"
    child_inv = "22222222-2222-4222-8222-222222222222"
    event = EventRecord(
        event_id="evt-1",
        event_code="result.any.captured",
        timestamp=event_time,
        emitted_by_invocation_id=source_inv,
        triggered_invocation_ids=[child_inv],
    )
    run = TriggerRunRecord(
        trigger_run_id="run-1",
        trigger_id="trig-1",
        task_id_key="pkg.task",
        logic_value="and",
        valid_condition_ids=["vc-1"],
        condition_ids=["c-1"],
        event_ids=[event.event_id],
        source_invocation_ids=[source_inv],
        triggered_invocation_id=child_inv,
        claimed_at=event_time,
        executed_at=event_time,
        participants=[],
    )
    app = _make_app(
        event,
        run,
        [
            _history(
                source_inv, InvocationStatus.RUNNING, event_time - timedelta(seconds=2)
            ),
            _history(
                child_inv, InvocationStatus.RUNNING, event_time - timedelta(seconds=2)
            ),
        ],
    )
    svg = asyncio.run(
        build_log_svg(
            LogSvgParams(
                app=app,
                all_refs=[EntityRef(kind="event", value=event.event_id)],
                utc_timestamps=[event_time],
            )
        )
    )

    assert f'data-invocation-id="{source_inv}"' in svg
    assert f'data-invocation-id="{child_inv}"' in svg
    assert "event-trigger-relation-line" in svg


def test_log_svg_should_backfill_visible_invocation_left_boundary() -> None:
    """Visible status points should keep the pre-window running segment."""
    success_time = datetime(2026, 5, 18, 15, 28, 37, 800000, tzinfo=UTC)
    invocation_id = "11111111-1111-4111-8111-111111111111"
    event = EventRecord(
        event_id="evt-unused", event_code="unused", timestamp=success_time
    )
    app = _make_app(
        event,
        None,
        [
            _history(
                invocation_id,
                InvocationStatus.RUNNING,
                success_time - timedelta(milliseconds=500),
            ),
            _history(invocation_id, InvocationStatus.SUCCESS, success_time),
        ],
    )

    svg = asyncio.run(
        build_log_svg(
            LogSvgParams(
                app=app,
                all_refs=[EntityRef(kind="invocation", value=invocation_id)],
                utc_timestamps=[success_time],
            )
        )
    )

    assert 'class="status-segment"><rect' in svg
    assert f'data-invocation-id="{invocation_id}" data-status="RUNNING"' in svg
    assert f'data-invocation-id="{invocation_id}" data-status="SUCCESS"' in svg


def test_log_svg_should_expand_to_event_emitter_for_later_trigger_run_ref() -> None:
    """A trigger-run log should pull in the earlier event emitter invocation."""
    event_time = datetime(2026, 5, 18, 15, 28, 37, 800000, tzinfo=UTC)
    run_time = event_time + timedelta(seconds=2)
    source_inv = "11111111-1111-4111-8111-111111111111"
    child_inv = "22222222-2222-4222-8222-222222222222"
    event = EventRecord(
        event_id="evt-stock",
        event_code="stock.reserved",
        timestamp=event_time,
        emitted_by_invocation_id=source_inv,
        triggered_invocation_ids=[child_inv],
        matched_condition_ids=["condition:event#stock.reserved#static_no_args"],
    )
    run = TriggerRunRecord(
        trigger_run_id="run-release",
        trigger_id="trigger-release",
        task_id_key="pkg.release_shipment",
        logic_value="and",
        valid_condition_ids=["vc-stock"],
        condition_ids=["condition:event#stock.reserved#static_no_args"],
        event_ids=[event.event_id],
        triggered_invocation_id=child_inv,
        claimed_at=run_time,
        executed_at=run_time + timedelta(milliseconds=10),
        participants=[
            TriggerRunParticipant(
                context_type="EventContext",
                condition_id="condition:event#stock.reserved#static_no_args",
                valid_condition_id="vc-stock",
                event_id=event.event_id,
                context_timestamp=event_time,
                context_summary="event:stock.reserved",
            )
        ],
    )
    app = _make_app(
        event,
        run,
        [
            _history(source_inv, InvocationStatus.RUNNING, event_time),
            _history(
                source_inv,
                InvocationStatus.SUCCESS,
                event_time + timedelta(milliseconds=80),
            ),
            _history(child_inv, InvocationStatus.REGISTERED, run_time),
        ],
    )
    timeline_start, timeline_end = asyncio.run(
        compute_log_svg_time_range(
            LogSvgParams(
                app=app,
                all_refs=[EntityRef(kind="trigger-run", value=run.trigger_run_id)],
                utc_timestamps=[run_time],
            )
        )
    )

    svg = asyncio.run(
        build_log_svg(
            LogSvgParams(
                app=app,
                all_refs=[EntityRef(kind="trigger-run", value=run.trigger_run_id)],
                utc_timestamps=[run_time],
            )
        )
    )

    assert timeline_start == event_time - timedelta(milliseconds=250)
    assert run.executed_at is not None
    assert timeline_end == run.executed_at + timedelta(milliseconds=250)
    assert 'data-event-code="stock.reserved"' in svg
    assert f'data-invocation-id="{source_inv}" data-status="RUNNING"' in svg
    assert f'data-invocation-id="{child_inv}"' in svg


def test_log_svg_should_render_atomic_service_window_from_run_ref() -> None:
    """AS refs without an execution record no longer synthesize a window.

    With purge protection on referenced executions in place, the absence
    of an execution record is treated as data loss rather than rendered
    via trigger-run timestamps.
    """
    event_time = datetime(2026, 5, 18, 15, 28, 37, 800000, tzinfo=UTC)
    event = EventRecord(
        event_id="evt-unused",
        event_code="unused",
        timestamp=event_time,
    )
    run = TriggerRunRecord(
        trigger_run_id="run-as-1",
        trigger_id="trig-1",
        task_id_key="pkg.task",
        logic_value="and",
        claimed_at=event_time,
        executed_at=event_time + timedelta(milliseconds=5),
        atomic_service_run_id="as-run-1",
        atomic_service_runner_id="runner-1",
    )
    app = _make_app(event, run, [])

    svg = asyncio.run(
        build_log_svg(
            LogSvgParams(
                app=app,
                all_refs=[EntityRef(kind="atomic-service-run", value="as-run-1")],
                utc_timestamps=[event_time],
            )
        )
    )

    assert 'data-atomic-service-run-id="as-run-1"' not in svg


def test_log_svg_should_render_trigger_ref_timeline_elements() -> None:
    """Trigger refs should pull matching trigger runs into the mini-timeline."""
    event_time = datetime(2026, 5, 18, 15, 28, 37, 800000, tzinfo=UTC)
    event = EventRecord(
        event_id="evt-trigger",
        event_code="result.any.captured",
        timestamp=event_time,
        emitted_by_invocation_id="source-inv",
        triggered_invocation_ids=["child-inv"],
    )
    run = TriggerRunRecord(
        trigger_run_id="run-trigger",
        trigger_id="trigger-1",
        task_id_key="pkg.task",
        logic_value="and",
        valid_condition_ids=["vc-1"],
        condition_ids=["c-1"],
        event_ids=[event.event_id],
        source_invocation_ids=["source-inv"],
        triggered_invocation_id="child-inv",
        claimed_at=event_time,
        executed_at=event_time,
        participants=[
            TriggerRunParticipant(
                context_type="EventContext",
                condition_id="c-1",
                valid_condition_id="vc-1",
                event_id=event.event_id,
                context_timestamp=event_time,
                context_summary="event:result.any.captured",
            )
        ],
    )
    app = _make_app(
        event,
        run,
        [
            _history(
                "source-inv",
                InvocationStatus.RUNNING,
                event_time - timedelta(milliseconds=50),
            ),
            _history("source-inv", InvocationStatus.SUCCESS, event_time),
            _history("child-inv", InvocationStatus.REGISTERED, event_time),
        ],
    )

    svg = asyncio.run(
        build_log_svg(
            LogSvgParams(
                app=app,
                all_refs=[EntityRef(kind="trigger", value="trigger-1")],
                utc_timestamps=[event_time],
            )
        )
    )

    assert 'data-trigger-id="trigger-1"' in svg
    assert 'data-trigger-run-id="run-trigger"' in svg
    assert 'data-event-id="evt-trigger"' in svg


def test_log_svg_should_not_render_recorded_only_event_markers() -> None:
    event_time = datetime(2026, 5, 18, 15, 28, 37, 800000, tzinfo=UTC)
    event = EventRecord(
        event_id="evt-1",
        event_code="result.any.captured",
        timestamp=event_time,
        emitted_by_invocation_id="source-inv",
    )
    app = _make_app(
        event,
        None,
        [_history("source-inv", InvocationStatus.SUCCESS, event_time)],
    )

    svg = asyncio.run(
        build_log_svg(
            LogSvgParams(
                app=app,
                all_refs=[EntityRef(kind="event", value=event.event_id)],
                utc_timestamps=[event_time],
            )
        )
    )

    assert "event-marker-link" not in svg
    assert 'data-event-id="evt-1"' not in svg
