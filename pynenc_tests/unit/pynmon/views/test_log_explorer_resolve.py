"""Unit tests for pynmon.views.log_explorer_resolve trigger enrichment.

Covers the Phase 3 trigger entity enrichment helpers added to populate
``ref_details`` with summaries for ``event``, ``trigger-run``,
``condition``, and ``trigger`` entity references.
"""

from datetime import UTC, datetime
from types import SimpleNamespace
from urllib.parse import parse_qs

import pytest

from pynenc_tests.conftest import MockPynenc
from pynmon.util.log_parser import EntityRef, parse_log_line
from pynmon.views.log_explorer import LineAnalysis
from pynmon.views.log_explorer_resolve import (
    build_shared_timeline_qs,
    build_timeline_qs,
    collect_all_entity_refs,
    enrich_trigger_entities,
)


def _make_event_record(event_id: str) -> SimpleNamespace:
    """Return a minimal duck-typed EventRecord for the resolver."""
    return SimpleNamespace(
        event_id=event_id,
        event_code="order.placed",
        matched=True,
        triggered=True,
        emitted_by_invocation_id="inv-abc",
        emitted_by_task_id="pkg.task",
        timestamp=datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC),
    )


def _make_trigger_run_record(run_id: str) -> SimpleNamespace:
    return SimpleNamespace(
        trigger_run_id=run_id,
        trigger_id="trig-1",
        task_id_key="pkg.task",
        logic_value="and",
        triggered_invocation_id="inv-xyz",
        source_invocation_ids=[],
        event_ids=[],
        claimed_at=datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC),
        executed_at=datetime(2025, 1, 1, 12, 0, 1, tzinfo=UTC),
        participants=[],
    )


def _make_condition() -> SimpleNamespace:
    cls = type("EventCondition", (SimpleNamespace,), {})
    return cls()


def _make_trigger() -> SimpleNamespace:
    return SimpleNamespace(
        task_id="pkg.task",
        logic=SimpleNamespace(value="and"),
        condition_ids=["c1", "c2", "c3"],
    )


def _make_app() -> MockPynenc:
    """Return a MockPynenc with a configured trigger MagicMock."""
    app = MockPynenc()
    app.trigger.get_events_batch.side_effect = lambda ids: {
        eid: _make_event_record(eid) for eid in ids
    }
    app.trigger.get_trigger_runs_batch.side_effect = lambda ids: {
        rid: _make_trigger_run_record(rid) for rid in ids
    }
    app.trigger.get_conditions_batch.side_effect = lambda ids: {
        cid: _make_condition() for cid in ids
    }
    app.trigger.get_triggers_batch.side_effect = lambda ids: {
        tid: _make_trigger() for tid in ids
    }
    return app


def test_build_timeline_qs_uses_tight_microsecond_window() -> None:
    parsed = parse_log_line(
        "2026-05-18 11:03:06.831123+00:00 INFO pynenc.app invocation:abc task:pkg.task"
    )

    query = build_timeline_qs(parsed, selected="abc")
    params = parse_qs(query)

    assert "+" not in params["start_date"][0]
    assert "+" not in params["end_date"][0]
    start = datetime.fromisoformat(params["start_date"][0])
    end = datetime.fromisoformat(params["end_date"][0])
    center = datetime(2026, 5, 18, 11, 3, 6, 831123)
    assert (center - start).total_seconds() == pytest.approx(0.25, abs=0.001)
    assert (end - center).total_seconds() == pytest.approx(0.25, abs=0.001)
    assert params["resolution"] == ["100ms"]
    assert params["selected"] == ["abc"]


def test_build_shared_timeline_qs_uses_subsecond_minimum_padding() -> None:
    parsed = [
        parse_log_line(
            "2026-05-18 11:03:06.831123+00:00 INFO pynenc.app invocation:abc"
        )
    ]

    query = build_shared_timeline_qs(parsed)
    params = parse_qs(query)

    assert "+" not in params["start_date"][0]
    assert "+" not in params["end_date"][0]
    start = datetime.fromisoformat(params["start_date"][0])
    end = datetime.fromisoformat(params["end_date"][0])
    center = datetime(2026, 5, 18, 11, 3, 6, 831123)
    assert (center - start).total_seconds() == pytest.approx(0.25, abs=0.001)
    assert (end - center).total_seconds() == pytest.approx(0.25, abs=0.001)
    assert params["resolution"] == ["100ms"]


def test_collect_all_entity_refs_includes_atomic_service_bracket_ref() -> None:
    parsed = parse_log_line(
        "2026-05-18 11:03:06.831123+00:00 INFO pynenc.app [AS(as-run-1)] trigger loop"
    )

    refs = collect_all_entity_refs([LineAnalysis(parsed=parsed)])

    assert EntityRef(kind="atomic-service-run", value="as-run-1") in refs


@pytest.mark.asyncio
async def test_enrich_trigger_entities_populates_event_details() -> None:
    app = _make_app()
    refs = [EntityRef(kind="event", value="evt-1")]
    details: dict[str, dict[str, str]] = {}

    await enrich_trigger_entities(app, refs, details)

    assert "event:evt-1" in details
    d = details["event:evt-1"]
    assert d["code"] == "order.placed"
    assert d["matched"] == "1"
    assert d["triggered"] == "1"
    assert d["emitted_by_invocation_id"] == "inv-abc"


@pytest.mark.asyncio
async def test_enrich_trigger_entities_populates_trigger_run_details() -> None:
    app = _make_app()
    refs = [EntityRef(kind="trigger-run", value="run-1")]
    details: dict[str, dict[str, str]] = {}

    await enrich_trigger_entities(app, refs, details)

    assert "trigger-run:run-1" in details
    d = details["trigger-run:run-1"]
    assert d["trigger_id"] == "trig-1"
    assert d["task"] == "pkg.task"
    assert d["logic"] == "and"
    assert d["triggered_invocation_id"] == "inv-xyz"
    assert d["timeline_url"].startswith("/invocations/timeline?")
    assert "selected=inv-xyz" in d["timeline_url"]


@pytest.mark.asyncio
async def test_enrich_trigger_entities_populates_condition_type() -> None:
    app = _make_app()
    refs = [EntityRef(kind="condition", value="cond-1")]
    details: dict[str, dict[str, str]] = {}

    await enrich_trigger_entities(app, refs, details)

    assert "condition:cond-1" in details
    assert details["condition:cond-1"]["type"] == "EventCondition"


@pytest.mark.asyncio
async def test_enrich_trigger_entities_skips_valid_condition_kind() -> None:
    """valid-condition refs are not durable; never fetched."""
    app = _make_app()
    refs = [EntityRef(kind="valid-condition", value="vc-1")]
    details: dict[str, dict[str, str]] = {}

    await enrich_trigger_entities(app, refs, details)

    assert details == {}
    app.trigger.get_conditions_batch.assert_not_called()


@pytest.mark.asyncio
async def test_enrich_trigger_entities_populates_trigger_details() -> None:
    app = _make_app()
    refs = [EntityRef(kind="trigger", value="trig-1")]
    details: dict[str, dict[str, str]] = {}

    await enrich_trigger_entities(app, refs, details)

    assert "trigger:trig-1" in details
    d = details["trigger:trig-1"]
    assert d["task"] == "pkg.task"
    assert d["logic"] == "and"
    assert d["conditions"] == "3"


@pytest.mark.asyncio
async def test_enrich_trigger_entities_handles_missing_records() -> None:
    """None results from the backend are skipped silently."""
    app = MockPynenc()
    app.trigger.get_events_batch.return_value = {"missing-1": None}
    app.trigger.get_trigger_runs_batch.return_value = {"missing-2": None}
    app.trigger.get_conditions_batch.return_value = {"missing-3": None}
    app.trigger.get_triggers_batch.return_value = {"missing-4": None}
    refs = [
        EntityRef(kind="event", value="missing-1"),
        EntityRef(kind="trigger-run", value="missing-2"),
        EntityRef(kind="condition", value="missing-3"),
        EntityRef(kind="trigger", value="missing-4"),
    ]
    details: dict[str, dict[str, str]] = {}

    await enrich_trigger_entities(app, refs, details)

    # Purged or unknown records render without metadata.
    assert details == {}


@pytest.mark.asyncio
async def test_enrich_trigger_entities_skips_already_resolved() -> None:
    """Pre-populated entries in ``details`` are not refetched."""
    app = _make_app()
    refs = [EntityRef(kind="event", value="evt-1")]
    details = {"event:evt-1": {"code": "cached"}}

    await enrich_trigger_entities(app, refs, details)

    assert details["event:evt-1"] == {"code": "cached"}
    app.trigger.get_events_batch.assert_not_called()
