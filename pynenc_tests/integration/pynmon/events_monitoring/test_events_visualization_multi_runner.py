"""
Integration tests for Pynmon event visualization views.

The scenario intentionally creates a rich event graph:
- task-emitted order events
- event-triggered route/reserve/release tasks
- high-priority branch events
- nested task dependencies inside triggered tasks
- unmatched audit events for filter and marker states

Run with ``PYNMON_KEEP_ALIVE=1`` to keep the browser-accessible Pynmon
server alive after the first test completes.
"""

from __future__ import annotations

import os
import re
import tempfile
import threading
import time
from collections.abc import Generator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode

import pytest

from pynenc.builder import PynencBuilder
from pynenc.runner.base_runner import BaseRunner
from pynenc.runner.multi_thread_runner import MultiThreadRunner
from pynenc.runner.persistent_process_runner import PersistentProcessRunner
from pynenc.runner.process_runner import ProcessRunner
from pynenc.runner.thread_runner import ThreadRunner
from pynenc.trigger.conditions import EventContext
from pynenc.trigger.trigger_builder import on_cron, on_event, on_status
from pynenc_tests.conftest import (
    check_all_status_transitions,
    check_no_atomic_service_overlap,
)

if TYPE_CHECKING:
    from pynenc.trigger.monitoring import EventRecord
    from pynenc_tests.integration.pynmon.conftest import PynmonClient

# Keep this at 0 for normal test-suite runs. Use PYNMON_KEEP_ALIVE=1 when
# running this module manually to keep the server open for visual inspection.
KEEP_ALIVE = 0
APP_ID = "test-pynmon-events-visualization"

_temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_temp_db_path = _temp_db.name
_temp_db.close()

app = (
    PynencBuilder()
    .sqlite(_temp_db_path)
    .thread_runner()
    .runner_tuning(
        runner_loop_sleep_time_sec=0.01,
        invocation_wait_results_sleep_time_sec=0.01,
    )
    .app_id(APP_ID)
    .build()
)
# Lower atomic-service cadence so the runners started by the fixture
# tick cron triggers (e.g. ``heartbeat_sweep``) well within the ~20 s
# test window. Cron and event-trigger evaluation both run through the
# real runner atomic-service loop.
app.conf.atomic_service_interval_minutes = 0.01
app.conf.atomic_service_check_interval_minutes = 0.005
app.conf.atomic_service_spread_margin_minutes = 0.0
app.conf.atomic_service_max_start_slot_fraction = 1.0


ORDERS: list[dict[str, Any]] = [
    {
        "order_id": "ord-1001",
        "priority": "high",
        "region": "eu-west",
        "line_items": ["sku-red", "sku-blue"],
    },
    {
        "order_id": "ord-1002",
        "priority": "normal",
        "region": "us-east",
        "line_items": ["sku-green"],
    },
    {
        "order_id": "ord-1003",
        "priority": "high",
        "region": "ap-south",
        "line_items": ["sku-black", "sku-white", "sku-gold"],
    },
    {
        "order_id": "ord-1004",
        "priority": "normal",
        "region": "eu-west",
        "line_items": ["sku-silver"],
    },
]

_event_emit_lock = threading.RLock()


@dataclass(frozen=True)
class EventScenario:
    """Identifiers and window bounds for the generated visualization data."""

    received_event_id: str
    high_priority_event_id: str
    approval_event_id: str
    approval_emitter_invocation_id: str
    audit_event_id: str
    event_ids: list[str]
    start_time: datetime
    end_time: datetime


def _payload_text(payload: dict[str, Any], key: str) -> str:
    """Return a string value from an event payload."""
    return str(payload[key])


def _payload_list(payload: dict[str, Any], key: str) -> list[str]:
    """Return a string list from an event payload."""
    value = payload.get(key, [])
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _route_args_from_event(context: EventContext) -> dict[str, Any]:
    """Build route task arguments from an ``order.received`` event."""
    payload = context.payload
    return {
        "order_id": _payload_text(payload, "order_id"),
        "priority": _payload_text(payload, "priority"),
        "region": _payload_text(payload, "region"),
        "line_items": _payload_list(payload, "line_items"),
        "batch_id": _payload_text(payload, "batch_id"),
    }


def _stock_args_from_event(context: EventContext) -> dict[str, Any]:
    """Build stock-reservation task arguments from an ``order.routed`` event."""
    payload = context.payload
    return {
        "order_id": _payload_text(payload, "order_id"),
        "region": _payload_text(payload, "region"),
        "line_items": _payload_list(payload, "line_items"),
        "risk_score": int(payload["risk_score"]),
    }


def _shipment_args_from_event(context: EventContext) -> dict[str, Any]:
    """Build release task arguments from a ``stock.reserved`` event."""
    payload = context.payload
    return {
        "order_id": _payload_text(payload, "order_id"),
        "region": _payload_text(payload, "region"),
        "line_items": _payload_list(payload, "line_items"),
    }


def _ops_args_from_event(context: EventContext) -> dict[str, str]:
    """Build operations notification arguments from a high-priority event."""
    payload = context.payload
    return {
        "order_id": _payload_text(payload, "order_id"),
        "region": _payload_text(payload, "region"),
    }


def _archive_args_from_event(context: EventContext) -> dict[str, str | int]:
    """Build archive task arguments from a ``batch.closed`` event."""
    payload = context.payload
    return {
        "batch_id": _payload_text(payload, "batch_id"),
        "order_count": int(payload["order_count"]),
    }


def _is_high_priority(payload: dict[str, Any]) -> bool:
    """Match only high-priority order events."""
    return payload.get("priority") == "high"


def _emit_event(event_code: str, payload: dict[str, Any]) -> str:
    """Emit an event through the normal trigger API."""
    with _event_emit_lock:
        return app.trigger.emit_event(event_code, payload)


@app.task
def receive_order_batch(batch_id: str, orders: list[dict[str, Any]]) -> str:
    """Emit the first wave of order events from inside a task invocation."""
    for index, order in enumerate(orders):
        payload = {**order, "batch_id": batch_id, "sequence": index}
        _emit_event("order.received", payload)
        time.sleep(0.01)
    _emit_event(
        "batch.closed",
        {"batch_id": batch_id, "order_count": len(orders)},
    )
    return batch_id


@app.task
def risk_check(order_id: str, priority: str) -> int:
    """Dependency task used by ``route_order``."""
    time.sleep(0.01)
    risk_score = 90 if priority == "high" else 30
    _emit_event(
        "risk.checked",
        {"order_id": order_id, "priority": priority, "risk_score": risk_score},
    )
    return risk_score


@app.task
def price_quote(order_id: str, line_items: list[str], region: str) -> float:
    """Dependency task used by ``route_order``."""
    time.sleep(0.01)
    quote = float(len(line_items) * 11 + len(region))
    _emit_event(
        "quote.ready",
        {"order_id": order_id, "quote": quote, "region": region},
    )
    return quote


@app.task
def reserve_line_item(order_id: str, sku: str) -> str:
    """Dependency task used by ``reserve_stock``."""
    time.sleep(0.005)
    _emit_event("line.reserved", {"order_id": order_id, "sku": sku})
    return sku


@app.task
def pack_order(order_id: str, region: str) -> str:
    """Dependency task used by ``release_shipment``."""
    time.sleep(0.01)
    _emit_event(
        "order.packed",
        {"order_id": order_id, "region": region},
    )
    return f"pack-{order_id}"


@app.task(
    triggers=on_event("order.received").with_args_from_event(_route_args_from_event)
)
def route_order(
    order_id: str,
    priority: str,
    region: str,
    line_items: list[str],
    batch_id: str,
) -> str:
    """Event-triggered task that fans out into dependency tasks."""
    risk_invocation = risk_check(order_id, priority)
    quote_invocation = price_quote(order_id, line_items, region)
    risk_score = risk_invocation.result
    quote = quote_invocation.result
    _emit_event(
        "order.routed",
        {
            "order_id": order_id,
            "batch_id": batch_id,
            "region": region,
            "line_items": line_items,
            "risk_score": risk_score,
            "quote": quote,
        },
    )
    return f"route-{order_id}"


@app.task(
    triggers=on_event("order.routed").with_args_from_event(_stock_args_from_event)
)
def reserve_stock(
    order_id: str,
    region: str,
    line_items: list[str],
    risk_score: int,
) -> str:
    """Event-triggered task that reserves every order line in parallel."""
    reservations = reserve_line_item.parallelize(
        [(order_id, sku) for sku in line_items]
    )
    reserved_items = list(reservations.results)
    _emit_event(
        "stock.reserved",
        {
            "order_id": order_id,
            "region": region,
            "line_items": reserved_items,
            "risk_score": risk_score,
        },
    )
    return f"stock-{order_id}"


@app.task(
    triggers=on_event("stock.reserved").with_args_from_event(_shipment_args_from_event)
)
def release_shipment(order_id: str, region: str, line_items: list[str]) -> str:
    """Event-triggered task that creates the last event in the order chain."""
    pack_id = pack_order(order_id, region).result
    _emit_event(
        "shipment.released",
        {"order_id": order_id, "region": region, "pack_id": pack_id},
    )
    return f"shipment-{order_id}-{len(line_items)}"


@app.task(
    triggers=on_event("order.received", _is_high_priority).with_args_from_event(
        _ops_args_from_event
    )
)
def notify_ops(order_id: str, region: str) -> str:
    """Event-triggered high-priority branch for visual contrast."""
    time.sleep(0.005)
    _emit_event("ops.notified", {"order_id": order_id, "region": region})
    return f"ops-{order_id}"


@app.task(
    triggers=on_event("batch.closed").with_args_from_event(_archive_args_from_event)
)
def archive_batch(batch_id: str, order_count: int) -> str:
    """Event-triggered batch branch that is not order-specific."""
    time.sleep(0.005)
    _emit_event(
        "batch.archived",
        {"batch_id": batch_id, "order_count": order_count},
    )
    return f"archive-{batch_id}"


# ---------------------------------------------------------------------------
# Additional trigger flavours for the Pynmon walkthrough
# ---------------------------------------------------------------------------
#
# The tasks below intentionally exercise the *other* trigger primitives so the
# event monitor / trigger-monitoring views in Pynmon have something to show
# beyond plain ``on_event`` fan-out:
#
# * ``on_status(...)``  \u2014 run after another task finishes.
# * ``on_cron(...)``    \u2014 run on a schedule. The cron is registered and fired
#                          by the real runner atomic-service loop.
# * Composite ``OR``    \u2014 run when *either* of two unrelated signals
#                          appears (event match or upstream-task success).


def _route_audit_args_from_status(context: Any) -> dict[str, str]:
    """Pick fields from a successful ``route_order`` invocation for the audit."""
    call_args = getattr(context, "call_arguments", None) or {}
    return {
        "order_id": str(call_args.get("order_id", "unknown")),
        "region": str(call_args.get("region", "unknown")),
    }


@app.task(
    triggers=on_status(route_order).with_args_from_status(_route_audit_args_from_status)
)
def audit_routed_order(order_id: str, region: str) -> str:
    """Status-triggered task: runs after each ``route_order`` invocation succeeds."""
    time.sleep(0.005)
    _emit_event(
        "audit.route_completed",
        {"order_id": order_id, "region": region},
    )
    return f"audit-route-{order_id}"


@app.task(
    triggers=(
        on_event("order.received", _is_high_priority)
        .on_status(notify_ops)
        .with_logic("or")
        .with_arguments({"label": "priority-or-notify"})
    )
)
def priority_digest(label: str) -> str:
    """Composite-OR trigger: runs when a high-priority order arrives *or* when ``notify_ops`` succeeds.

    The two trigger sources carry different context types (event vs. status),
    so the task uses a static argument set rather than pulling fields from
    the context. This keeps the OR branches symmetric and keeps the demo
    focused on the *trigger* shape rather than the argument plumbing.
    """
    time.sleep(0.005)
    _emit_event("priority.digest_built", {"label": label})
    return f"digest-{label}"


@app.task(triggers=on_cron("* * * * *"))
def heartbeat_sweep() -> str:
    """Cron-triggered task: registered with a minute-resolution schedule.

    The cron schedule is registered so that the trigger-monitoring view in
    Pynmon shows a cron entry alongside the event/status entries. The fixture
    waits for a real runner atomic-service loop to fire it once.
    """
    time.sleep(0.005)
    _emit_event("heartbeat.tick", {"source": "cron"})
    return "heartbeat-ok"


@app.task
def validate_address(batch_id: str) -> str:
    """One of three upstream status conditions for the composite approval."""
    time.sleep(0.005)
    _emit_event("approval.address_validated", {"batch_id": batch_id})
    return f"address-{batch_id}"


@app.task
def validate_payment(batch_id: str) -> str:
    """Second upstream status condition for the composite approval."""
    time.sleep(0.005)
    _emit_event("approval.payment_validated", {"batch_id": batch_id})
    return f"payment-{batch_id}"


@app.task
def validate_inventory(batch_id: str) -> str:
    """Third upstream status condition for the composite approval."""
    time.sleep(0.005)
    _emit_event("approval.inventory_validated", {"batch_id": batch_id})
    return f"inventory-{batch_id}"


@app.task
def request_approval(batch_id: str) -> str:
    """Emit the event side of the composite approval from inside an invocation."""
    _emit_event("approval.requested", {"batch_id": batch_id})
    return f"request-{batch_id}"


@app.task(
    triggers=(
        on_event("approval.requested", {"batch_id": "combo-a"})
        .on_status(validate_address, call_arguments={"batch_id": "combo-a"})
        .on_status(validate_payment, call_arguments={"batch_id": "combo-a"})
        .on_status(validate_inventory, call_arguments={"batch_id": "combo-a"})
        .with_logic("and")
        .with_arguments({"batch_id": "combo-a"})
    )
)
def release_approval(batch_id: str) -> str:
    """Composite AND trigger: event + three successful tasks release approval."""
    time.sleep(0.005)
    _emit_event("approval.released", {"batch_id": batch_id})
    return f"released-{batch_id}"


@pytest.fixture(scope="module", autouse=True)
def cleanup_temp_db() -> Generator[None, None, None]:
    """Clean up the SQLite database after the visual fixture is done."""
    yield
    if _temp_db_path and os.path.exists(_temp_db_path):
        try:
            os.unlink(_temp_db_path)
        except OSError:
            pass


@pytest.fixture(scope="module")
def event_scenario() -> Generator[EventScenario, None, None]:
    """Create a reusable, browser-friendly event visualization scenario."""
    app.purge()
    app.register_deferred_triggers()
    runners = _start_runners()
    try:
        start_time = datetime.now(UTC) - timedelta(seconds=1)
        app.trigger.emit_event(
            "audit.note",
            {"message": "unmatched external event", "component": "visual-test"},
        )
        invocation = receive_order_batch("batch-visual-a", ORDERS)
        assert invocation.result == "batch-visual-a"
        assert validate_address("combo-a").result == "address-combo-a"
        assert validate_payment("combo-a").result == "payment-combo-a"
        assert validate_inventory("combo-a").result == "inventory-combo-a"
        assert request_approval("combo-a").result == "request-combo-a"
        _wait_for_event_count("shipment.released", len(ORDERS))
        _wait_for_event_count("batch.archived", 1)
        _wait_for_event_count("audit.route_completed", len(ORDERS))
        _wait_for_event_count("priority.digest_built", 1)
        _wait_for_event_count("heartbeat.tick", 1)
        _wait_for_event_count("approval.released", 1)
        end_time = datetime.now(UTC) + timedelta(seconds=1)
        yield _build_event_scenario(start_time, end_time)
    finally:
        for runner in runners:
            runner.stop_runner_loop()
        check_all_status_transitions(app)
        check_no_atomic_service_overlap(app)


def _start_runners() -> list[BaseRunner]:
    """Start one runner per supported runner class.

    Each runner gets its own ``Pynenc`` app instance bound to the same
    SQLite database and ``APP_ID``. This mirrors a realistic deployment
    where multiple workers share state through the backend, and lets the
    pynmon visualization receive activity from every runner topology
    (thread, process, multi-thread, persistent-process) at once.
    """
    runner_classes: list[type[BaseRunner]] = [
        ThreadRunner,
        ProcessRunner,
        MultiThreadRunner,
        PersistentProcessRunner,
    ]
    runners: list[BaseRunner] = []
    for runner_cls in runner_classes:
        runner_app = (
            PynencBuilder()
            .sqlite(_temp_db_path)
            .thread_runner()
            .runner_tuning(
                runner_loop_sleep_time_sec=0.01,
                invocation_wait_results_sleep_time_sec=0.01,
            )
            .app_id(APP_ID)
            .build()
        )
        # Match the test app's atomic-service cadence so each runner
        # ticks crons inside the test window.
        runner_app.conf.atomic_service_interval_minutes = 0.01
        runner_app.conf.atomic_service_check_interval_minutes = 0.005
        runner_app.conf.atomic_service_spread_margin_minutes = 0.0
        runner_app.conf.atomic_service_max_start_slot_fraction = 1.0
        runner = runner_cls(runner_app)  # type: ignore[abstract]
        thread = threading.Thread(target=runner.run, daemon=True)
        thread.start()
        runners.append(runner)
    return runners


def _wait_for_event_count(event_code: str, expected_count: int) -> None:
    """Wait until the trigger backend contains ``expected_count`` events."""
    deadline = time.time() + 60
    while time.time() < deadline:
        count = app.trigger.count_events(event_code=event_code)
        if count >= expected_count:
            return
        time.sleep(0.05)
    count = app.trigger.count_events(event_code=event_code)
    raise AssertionError(
        f"Timed out waiting for {expected_count} {event_code} events (got {count})"
    )


def _build_event_scenario(start_time: datetime, end_time: datetime) -> EventScenario:
    """Build a stable set of IDs used by the event-view tests."""
    events = app.trigger.get_events(start_time=start_time, end_time=end_time, limit=250)
    received = _event_by_code(events, "order.received")
    high_priority = _event_by_code(events, "order.received", priority="high")
    approval = _event_by_code(events, "approval.requested")
    audit = _event_by_code(events, "audit.note")
    return EventScenario(
        received_event_id=received.event_id,
        high_priority_event_id=high_priority.event_id,
        approval_event_id=approval.event_id,
        approval_emitter_invocation_id=str(approval.emitted_by_invocation_id or ""),
        audit_event_id=audit.event_id,
        event_ids=[event.event_id for event in events],
        start_time=start_time,
        end_time=end_time,
    )


def _event_by_code(
    events: list[EventRecord], event_code: str, *, priority: str | None = None
) -> EventRecord:
    """Return the first event matching ``event_code`` and optional priority."""
    for event in events:
        if event.event_code != event_code:
            continue
        if priority is not None and event.payload.get("priority") != priority:
            continue
        return event
    raise AssertionError(f"Expected event not found: {event_code}")


def _marker_query(scenario: EventScenario, **extra: object) -> str:
    """Build the marker API query string for the generated time window."""
    params: dict[str, object] = {
        "start": scenario.start_time.isoformat(),
        "end": scenario.end_time.isoformat(),
        "limit": 250,
    }
    params.update(extra)
    return urlencode(params)


@pytest.mark.slow
def test_event_monitor_pages_should_render_rich_fixture(
    event_scenario: EventScenario,
    pynmon_client: PynmonClient,
) -> None:
    """Render list/detail pages with triggered, matched, and unmatched events."""
    response = pynmon_client.get("/events?page_size=200")
    assert response.status_code == 200
    content = response.text
    for event_code in [
        "order.received",
        "order.routed",
        "stock.reserved",
        "shipment.released",
        "audit.route_completed",
        "priority.digest_built",
        "heartbeat.tick",
        "approval.requested",
        "approval.released",
        "audit.note",
    ]:
        assert event_code in content

    filtered = pynmon_client.get(
        "/events?event_code=order.received&matched=yes&triggered=yes&page_size=50"
    )
    assert filtered.status_code == 200
    assert "order.received" in filtered.text
    assert "Event Monitor" in filtered.text

    unmatched = pynmon_client.get(
        "/events?event_code=audit.note&matched=no&triggered=no&page_size=50"
    )
    assert unmatched.status_code == 200
    assert event_scenario.audit_event_id[:8] in unmatched.text

    detail = pynmon_client.get(f"/events/{event_scenario.high_priority_event_id}")
    assert detail.status_code == 200
    assert event_scenario.high_priority_event_id in detail.text
    assert "Triggered invocations" in detail.text


@pytest.mark.slow
def test_composite_event_and_status_trigger_should_be_visible(
    event_scenario: EventScenario,
    pynmon_client: PynmonClient,
) -> None:
    """Show the event + three status-condition trigger as one dashboard relation."""
    detail = pynmon_client.get(f"/events/{event_scenario.approval_event_id}/api")
    assert detail.status_code == 200
    runs = detail.json()["trigger_runs"]
    approval_runs = [run for run in runs if "release_approval" in run["task_id_key"]]
    assert approval_runs
    run = approval_runs[0]
    assert run["logic_value"] == "and"
    assert event_scenario.approval_event_id in run["event_ids"]
    assert len(run["condition_ids"]) == 4
    assert len(run["source_invocation_ids"]) == 3

    invocation_id = run["triggered_invocation_id"]
    invocation = pynmon_client.get(f"/invocations/{invocation_id}/api")
    assert invocation.status_code == 200
    invocation_json = invocation.json()
    assert invocation_json["parent_event_id"] == event_scenario.approval_event_id

    family_tree = pynmon_client.get(
        f"/invocations/{event_scenario.approval_emitter_invocation_id}/family-tree?bare=1"
    )
    assert family_tree.status_code == 200
    assert "ft-edge-event" in family_tree.text


@pytest.mark.slow
def test_event_apis_should_expose_markers_and_trace(
    event_scenario: EventScenario,
    pynmon_client: PynmonClient,
) -> None:
    """Exercise JSON endpoints used by event detail panels and overlays."""
    detail = pynmon_client.get(f"/events/{event_scenario.high_priority_event_id}/api")
    assert detail.status_code == 200
    detail_json = detail.json()
    assert detail_json["event"]["event_code"] == "order.received"
    assert len(detail_json["trigger_runs"]) >= 2

    trace = pynmon_client.get(f"/events/{event_scenario.high_priority_event_id}/trace")
    assert trace.status_code == 200
    trace_json = trace.json()
    assert trace_json["focus_kind"] == "event"
    assert trace_json["focus_id"] == event_scenario.high_priority_event_id
    assert len(trace_json["generated_invocation_ids"]) >= 2

    markers = pynmon_client.get(f"/events/api/markers?{_marker_query(event_scenario)}")
    assert markers.status_code == 200
    marker_json = markers.json()
    assert marker_json["truncated"] is False
    marker_codes = {marker["event_code"] for marker in marker_json["markers"]}
    assert {"order.received", "stock.reserved", "shipment.released"} <= marker_codes

    audit_query = _marker_query(
        event_scenario,
        event_code="audit.note",
        state="unmatched",
    )
    audit_markers = pynmon_client.get(f"/events/api/markers?{audit_query}")
    assert audit_markers.status_code == 200
    audit_json = audit_markers.json()
    assert audit_json["markers"][0]["event_id"] == event_scenario.audit_event_id
    assert audit_json["markers"][0]["matched"] is False


@pytest.mark.slow
def test_invocations_timeline_should_render_event_markers(
    event_scenario: EventScenario,
    pynmon_client: PynmonClient,
) -> None:
    """Render the invocation timeline with event markers in the SVG."""
    response = pynmon_client.get("/invocations/timeline?time_range=15m")
    assert response.status_code == 200
    content = response.text
    assert "event-marker-link" in content
    assert "timeline-relations" in content
    assert "event-trigger-relation-line" in content
    assert "direct-call-relation-line" in content
    assert "matched only" in content
    assert "event origin" in content
    assert "event trigger" in content
    assert f'data-event-id="{event_scenario.high_priority_event_id}"' in content
    assert 'data-event-state="triggered"' in content
    assert "order.received" in content


# ---------------------------------------------------------------------------
# Family-tree "load more" regression test
# ---------------------------------------------------------------------------

_FT_NODE_RE = re.compile(r'class="ft-node[" ]')
_FT_LOAD_MORE_RE = re.compile(r'class="ft-load-more"[^>]*data-expand-id="([^"]+)"')


def _ft_node_count(html: str) -> int:
    return len(_FT_NODE_RE.findall(html))


def _ft_load_more_ids(html: str) -> list[str]:
    return _FT_LOAD_MORE_RE.findall(html)


@pytest.mark.slow
def test_family_tree_load_more_should_grow_tree_on_each_click(
    event_scenario: EventScenario,
    pynmon_client: PynmonClient,
) -> None:
    """Clicking a load-more badge must render strictly more nodes.

    Reproduces the bug observed in pynmon: clicking the ``▼ load more``
    badge for a truncated subtree sometimes returns a response with the
    same node set as the initial render, making the click feel useless.

    Uses the rich event-monitoring fixture: the ``receive_order_batch``
    invocation roots a tree with > 60 descendants (orders, dependency
    tasks, event-triggered tasks, status-triggered audits, composite
    triggers), so the initial render is always truncated and produces
    real load-more badges.
    """
    # 1. Locate the emitter of order.received events — that invocation
    #    sits at the root of the biggest family tree in the scenario.
    received = app.trigger.get_event(event_scenario.received_event_id)
    assert received is not None
    batch_emitter = received.emitted_by_invocation_id
    assert batch_emitter, "Expected an emitter invocation for order.received"
    focus_id = str(batch_emitter)

    # 2. Initial render.
    initial = pynmon_client.get(f"/invocations/{focus_id}/family-tree?bare=1")
    assert initial.status_code == 200, initial.text[:500]
    initial_html = initial.text
    initial_count = _ft_node_count(initial_html)
    initial_badges = _ft_load_more_ids(initial_html)
    print(
        f"[load-more] focus={focus_id} initial_nodes={initial_count} "
        f"badges={len(initial_badges)} targets={initial_badges[:5]}"
    )

    # Regression guard for the user-reported bug: the focus invocation
    # must NOT be marked as truncated when all of its missing children
    # are already rendered elsewhere via different paths (event/trigger
    # edges). Before the fix the focus showed a "load more" badge that,
    # when clicked, returned the exact same tree because the "missing"
    # children were duplicates.
    assert focus_id not in initial_badges, (
        f"Focus {focus_id} was marked as truncated but its missing "
        f"children are duplicates rendered elsewhere. Clicking load-more "
        f"on the focus would return the same tree (bug). "
        f"initial_nodes={initial_count}, badges={initial_badges}"
    )

    if not initial_badges:
        # Nothing to click — the rich-fixture tree fits in the budget
        # after de-duplication. The regression guard above is the
        # meaningful assertion in this case.
        return

    # 3. Click each badge in sequence, matching the JS behaviour: each
    #    click pushes a new id into the expand list and re-requests the
    #    tree with the cumulative expand=<csv>. After every click the
    #    response MUST have strictly more nodes than the previous one.
    expanded_ids: list[str] = []
    previous_count = initial_count
    seen_targets: set[str] = set()

    current_html = initial_html
    for click_index in range(8):
        candidates = [
            eid for eid in _ft_load_more_ids(current_html) if eid not in seen_targets
        ]
        if not candidates:
            break
        target = candidates[0]
        seen_targets.add(target)
        expanded_ids.append(target)
        url = (
            f"/invocations/{focus_id}/family-tree"
            f"?bare=1&expand={','.join(expanded_ids)}"
        )
        resp = pynmon_client.get(url)
        assert resp.status_code == 200, resp.text[:500]
        current_html = resp.text
        current_count = _ft_node_count(current_html)
        print(
            f"[load-more] click={click_index + 1} target={target} "
            f"expand_chain={len(expanded_ids)} nodes={current_count} "
            f"(was {previous_count})"
        )
        assert current_count > previous_count, (
            f"Load-more click #{click_index + 1} on {target} did NOT grow "
            f"the tree: nodes={current_count}, was {previous_count}. "
            f"focus={focus_id}, expand_chain={expanded_ids}. "
            f"This is the user-reported bug — the badge appears but the "
            f"response is identical to the previous render."
        )
        previous_count = current_count
