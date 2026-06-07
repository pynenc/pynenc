"""Focused Pynmon coverage for ``on_event`` and ``with_args_from_event``."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

from pynenc.trigger.conditions import EventContext
from pynenc.trigger.trigger_builder import on_event
from pynenc_tests.integration.pynmon.events_monitoring.conftest import (
    EventMonitoringHarness,
    build_monitoring_app,
)

if TYPE_CHECKING:
    from pynenc_tests.integration.pynmon.conftest import PynmonClient
    from pynenc_tests.integration.pynmon.events_monitoring.conftest import (
        PynmonEventAssertions,
    )


APP_ID = "test-pynmon-event-args"
app, TEMP_DB_PATH = build_monitoring_app(APP_ID)
KEEP_ALIVE = 0
harness = EventMonitoringHarness(app)


@dataclass(frozen=True)
class EventArgsScenario:
    source_event_id: str
    triggered_invocation_id: str


def _args_from_event(context: EventContext) -> dict[str, str]:
    return {
        "label": str(context.payload["label"]),
        "origin": context.event_code,
    }


@app.task
def publish_event_args_source() -> str:
    """Emit the event that drives this small scenario."""
    return harness.emit_event(
        "event.args.source", {"kind": "event", "label": "from-event"}
    )


@app.task(
    triggers=on_event("event.args.source", {"kind": "event"}).with_args_from_event(
        _args_from_event
    )
)
def capture_event_args(label: str, origin: str) -> str:
    harness.emit_event("event.args.captured", {"label": label, "origin": origin})
    return label


@pytest.fixture(scope="module")
def event_args_scenario() -> Iterator[EventArgsScenario]:
    with harness.running():
        source_event_id = publish_event_args_source().result
        harness.wait_for_event_count("event.args.captured")
        runs = app.trigger.get_trigger_runs_for_event(source_event_id)
        run = next(r for r in runs if "capture_event_args" in r.task_id_key)
        assert run.triggered_invocation_id is not None
        yield EventArgsScenario(source_event_id, run.triggered_invocation_id)


class TestOnEventArguments:
    def test_event_detail_exposes_event_context(
        self,
        event_args_scenario: EventArgsScenario,
        pynmon_client: PynmonClient,
    ) -> None:
        response = pynmon_client.get(
            f"/events/{event_args_scenario.source_event_id}/api"
        )
        assert response.status_code == 200
        run = response.json()["trigger_runs"][0]
        assert {p["context_type"] for p in run["participants"]} == {"EventContext"}
        assert (
            run["triggered_invocation_id"]
            == event_args_scenario.triggered_invocation_id
        )

    def test_event_created_invocation_keeps_parent_event(
        self,
        event_args_scenario: EventArgsScenario,
        pynmon_client: PynmonClient,
        event_monitor: PynmonEventAssertions,
    ) -> None:
        event_monitor.event_list_contains(pynmon_client, "event.args.captured")
        triggered_by = event_monitor.invocation_triggered_by_context(
            pynmon_client,
            event_args_scenario.triggered_invocation_id,
            "EventContext",
        )
        assert triggered_by["event_ids"] == [event_args_scenario.source_event_id]
