"""Focused Pynmon coverage for composite trigger logic."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

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


APP_ID = "test-pynmon-composite-logic"
app, TEMP_DB_PATH = build_monitoring_app(APP_ID)
KEEP_ALIVE = 0
harness = EventMonitoringHarness(app)


@dataclass(frozen=True)
class CompositeLogicScenario:
    and_invocation_id: str
    or_invocation_id: str


@app.task(
    triggers=on_event("logic.and.left")
    .on_event("logic.and.right")
    .with_logic("and")
    .with_args_static({"label": "and"})
)
def capture_and_logic(label: str) -> str:
    harness.emit_event("logic.and.captured", {"label": label})
    return label


@app.task(
    triggers=on_event("logic.or.left")
    .on_event("logic.or.right")
    .with_logic("or")
    .with_args_static({"label": "or"})
)
def capture_or_logic(label: str) -> str:
    harness.emit_event("logic.or.captured", {"label": label})
    return label


@pytest.fixture(scope="module")
def composite_logic_scenario() -> Iterator[CompositeLogicScenario]:
    with harness.running(runner_count=2):
        harness.emit_event("logic.and.left", {"side": "left"})
        harness.emit_event("logic.and.right", {"side": "right"})
        harness.emit_event("logic.or.left", {"side": "left"})
        harness.wait_for_event_count("logic.and.captured")
        harness.wait_for_event_count("logic.or.captured")
        and_event = harness.event_by_code("logic.and.captured")
        or_event = harness.event_by_code("logic.or.captured")
        yield CompositeLogicScenario(
            str(and_event.emitted_by_invocation_id),
            str(or_event.emitted_by_invocation_id),
        )


class TestCompositeLogic:
    def test_and_logic_is_visible_in_pynmon(
        self,
        composite_logic_scenario: CompositeLogicScenario,
        pynmon_client: PynmonClient,
        event_monitor: PynmonEventAssertions,
    ) -> None:
        event_monitor.event_list_contains(pynmon_client, "logic.and.captured")
        triggered_by = event_monitor.invocation_triggered_by_context(
            pynmon_client,
            composite_logic_scenario.and_invocation_id,
            "EventContext",
        )
        assert triggered_by["logic_value"] == "and"
        assert len(triggered_by["event_ids"]) == 2

    def test_or_logic_is_visible_in_pynmon(
        self,
        composite_logic_scenario: CompositeLogicScenario,
        pynmon_client: PynmonClient,
        event_monitor: PynmonEventAssertions,
    ) -> None:
        event_monitor.event_list_contains(pynmon_client, "logic.or.captured")
        triggered_by = event_monitor.invocation_triggered_by_context(
            pynmon_client,
            composite_logic_scenario.or_invocation_id,
            "EventContext",
        )
        assert triggered_by["logic_value"] == "or"
        assert len(triggered_by["event_ids"]) == 1
