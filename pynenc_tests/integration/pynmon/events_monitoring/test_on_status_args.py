"""Focused Pynmon coverage for ``on_status`` and ``with_args_from_status``."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

from pynenc.trigger.conditions import StatusContext
from pynenc.trigger.trigger_builder import on_status
from pynenc_tests.integration.pynmon.events_monitoring.conftest import (
    EventMonitoringHarness,
    build_monitoring_app,
)

if TYPE_CHECKING:
    from pynenc_tests.integration.pynmon.conftest import PynmonClient
    from pynenc_tests.integration.pynmon.events_monitoring.conftest import (
        PynmonEventAssertions,
    )


APP_ID = "test-pynmon-status-args"
app, TEMP_DB_PATH = build_monitoring_app(APP_ID)
KEEP_ALIVE = 0
harness = EventMonitoringHarness(app)


@dataclass(frozen=True)
class StatusArgsScenario:
    source_invocation_id: str
    triggered_invocation_id: str


def _args_from_status(context: StatusContext) -> dict[str, str]:
    return {
        "job_id": str(context.arguments.kwargs["job_id"]),
        "status": context.status.value,
    }


@app.task
def finish_status_source(job_id: str) -> str:
    return f"done-{job_id}"


@app.task(
    triggers=on_status(
        finish_status_source,
        call_arguments={"job_id": "status-a"},
    ).with_args_from_status(_args_from_status)
)
def capture_status_args(job_id: str, status: str) -> str:
    harness.emit_event("status.args.captured", {"job_id": job_id, "status": status})
    return job_id


@pytest.fixture(scope="module")
def status_args_scenario() -> Iterator[StatusArgsScenario]:
    with harness.running():
        source = finish_status_source("status-a")
        assert source.result == "done-status-a"
        harness.wait_for_event_count("status.args.captured")
        event = harness.event_by_code("status.args.captured")
        yield StatusArgsScenario(
            str(source.invocation_id), str(event.emitted_by_invocation_id)
        )


class TestOnStatusArguments:
    def test_status_trigger_is_visible_from_triggered_invocation(
        self,
        status_args_scenario: StatusArgsScenario,
        pynmon_client: PynmonClient,
        event_monitor: PynmonEventAssertions,
    ) -> None:
        event_monitor.event_list_contains(pynmon_client, "status.args.captured")
        triggered_by = event_monitor.invocation_triggered_by_context(
            pynmon_client,
            status_args_scenario.triggered_invocation_id,
            "StatusContext",
        )
        assert triggered_by["source_invocation_ids"] == [
            status_args_scenario.source_invocation_id
        ]
