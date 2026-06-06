"""Focused Pynmon coverage for exception triggers and argument providers."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

from pynenc.trigger.conditions.exception import ExceptionContext
from pynenc.trigger.trigger_builder import TriggerBuilder
from pynenc_tests.integration.pynmon.events_monitoring.conftest import (
    EventMonitoringHarness,
    build_monitoring_app,
)

if TYPE_CHECKING:
    from pynenc_tests.integration.pynmon.conftest import PynmonClient
    from pynenc_tests.integration.pynmon.events_monitoring.conftest import (
        PynmonEventAssertions,
    )


APP_ID = "test-pynmon-exception-args"
app, TEMP_DB_PATH = build_monitoring_app(APP_ID)
KEEP_ALIVE = 0
harness = EventMonitoringHarness(app)


@dataclass(frozen=True)
class ExceptionArgsScenario:
    source_invocation_id: str
    triggered_invocation_id: str


def _args_from_exception(context: ExceptionContext) -> dict[str, str]:
    return {
        "case_id": str(context.arguments.kwargs["case_id"]),
        "exception_type": context.exception_type,
    }


@app.task
def fail_case(case_id: str) -> str:
    raise ValueError(f"failed-{case_id}")


@app.task(
    triggers=TriggerBuilder()
    .on_exception(
        fail_case,
        exception_types="ValueError",
        call_arguments={"case_id": "exception-a"},
    )
    .with_args_from_exception(_args_from_exception)
)
def capture_exception_args(case_id: str, exception_type: str) -> str:
    harness.emit_event(
        "exception.args.captured",
        {"case_id": case_id, "exception_type": exception_type},
    )
    return case_id


@pytest.fixture(scope="module")
def exception_args_scenario() -> Iterator[ExceptionArgsScenario]:
    with harness.running(runner_count=2):
        source = fail_case("exception-a")
        with pytest.raises(ValueError):
            _ = source.result
        harness.wait_for_event_count("exception.args.captured")
        event = harness.event_by_code("exception.args.captured")
        yield ExceptionArgsScenario(
            str(source.invocation_id), str(event.emitted_by_invocation_id)
        )


class TestOnExceptionArguments:
    def test_exception_trigger_is_visible_from_pynmon_invocation_api(
        self,
        exception_args_scenario: ExceptionArgsScenario,
        pynmon_client: PynmonClient,
        event_monitor: PynmonEventAssertions,
    ) -> None:
        event_monitor.event_list_contains(pynmon_client, "exception.args.captured")
        triggered_by = event_monitor.invocation_triggered_by_context(
            pynmon_client,
            exception_args_scenario.triggered_invocation_id,
            "ExceptionContext",
        )
        assert triggered_by["source_invocation_ids"] == [
            exception_args_scenario.source_invocation_id
        ]
