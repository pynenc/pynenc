"""Focused Pynmon coverage for result triggers and result argument providers."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pytest

from pynenc.trigger.conditions.result import ResultContext
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


APP_ID = "test-pynmon-result-args"
app, TEMP_DB_PATH = build_monitoring_app(APP_ID)
KEEP_ALIVE = 0
harness = EventMonitoringHarness(app)


@dataclass(frozen=True)
class ResultArgsScenario:
    source_invocation_id: str
    any_result_invocation_id: str
    filtered_result_invocation_id: str


def _is_high_score(result: dict[str, Any]) -> bool:
    return int(result["score"]) >= 90


def _args_from_result(context: ResultContext) -> dict[str, str | int]:
    return {
        "case_id": str(context.arguments.kwargs["case_id"]),
        "score": int(context.result["score"]),
    }


@app.task
def score_case(case_id: str) -> dict[str, Any]:
    return {"case_id": case_id, "score": 95}


@app.task(
    triggers=TriggerBuilder()
    .on_any_result(score_case, call_arguments={"case_id": "result-a"})
    .with_args_static({"label": "any-result"})
)
def capture_any_result(label: str) -> str:
    harness.emit_event("result.any.captured", {"label": label})
    return label


@app.task(
    triggers=TriggerBuilder()
    .on_result(score_case, _is_high_score, call_arguments={"case_id": "result-a"})
    .with_args_from_result(_args_from_result)
)
def capture_filtered_result(case_id: str, score: int) -> str:
    harness.emit_event("result.filtered.captured", {"case_id": case_id, "score": score})
    return case_id


@pytest.fixture(scope="module")
def result_args_scenario() -> Iterator[ResultArgsScenario]:
    with harness.running(runner_count=2):
        source = score_case("result-a")
        assert source.result == {"case_id": "result-a", "score": 95}
        harness.wait_for_event_count("result.any.captured")
        harness.wait_for_event_count("result.filtered.captured")
        any_event = harness.event_by_code("result.any.captured")
        filtered_event = harness.event_by_code("result.filtered.captured")
        yield ResultArgsScenario(
            str(source.invocation_id),
            str(any_event.emitted_by_invocation_id),
            str(filtered_event.emitted_by_invocation_id),
        )


class TestOnResultArguments:
    def test_on_any_result_is_visible_from_pynmon_invocation_api(
        self,
        result_args_scenario: ResultArgsScenario,
        pynmon_client: PynmonClient,
        event_monitor: PynmonEventAssertions,
    ) -> None:
        event_monitor.event_list_contains(pynmon_client, "result.any.captured")
        triggered_by = event_monitor.invocation_triggered_by_context(
            pynmon_client,
            result_args_scenario.any_result_invocation_id,
            "ResultContext",
        )
        assert triggered_by["source_invocation_ids"] == [
            result_args_scenario.source_invocation_id
        ]
        family_tree = pynmon_client.get(
            f"/invocations/{result_args_scenario.any_result_invocation_id}"
            "/family-tree?bare=1"
        )
        assert family_tree.status_code == 200
        assert result_args_scenario.source_invocation_id in family_tree.text
        assert "events_monitoring.test_on_result_args.score_case" in family_tree.text
        assert "capture_any_result" in family_tree.text
        assert 'data-relation-kind="result_trigger"' in family_tree.text
        assert 'stroke="#15803d"' in family_tree.text

    def test_on_result_with_args_from_result_is_visible_from_pynmon_invocation_api(
        self,
        result_args_scenario: ResultArgsScenario,
        pynmon_client: PynmonClient,
        event_monitor: PynmonEventAssertions,
    ) -> None:
        event_monitor.event_list_contains(pynmon_client, "result.filtered.captured")
        triggered_by = event_monitor.invocation_triggered_by_context(
            pynmon_client,
            result_args_scenario.filtered_result_invocation_id,
            "ResultContext",
        )
        assert triggered_by["source_invocation_ids"] == [
            result_args_scenario.source_invocation_id
        ]
