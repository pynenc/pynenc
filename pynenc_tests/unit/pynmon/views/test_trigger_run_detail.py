"""Unit tests for the pynmon trigger-run detail view."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from urllib.parse import quote_plus
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from pynenc.identifiers.task_id import TaskId
from pynenc.trigger.arguments import create_argument_filter
from pynenc.trigger.arguments.result_filter import create_result_filter
from pynenc.trigger.conditions import ResultCondition
from pynenc.trigger.monitoring import TriggerRunParticipant, TriggerRunRecord
from pynmon.app import app as pynmon_app
from pynmon.app import setup_routes

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest

    from pynenc import Pynenc


@pytest.fixture
def app_trigger_runs(request: FixtureRequest, app_instance: Pynenc) -> Pynenc:
    app = app_instance
    app.purge()
    request.addfinalizer(app.purge)
    return app


def test_trigger_run_detail_shows_condition_and_context_details(
    app_trigger_runs: Pynenc,
) -> None:
    condition = ResultCondition(
        TaskId.from_key("events_monitoring.score_case"),
        create_argument_filter({"case_id": "result-a"}),
        create_result_filter({"score": 95}),
    )
    app_trigger_runs.trigger.register_condition(condition)
    run_id = "trigger-run-full-id-1234567890"
    valid_condition_id = (
        f"valid_condition_{condition.condition_id}_context_result-source-invocation"
    )
    run = TriggerRunRecord(
        trigger_run_id=run_id,
        trigger_id="trigger-full-id-1234567890",
        task_id_key="orders.capture",
        logic_value="and",
        valid_condition_ids=[valid_condition_id],
        condition_ids=[condition.condition_id],
        event_ids=["evt-1"],
        claimed_at=datetime(2026, 5, 24, 10, 0, tzinfo=UTC),
        executed_at=datetime(2026, 5, 24, 10, 0, 1, tzinfo=UTC),
        participants=[
            TriggerRunParticipant(
                context_type="ResultContext",
                condition_id=condition.condition_id,
                valid_condition_id=valid_condition_id,
                source_invocation_id="source-invocation-id-1234567890",
                context_timestamp=datetime(2026, 5, 24, 10, 0, tzinfo=UTC),
                context_summary="status:SUCCESS task:events_monitoring.score_case",
            )
        ],
    )
    app_trigger_runs.trigger.store_trigger_run(run)
    setup_routes()

    with patch(
        "pynmon.views.trigger_runs.get_pynenc_instance", return_value=app_trigger_runs
    ):
        client = TestClient(pynmon_app)
        response = client.get(f"/trigger-runs/{run_id}")
        valid_response = client.get(
            "/trigger-runs/valid-condition",
            params={"valid_condition_id": valid_condition_id},
        )

    assert response.status_code == 200
    assert f"Trigger Run {run_id}" in response.text
    assert "ResultCondition" in response.text
    assert "ResultContext" in response.text
    assert "Context type" not in response.text
    assert "<th>Source invocation</th>" not in response.text
    assert "Conditions (" not in response.text
    assert "TriggerCondition" not in response.text
    assert "ConditionContext" not in response.text
    assert "pynmon-status-badge" in response.text
    assert "SUCCESS" in response.text
    assert "ValidCondition ID" in response.text
    assert condition.condition_id in response.text
    assert valid_condition_id in response.text
    assert (
        f"/trigger-runs/condition?condition_id={quote_plus(condition.condition_id)}"
        in response.text
    )
    assert (
        "/trigger-runs/valid-condition?"
        f"valid_condition_id={quote_plus(valid_condition_id)}" in response.text
    )
    assert "events_monitoring.score_case" in response.text
    assert "score" in response.text

    assert valid_response.status_code == 200
    assert "Valid Condition" in valid_response.text
    assert "reconstructed from trigger-run history" in valid_response.text
    assert "ResultCondition" in valid_response.text
    assert "ResultContext" in valid_response.text
    assert "score" in valid_response.text


def test_trigger_run_api_includes_related_invocation_summaries(
    app_trigger_runs: Pynenc,
) -> None:
    run = TriggerRunRecord(
        trigger_run_id="run-api-summary",
        trigger_id="trigger-api-summary",
        task_id_key="orders.capture",
        logic_value="and",
        valid_condition_ids=["vc-1"],
        condition_ids=["c-1"],
        source_invocation_ids=["source-inv"],
        triggered_invocation_id="target-inv",
        participants=[
            TriggerRunParticipant(
                context_type="StatusContext",
                condition_id="c-1",
                valid_condition_id="vc-1",
                source_invocation_id="source-inv",
            )
        ],
    )
    app_trigger_runs.trigger.store_trigger_run(run)
    setup_routes()

    with patch(
        "pynmon.views.trigger_runs.get_pynenc_instance",
        return_value=app_trigger_runs,
    ):
        client = TestClient(pynmon_app)
        response = client.get(f"/trigger-runs/{run.trigger_run_id}/api")

    assert response.status_code == 200
    assert set(response.json()["invocation_summaries"]) == {
        "source-inv",
        "target-inv",
    }


def test_trigger_condition_detail_resolves_registered_condition(
    app_trigger_runs: Pynenc,
) -> None:
    condition = ResultCondition(
        TaskId.from_key("events_monitoring.score_case"),
        create_argument_filter(None),
        create_result_filter(95),
    )
    app_trigger_runs.trigger.register_condition(condition)
    setup_routes()

    with patch(
        "pynmon.views.trigger_runs.get_pynenc_instance", return_value=app_trigger_runs
    ):
        client = TestClient(pynmon_app)
        response = client.get(
            "/trigger-runs/condition", params={"condition_id": condition.condition_id}
        )

    assert response.status_code == 200
    assert "Trigger Condition" in response.text
    assert "ResultCondition" in response.text
    assert "SUCCESS" in response.text
