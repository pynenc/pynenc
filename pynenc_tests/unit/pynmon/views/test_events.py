"""Unit tests for the pynmon events router."""

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import TYPE_CHECKING
from urllib.parse import parse_qs, urlparse
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from pynenc.trigger.monitoring import (
    EventRecord,
    TriggerRunParticipant,
    TriggerRunRecord,
)
from pynenc_tests.conftest import MockPynenc
from pynmon.app import app as pynmon_app
from pynmon.app import setup_routes
from pynmon.util.timeline_zoom import invocation_zoom_window
from pynmon.views.events import _timeline_url_for_event
from pynmon.views.trigger_runs import _timeline_url_for_run

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest

    from pynenc import Pynenc

mock_app = MockPynenc()


@mock_app.task
def reactor_task(x: int) -> int:
    return x


@pytest.fixture
def app_events(request: "FixtureRequest", app_instance: "Pynenc") -> "Pynenc":
    app = app_instance
    app._tasks = mock_app._tasks
    reactor_task.app = app
    app.purge()
    request.addfinalizer(app.purge)
    return app


def _store_event(
    app: "Pynenc",
    event_id: str = "evt-1",
    code: str = "alpha",
    matched_condition_ids: list[str] | None = None,
    valid_condition_ids: list[str] | None = None,
    triggered_invocation_ids: list[str] | None = None,
    emitted_by_invocation_id: str | None = None,
) -> EventRecord:
    record = EventRecord(
        event_id=event_id,
        event_code=code,
        payload={"x": 1},
        timestamp=datetime.now(UTC),
        matched_condition_ids=matched_condition_ids or [],
        valid_condition_ids=valid_condition_ids or matched_condition_ids or [],
        triggered_invocation_ids=triggered_invocation_ids or [],
        emitted_by_invocation_id=emitted_by_invocation_id,
    )
    app.trigger.store_event(record)
    return record


def test_events_list_returns_html(app_events: "Pynenc") -> None:
    setup_routes()
    _store_event(app_events, "evt-1", "alpha")
    _store_event(app_events, "evt-2", "beta")

    with patch("pynmon.views.events.get_pynenc_instance", return_value=app_events):
        client = TestClient(pynmon_app)
        response = client.get("/events/")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "Event Monitor" in response.text
    assert "alpha" in response.text
    assert "beta" in response.text


def test_events_list_filters_by_event_code(app_events: "Pynenc") -> None:
    setup_routes()
    _store_event(app_events, "evt-a", "alpha")
    _store_event(app_events, "evt-b", "beta")

    with patch("pynmon.views.events.get_pynenc_instance", return_value=app_events):
        client = TestClient(pynmon_app)
        response = client.get("/events/?event_code=alpha")

    assert response.status_code == 200
    assert "evt-a"[:8] in response.text
    # other code should not appear in table rows (id prefix)
    # 'beta' may appear in the dropdown options; rely on the event id prefix
    assert "evt-b"[:8] not in response.text


def test_events_list_ignores_empty_event_code_with_other_filters(
    app_events: "Pynenc",
) -> None:
    setup_routes()
    _store_event(app_events, "evt-matched", "alpha", matched_condition_ids=["cond-1"])
    _store_event(app_events, "evt-plain", "beta")

    with patch("pynmon.views.events.get_pynenc_instance", return_value=app_events):
        client = TestClient(pynmon_app)
        response = client.get("/events/?event_code=&matched=yes")

    assert response.status_code == 200
    assert "evt-mat" in response.text
    assert "evt-pla" not in response.text


def test_events_list_should_hide_pagination_when_everything_fits(
    app_events: "Pynenc",
) -> None:
    """A one-page result set should not offer dead pagination links."""
    setup_routes()
    _store_event(app_events, "evt-a", "alpha")
    _store_event(app_events, "evt-b", "beta")

    with patch("pynmon.views.events.get_pynenc_instance", return_value=app_events):
        client = TestClient(pynmon_app)
        response = client.get("/events/?page=25&page_size=200")

    assert response.status_code == 200
    assert "Showing 2 of 2 events" in response.text
    assert "Page 1 of 1" in response.text
    assert "Events pagination" not in response.text
    assert "evt-a"[:8] in response.text
    assert "evt-b"[:8] in response.text


def test_events_list_shows_condition_and_triggered_invocation_ids(
    app_events: "Pynenc",
) -> None:
    setup_routes()
    _store_event(
        app_events,
        "evt-aggregated",
        "result.any.captured",
        matched_condition_ids=["condition-result-captured-long-id"],
        valid_condition_ids=[
            "condition-result-captured-long-id",
            "condition-other-evaluated-long-id",
        ],
        triggered_invocation_ids=["triggered-invocation-long-id"],
        emitted_by_invocation_id="source-invocation-long-id",
    )

    with patch("pynmon.views.events.get_pynenc_instance", return_value=app_events):
        client = TestClient(pynmon_app)
        response = client.get("/events/?event_code=result.any.captured")

    assert response.status_code == 200
    assert "Emitter invocation" in response.text
    assert "/events/evt-aggregated" in response.text
    assert "evt-aggregated" in response.text
    assert "condition-result-captured-long-id" in response.text
    assert "2 evaluated" in response.text
    assert "/invocations/triggered-invocation-long-id" in response.text
    assert "source-invocation-long-id" in response.text


def test_events_list_redirects_empty_page_past_end(app_events: "Pynenc") -> None:
    """Manual page URLs beyond the last page should canonicalize to page 1."""
    setup_routes()

    with patch("pynmon.views.events.get_pynenc_instance", return_value=app_events):
        client = TestClient(pynmon_app)
        response = client.get(
            "/events/?event_code=&start=&end=&matched=all&triggered=all&page_size=25&page=2",
            follow_redirects=False,
        )

    assert response.status_code == 303
    assert "page=1" in response.headers["location"]


def test_event_detail_returns_html(app_events: "Pynenc") -> None:
    setup_routes()
    record = _store_event(app_events, "evt-detail", "alpha")

    with patch("pynmon.views.events.get_pynenc_instance", return_value=app_events):
        client = TestClient(pynmon_app)
        response = client.get(f"/events/{record.event_id}")

    assert response.status_code == 200
    assert "Event Detail" in response.text
    assert record.event_id in response.text


def test_event_detail_returns_404_for_unknown(app_events: "Pynenc") -> None:
    setup_routes()
    with patch("pynmon.views.events.get_pynenc_instance", return_value=app_events):
        client = TestClient(pynmon_app)
        response = client.get("/events/does-not-exist")

    assert response.status_code == 404


def test_event_detail_lists_trigger_runs(app_events: "Pynenc") -> None:
    setup_routes()
    record = _store_event(app_events, "evt-with-run", "alpha")
    run = TriggerRunRecord(
        trigger_run_id="run-1",
        trigger_id="trg-1",
        task_id_key=reactor_task.task_id.key,
        logic_value="EVENT",
        valid_condition_ids=["c1"],
        condition_ids=["c1"],
        event_ids=[record.event_id],
        source_invocation_ids=[],
        triggered_invocation_id="inv-1",
        claimed_at=datetime.now(UTC),
        executed_at=datetime.now(UTC),
    )
    app_events.trigger.store_trigger_run(run)

    with patch("pynmon.views.events.get_pynenc_instance", return_value=app_events):
        client = TestClient(pynmon_app)
        response = client.get(f"/events/{record.event_id}")

    assert response.status_code == 200
    assert "run-1"[:8] in response.text
    assert "inv-1"[:8] in response.text


def test_event_detail_api_returns_all_runs_and_invocation_summaries(
    app_events: "Pynenc",
) -> None:
    setup_routes()
    record = _store_event(
        app_events,
        "evt-multi-run",
        "alpha",
        triggered_invocation_ids=["inv-1", "inv-2"],
    )
    for index in (1, 2):
        condition_ids = [f"event-c-{index}"]
        valid_condition_ids = [f"event-vc-{index}"]
        participants = [
            TriggerRunParticipant(
                context_type="EventContext",
                condition_id=f"event-c-{index}",
                valid_condition_id=f"event-vc-{index}",
                event_id=record.event_id,
            )
        ]
        if index == 1:
            condition_ids.append("status-c-1")
            valid_condition_ids.append("status-vc-1")
            participants.append(
                TriggerRunParticipant(
                    context_type="StatusContext",
                    condition_id="status-c-1",
                    valid_condition_id="status-vc-1",
                    source_invocation_id="source-inv",
                )
            )
        app_events.trigger.store_trigger_run(
            TriggerRunRecord(
                trigger_run_id=f"run-{index}",
                trigger_id=f"trg-{index}",
                task_id_key=reactor_task.task_id.key,
                logic_value="and",
                valid_condition_ids=valid_condition_ids,
                condition_ids=condition_ids,
                event_ids=[record.event_id],
                source_invocation_ids=["source-inv"] if index == 1 else [],
                triggered_invocation_id=f"inv-{index}",
                claimed_at=datetime.now(UTC),
                executed_at=datetime.now(UTC),
                participants=participants,
            )
        )

    with patch("pynmon.views.events.get_pynenc_instance", return_value=app_events):
        client = TestClient(pynmon_app)
        response = client.get(f"/events/{record.event_id}/api")

    assert response.status_code == 200
    data = response.json()
    assert [run["trigger_run_id"] for run in data["trigger_runs"]] == [
        "run-1",
        "run-2",
    ]
    assert set(data["invocation_summaries"]) == {
        "inv-1",
        "inv-2",
        "source-inv",
    }


def test_event_timeline_url_should_use_tight_window_and_scope_invocations() -> None:
    timestamp = datetime(2026, 5, 18, 11, 3, 6, 831000, tzinfo=UTC)
    event = EventRecord(
        event_id="evt-1",
        event_code="result.any.captured",
        timestamp=timestamp,
        emitted_by_invocation_id="source-invocation",
        triggered_invocation_ids=["child-a", "child-b"],
    )

    url = _timeline_url_for_event(event)
    params = parse_qs(urlparse(url).query)

    # Dates are serialised without timezone to avoid '+' being mangled in form
    # submissions, so re-attach UTC before comparing with the aware timestamp.
    start = datetime.fromisoformat(params["start_date"][0]).replace(tzinfo=UTC)
    end = datetime.fromisoformat(params["end_date"][0]).replace(tzinfo=UTC)
    assert (timestamp - start).total_seconds() == pytest.approx(0.1, abs=0.001)
    assert (end - timestamp).total_seconds() == pytest.approx(0.1, abs=0.001)
    assert params["focus_event"] == ["evt-1"]
    assert params["selected"] == ["source-invocation"]
    assert params["inv_ids"][0].split(",") == [
        "source-invocation",
        "child-a",
        "child-b",
    ]


def test_event_timeline_url_should_not_duplicate_emitter_in_scope() -> None:
    event = EventRecord(
        event_id="evt-1",
        event_code="result.any.captured",
        emitted_by_invocation_id="same-invocation",
        triggered_invocation_ids=["same-invocation", "child"],
    )

    url = _timeline_url_for_event(event)
    params = parse_qs(urlparse(url).query)

    assert params["inv_ids"][0].split(",") == ["same-invocation", "child"]


def test_event_timeline_url_uses_emitter_trigger_origin_bounds() -> None:
    origin_time = datetime(2026, 5, 18, 15, 53, 17, 272000, tzinfo=UTC)
    emitter_done = datetime(2026, 5, 18, 15, 53, 17, 449000, tzinfo=UTC)
    event_time = datetime(2026, 5, 18, 15, 53, 17, 397000, tzinfo=UTC)
    event = EventRecord(
        event_id="evt-recorded",
        event_code="result.any.captured",
        timestamp=event_time,
        emitted_by_invocation_id="emitter-inv",
    )
    run = TriggerRunRecord(
        trigger_run_id="run-origin",
        trigger_id="trigger-origin",
        task_id_key="pkg.capture",
        logic_value="and",
        valid_condition_ids=["vc-result"],
        condition_ids=["c-result"],
        source_invocation_ids=["source-inv"],
        triggered_invocation_id="emitter-inv",
        claimed_at=origin_time,
        executed_at=event_time,
        participants=[
            TriggerRunParticipant(
                context_type="ResultContext",
                condition_id="c-result",
                valid_condition_id="vc-result",
                source_invocation_id="source-inv",
                context_timestamp=origin_time,
            )
        ],
    )
    fake_app = MockPynenc()
    fake_app.state_backend.get_history = MagicMock(
        side_effect=lambda invocation_id: [
            SimpleNamespace(timestamp=event_time),
            SimpleNamespace(timestamp=emitter_done),
        ]
    )
    fake_app.trigger.get_trigger_runs_for_invocation.side_effect = (
        lambda invocation_id: [run]
    )

    url = _timeline_url_for_event(event, fake_app, [])
    params = parse_qs(urlparse(url).query)

    assert datetime.fromisoformat(params["start_date"][0]).replace(
        tzinfo=UTC
    ) == origin_time.replace(microsecond=222000)
    assert datetime.fromisoformat(params["end_date"][0]).replace(
        tzinfo=UTC
    ) == emitter_done.replace(microsecond=499000)
    assert "focus_event" not in params
    assert params["selected"] == ["emitter-inv"]
    assert params["inv_ids"][0].split(",") == ["emitter-inv", "source-inv"]


def test_event_and_cron_trigger_run_use_same_zoom_as_invocation() -> None:
    invocation_id = "cron-started-inv"
    cron_context = datetime(2026, 5, 18, 17, 27, 49, 427889, tzinfo=UTC)
    claimed_at = datetime(2026, 5, 18, 17, 27, 49, 435854, tzinfo=UTC)
    executed_at = datetime(2026, 5, 18, 17, 27, 49, 441273, tzinfo=UTC)
    history_start = datetime(2026, 5, 18, 17, 27, 49, 427000, tzinfo=UTC)
    history_end = datetime(2026, 5, 18, 17, 27, 49, 490000, tzinfo=UTC)
    expected_start = datetime(2026, 5, 18, 17, 27, 49, 377000, tzinfo=UTC)
    expected_end = datetime(2026, 5, 18, 17, 27, 49, 540000, tzinfo=UTC)
    run = TriggerRunRecord(
        trigger_run_id="run-cron",
        trigger_id="trigger-cron",
        task_id_key="pkg.recover_pending_invocations",
        logic_value="and",
        valid_condition_ids=["valid-cron"],
        condition_ids=["cron_*/5 * * * *"],
        triggered_invocation_id=invocation_id,
        claimed_at=claimed_at,
        executed_at=executed_at,
        participants=[
            TriggerRunParticipant(
                context_type="CronContext",
                condition_id="cron_*/5 * * * *",
                valid_condition_id="valid-cron",
                context_timestamp=cron_context,
            )
        ],
    )
    event = EventRecord(
        event_id="evt-recorded-result",
        event_code="result.any.captured",
        timestamp=datetime(2026, 5, 18, 17, 27, 49, 480000, tzinfo=UTC),
        emitted_by_invocation_id=invocation_id,
    )
    fake_app = MockPynenc()
    fake_app.state_backend.get_history = MagicMock(
        side_effect=lambda received_invocation_id: [
            SimpleNamespace(timestamp=history_start),
            SimpleNamespace(timestamp=history_end),
        ]
    )
    fake_app.trigger.get_trigger_runs_for_invocation.side_effect = (
        lambda received_invocation_id: [run]
    )

    invocation_window = invocation_zoom_window(fake_app, invocation_id, run)
    trigger_params = parse_qs(urlparse(_timeline_url_for_run(run, [], fake_app)).query)
    event_params = parse_qs(
        urlparse(_timeline_url_for_event(event, fake_app, [])).query
    )

    assert invocation_window == (expected_start, expected_end)
    assert (
        datetime.fromisoformat(trigger_params["start_date"][0]).replace(tzinfo=UTC)
        == expected_start
    )
    assert (
        datetime.fromisoformat(trigger_params["end_date"][0]).replace(tzinfo=UTC)
        == expected_end
    )
    assert (
        datetime.fromisoformat(event_params["start_date"][0]).replace(tzinfo=UTC)
        == expected_start
    )
    assert (
        datetime.fromisoformat(event_params["end_date"][0]).replace(tzinfo=UTC)
        == expected_end
    )
    assert trigger_params["selected"] == [invocation_id]
    assert event_params["selected"] == [invocation_id]
    assert "focus_event" not in event_params


def test_events_auto_purge_returns_json(app_events: "Pynenc") -> None:
    setup_routes()
    with patch("pynmon.views.events.get_pynenc_instance", return_value=app_events):
        client = TestClient(pynmon_app)
        response = client.post(
            "/events/auto-purge", headers={"origin": "http://testserver"}
        )

    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert "deleted" in body
