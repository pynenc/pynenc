"""Unit tests for trigger definition detail views."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from pynenc.identifiers.task_id import TaskId
from pynmon.app import app as pynmon_app
from pynmon.app import setup_routes


class _Condition(SimpleNamespace):
    """Minimal condition object with the trigger-detail serialization hook."""

    condition_id = "condition#orders.created#success#static_hash"

    def _to_json(self, app: object) -> dict[str, str]:
        del app
        return {"event_code": "orders.created"}


def test_trigger_detail_resolves_trigger_and_conditions() -> None:
    trigger = SimpleNamespace(
        trigger_id="trig-1",
        task_id=TaskId.from_key("orders.capture"),
        logic=SimpleNamespace(value="and"),
        condition_ids=[_Condition.condition_id],
        argument_provider=None,
    )
    trigger_backend = MagicMock()
    trigger_backend.get_trigger.return_value = trigger
    trigger_backend.get_conditions_batch.return_value = {
        _Condition.condition_id: _Condition()
    }
    monitored_app = SimpleNamespace(app_id="test-app", trigger=trigger_backend)

    setup_routes()
    with patch("pynmon.views.triggers.get_pynenc_instance", return_value=monitored_app):
        client = TestClient(pynmon_app)
        response = client.get("/triggers/trig-1")

    assert response.status_code == 200
    assert "Trigger Definition" in response.text
    assert "trig-1" in response.text
    assert "orders.capture" in response.text
    assert _Condition.condition_id in response.text
    assert (
        "condition_id=condition%23orders.created%23success%23static_hash"
        in response.text
    )
    assert "orders.created" in response.text


def test_trigger_detail_returns_404_for_missing_trigger() -> None:
    monitored_app = SimpleNamespace(
        app_id="test-app",
        trigger=MagicMock(get_trigger=MagicMock(return_value=None)),
    )

    setup_routes()
    with patch("pynmon.views.triggers.get_pynenc_instance", return_value=monitored_app):
        client = TestClient(pynmon_app)
        response = client.get("/triggers/missing")

    assert response.status_code == 404
    assert "Trigger not found" in response.text
