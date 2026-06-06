"""Focused Pynmon coverage for remaining ``TriggerBuilder`` modifiers."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

from pynenc.trigger.arguments.argument_filters import create_argument_filter
from pynenc.trigger.arguments.argument_providers import StaticArgumentProvider
from pynenc.trigger.conditions import EventContext
from pynenc.trigger.conditions.event import EventCondition
from pynenc.trigger.trigger_builder import TriggerBuilder, on_event
from pynenc.trigger.trigger_context import TriggerContext
from pynenc_tests.integration.pynmon.events_monitoring.conftest import (
    EventMonitoringHarness,
    build_monitoring_app,
)

if TYPE_CHECKING:
    from pynenc_tests.integration.pynmon.conftest import PynmonClient
    from pynenc_tests.integration.pynmon.events_monitoring.conftest import (
        PynmonEventAssertions,
    )


APP_ID = "test-pynmon-builder-modifiers"
app, TEMP_DB_PATH = build_monitoring_app(APP_ID)
KEEP_ALIVE = 0
harness = EventMonitoringHarness(app)


@dataclass(frozen=True)
class BuilderModifierScenario:
    triggered_invocations: dict[str, str]


def _args_from_trigger_context(context: TriggerContext) -> dict[str, str]:
    valid = next(iter(context.valid_conditions.values()))
    event_context = valid.context
    if not isinstance(event_context, EventContext):
        raise TypeError("expected EventContext")
    return {"label": str(event_context.payload["label"])}


def _publish_sources() -> None:
    for event_code, label in [
        ("modifier.static.source", "static"),
        ("modifier.legacy.source", "legacy"),
        ("modifier.provider.source", "provider"),
        ("modifier.direct.source", "direct"),
        ("modifier.custom.source", "custom"),
    ]:
        harness.emit_event(event_code, {"label": label})


@app.task(
    triggers=on_event("modifier.static.source").with_args_static({"label": "static"})
)
def capture_static_args(label: str) -> str:
    harness.emit_event("modifier.static.captured", {"label": label})
    return label


@app.task(
    triggers=on_event("modifier.legacy.source").with_arguments({"label": "legacy"})
)
def capture_legacy_arguments(label: str) -> str:
    harness.emit_event("modifier.legacy.captured", {"label": label})
    return label


@app.task(
    triggers=on_event("modifier.provider.source").with_args_provider(
        StaticArgumentProvider({"label": "provider"})
    )
)
def capture_provider_args(label: str) -> str:
    harness.emit_event("modifier.provider.captured", {"label": label})
    return label


@app.task(
    triggers=on_event("modifier.direct.source").with_args_from_trigger_context(
        _args_from_trigger_context
    )
)
def capture_direct_context_args(label: str) -> str:
    harness.emit_event("modifier.direct.captured", {"label": label})
    return label


@app.task(
    triggers=TriggerBuilder()
    .add_condition(
        EventCondition(
            "modifier.custom.source",
            create_argument_filter({"label": "custom"}),
        )
    )
    .with_args_static({"label": "custom"})
)
def capture_added_condition(label: str) -> str:
    harness.emit_event("modifier.custom.captured", {"label": label})
    return label


@pytest.fixture(scope="module")
def builder_modifier_scenario() -> Iterator[BuilderModifierScenario]:
    with harness.running(runner_count=2):
        _publish_sources()
        triggered_invocations: dict[str, str] = {}
        for label in ["static", "legacy", "provider", "direct", "custom"]:
            event_code = f"modifier.{label}.captured"
            harness.wait_for_event_count(event_code)
            event = harness.event_by_code(event_code)
            triggered_invocations[label] = str(event.emitted_by_invocation_id)
        yield BuilderModifierScenario(triggered_invocations)


class TestBuilderModifiers:
    @pytest.mark.parametrize(
        ("label", "event_code"),
        [
            ("static", "modifier.static.captured"),
            ("legacy", "modifier.legacy.captured"),
            ("provider", "modifier.provider.captured"),
            ("direct", "modifier.direct.captured"),
            ("custom", "modifier.custom.captured"),
        ],
    )
    def test_modifier_trigger_is_visible_in_pynmon(
        self,
        builder_modifier_scenario: BuilderModifierScenario,
        pynmon_client: PynmonClient,
        event_monitor: PynmonEventAssertions,
        label: str,
        event_code: str,
    ) -> None:
        event_monitor.event_list_contains(pynmon_client, event_code)
        triggered_by = event_monitor.invocation_triggered_by_context(
            pynmon_client,
            builder_modifier_scenario.triggered_invocations[label],
            "EventContext",
        )
        assert triggered_by["task_id_key"].endswith(
            {
                "static": "capture_static_args",
                "legacy": "capture_legacy_arguments",
                "provider": "capture_provider_args",
                "direct": "capture_direct_context_args",
                "custom": "capture_added_condition",
            }[label]
        )
