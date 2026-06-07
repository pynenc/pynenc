"""Unit tests for Phase 6 trigger-sourced children in the family tree.

`_build_node` now treats invocations produced by trigger runs whose
source participant is the current node as additional children with
``relation_kind == "trigger"``. They appear alongside direct children
(``parent_invocation_id``) and event children (``parent_event_id``)
without duplicating any of them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

from pynenc.identifiers.invocation_id import InvocationId
from pynenc.trigger.monitoring import TriggerRunRecord
from pynmon.util.family_tree import build_family_tree

if TYPE_CHECKING:
    from pynenc.state_backend.base_state_backend import BaseStateBackend


@dataclass
class _FakeInv:
    invocation_id: str
    task_id_str: str = "pkg.task.fn"
    status_name: str = "REGISTERED"
    parent_invocation_id: object | None = None
    parent_event_id: str | None = None

    @property
    def task(self) -> object:
        module, _, func = self.task_id_str.rpartition(".")
        return SimpleNamespace(task_id=SimpleNamespace(module=module, func_name=func))

    @property
    def status(self) -> object:
        return SimpleNamespace(name=self.status_name)


@dataclass
class _FakeBackend:
    invocations: dict[str, _FakeInv]
    direct_children: dict[str, list[str]] = field(default_factory=dict)
    parent_event_children: dict[str, list[str]] = field(default_factory=dict)

    def get_invocation(self, inv_id: object) -> _FakeInv:
        return self.invocations[str(inv_id)]

    def get_child_invocations(self, inv_id: object) -> list[str]:
        return list(self.direct_children.get(str(inv_id), []))

    def get_invocations_by_parent_event(self, event_id: str) -> list[str]:
        return list(self.parent_event_children.get(event_id, []))

    def get_history(self, _inv_id: object) -> list:
        return []


@dataclass
class _FakeTrigger:
    sourced_runs: dict[str, list[TriggerRunRecord]] = field(default_factory=dict)
    emitted_events: dict[str, list[object]] = field(default_factory=dict)

    def get_events_emitted_by_invocation(
        self, inv_id: str, limit: int = 200
    ) -> list[object]:
        return list(self.emitted_events.get(inv_id, []))[:limit]

    def get_trigger_runs_sourced_by_invocation(
        self, inv_id: str, limit: int = 200
    ) -> list[TriggerRunRecord]:
        return list(self.sourced_runs.get(inv_id, []))[:limit]

    def get_event(self, _event_id: str) -> None:
        return None


def _run(*, run_id: str, target: str) -> TriggerRunRecord:
    return TriggerRunRecord(
        trigger_run_id=run_id,
        trigger_id="trig-1",
        task_id_key="pkg.task.child",
        logic_value="and",
        triggered_invocation_id=target,
    )


def test_trigger_sourced_child_appears_with_relation_kind_trigger() -> None:
    backend = _FakeBackend(
        invocations={
            "parent": _FakeInv("parent"),
            "child-via-trigger": _FakeInv("child-via-trigger"),
        }
    )
    trigger = _FakeTrigger(
        sourced_runs={"parent": [_run(run_id="run-1", target="child-via-trigger")]}
    )

    root = build_family_tree(
        cast("BaseStateBackend[Any, Any]", backend),
        InvocationId("parent"),
        max_depth=4,
        max_nodes=20,
        trigger=trigger,  # type: ignore[arg-type]
    )

    assert root is not None
    assert [c.invocation_id for c in root.children] == ["child-via-trigger"]
    assert root.children[0].relation_kind == "trigger"


def test_trigger_child_does_not_duplicate_direct_child() -> None:
    backend = _FakeBackend(
        invocations={
            "parent": _FakeInv("parent"),
            "shared": _FakeInv("shared", parent_invocation_id="parent"),
        },
        direct_children={"parent": ["shared"]},
    )
    trigger = _FakeTrigger(
        sourced_runs={"parent": [_run(run_id="run-1", target="shared")]}
    )

    root = build_family_tree(
        cast("BaseStateBackend[Any, Any]", backend),
        InvocationId("parent"),
        max_depth=4,
        max_nodes=20,
        trigger=trigger,  # type: ignore[arg-type]
    )

    assert root is not None
    kinds = [(c.invocation_id, c.relation_kind) for c in root.children]
    assert kinds == [("shared", "direct")]


def test_multiple_trigger_children_render_all_with_trigger_kind() -> None:
    backend = _FakeBackend(
        invocations={
            "parent": _FakeInv("parent"),
            "child-a": _FakeInv("child-a"),
            "child-b": _FakeInv("child-b"),
        }
    )
    trigger = _FakeTrigger(
        sourced_runs={
            "parent": [
                _run(run_id="run-1", target="child-a"),
                _run(run_id="run-2", target="child-b"),
            ]
        }
    )

    root = build_family_tree(
        cast("BaseStateBackend[Any, Any]", backend),
        InvocationId("parent"),
        max_depth=4,
        max_nodes=20,
        trigger=trigger,  # type: ignore[arg-type]
    )

    assert root is not None
    assert {c.invocation_id for c in root.children} == {"child-a", "child-b"}
    assert all(c.relation_kind == "trigger" for c in root.children)


def test_trigger_run_without_target_invocation_is_skipped() -> None:
    backend = _FakeBackend(invocations={"parent": _FakeInv("parent")})
    trigger = _FakeTrigger(
        sourced_runs={"parent": [_run(run_id="run-noop", target="")]}
    )

    root = build_family_tree(
        cast("BaseStateBackend[Any, Any]", backend),
        InvocationId("parent"),
        max_depth=4,
        max_nodes=20,
        trigger=trigger,  # type: ignore[arg-type]
    )

    assert root is not None
    assert root.children == []


def test_no_trigger_backend_keeps_legacy_behaviour() -> None:
    backend = _FakeBackend(
        invocations={
            "parent": _FakeInv("parent"),
            "kid": _FakeInv("kid", parent_invocation_id="parent"),
        },
        direct_children={"parent": ["kid"]},
    )

    root = build_family_tree(
        cast("BaseStateBackend[Any, Any]", backend),
        InvocationId("parent"),
        max_depth=4,
        max_nodes=20,
        trigger=None,  # type: ignore[arg-type]
    )

    assert root is not None
    assert [c.invocation_id for c in root.children] == ["kid"]
    assert root.children[0].relation_kind == "direct"
