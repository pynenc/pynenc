"""Invocation family tree data structures and tree-building logic.

Provides FamilyTreeNode and helpers to retrieve the full ancestor-descendant
tree for any invocation, traversing up via parent_invocation_id and down
via get_child_invocations without scanning all stored invocations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pynenc.identifiers.invocation_id import InvocationId
    from pynenc.state_backend.base_state_backend import BaseStateBackend
    from pynenc.trigger.base_trigger import BaseTrigger
    from pynenc.trigger.monitoring import TriggerRunRecord

# Extra budget granted when visiting a node the user explicitly expanded.
_EXPAND_BUDGET_BOOST: int = 30
_SOURCE_TRIGGER_RELATION_KINDS: dict[str, str] = {
    "StatusContext": "status_trigger",
    "ResultContext": "result_trigger",
    "ExceptionContext": "exception_trigger",
}


@dataclass
class FamilyTreeNode:
    """Single node in the invocation family tree.

    :param str invocation_id: The invocation ID string.
    :param str module_name: Task module name.
    :param str func_name: Task function name.
    :param str status: Latest invocation status name.
    :param datetime | None created_at: Timestamp of first history entry (REGISTERED).
    :param float | None elapsed_seconds: Duration from first to last history entry.
    :param list children: Direct and event-driven child nodes.
    :param str relation_kind: Relationship from this node's parent.
    :param bool truncated: True if this node's subtree was cut off by depth/node limits.
    """

    invocation_id: str
    module_name: str
    func_name: str
    status: str
    created_at: datetime | None = None
    elapsed_seconds: float | None = None
    children: list[FamilyTreeNode] = field(default_factory=list)
    relation_kind: str = "direct"
    truncated: bool = False


def _find_root(
    backend: BaseStateBackend,
    inv_id: InvocationId,
    trigger: BaseTrigger | None = None,
) -> InvocationId:
    """Walk the parent chain up to the root (first invocation with no parent).

    Follows ``parent_invocation_id`` first. When that link is absent but
    ``parent_event_id`` is set and a trigger backend is available, jumps
    to the invocation that emitted the originating event so trigger
    children still root onto their emitting ancestor.

    :param backend: State backend for loading invocations.
    :param inv_id: Starting invocation ID.
    :param trigger: Optional trigger backend used to resolve event ancestry.
    :return: Root invocation ID (highest ancestor found).
    """
    seen: set[str] = set()
    current = inv_id
    while str(current) not in seen:
        seen.add(str(current))
        try:
            inv = backend.get_invocation(current)
        except Exception:
            break
        if inv.parent_invocation_id is not None:
            current = inv.parent_invocation_id
            continue
        parent_event_id = getattr(inv, "parent_event_id", None)
        if parent_event_id and trigger is not None:
            try:
                ev = trigger.get_event(str(parent_event_id))
            except Exception:
                ev = None
            emitter = getattr(ev, "emitted_by_invocation_id", None) if ev else None
            if emitter:
                # Re-use InvocationId type via the backend's identifier class.
                from pynenc.identifiers.invocation_id import (
                    InvocationId as _InvId,
                )

                current = _InvId(str(emitter))
                continue
        source_parent = _trigger_source_parent(trigger, str(current))
        if source_parent:
            from pynenc.identifiers.invocation_id import InvocationId as _InvId

            current = _InvId(source_parent)
            continue
        return current
    return current


def _trigger_source_parent(
    trigger: BaseTrigger | None, invocation_id: str
) -> str | None:
    """Return the invocation that indirectly triggered *invocation_id*."""
    if trigger is None:
        return None
    try:
        runs = trigger.get_trigger_runs_for_invocation(invocation_id)
    except Exception:
        return None
    for run in runs:
        parent = _first_source_trigger_invocation(run)
        if parent:
            return parent
    return None


def _first_source_trigger_invocation(run: TriggerRunRecord) -> str | None:
    """Find a status/result/exception source invocation for a trigger run."""
    for participant in getattr(run, "participants", []) or []:
        if participant.context_type not in _SOURCE_TRIGGER_RELATION_KINDS:
            continue
        if participant.source_invocation_id:
            return str(participant.source_invocation_id)
    source_ids = getattr(run, "source_invocation_ids", []) or []
    return str(source_ids[0]) if source_ids else None


def _trigger_relation_kind_for_source(
    run: TriggerRunRecord, source_invocation_id: str
) -> str | None:
    """Return the relation kind for an edge from source to triggered child."""
    for participant in getattr(run, "participants", []) or []:
        if str(participant.source_invocation_id or "") != source_invocation_id:
            continue
        if relation_kind := _SOURCE_TRIGGER_RELATION_KINDS.get(
            participant.context_type
        ):
            return relation_kind
    source_ids = [str(src) for src in getattr(run, "source_invocation_ids", []) or []]
    if source_invocation_id in source_ids:
        return "trigger"
    if not getattr(run, "participants", []) and not source_ids:
        return "trigger"
    return None


def _expansion_count(expand_ids: tuple[str, ...] | frozenset[str], inv_id: str) -> int:
    """Return how many expansion clicks apply to an invocation."""
    return sum(1 for expanded_id in expand_ids if expanded_id == inv_id)


def _unseen_invocation_ids(
    invocation_ids: list[InvocationId], seen: set[str]
) -> list[InvocationId]:
    """Filter child IDs to those not already present in the rendered graph."""
    return [inv_id for inv_id in invocation_ids if str(inv_id) not in seen]


def _prioritize_expanded_child_refs(
    child_refs: list[tuple[InvocationId, str]],
    expand_ids: tuple[str, ...] | frozenset[str],
) -> list[tuple[InvocationId, str]]:
    """Move explicitly expanded children before the normal traversal order."""
    if not expand_ids:
        return child_refs
    expanded_order = {
        expanded_id: index for index, expanded_id in enumerate(expand_ids)
    }
    return sorted(
        child_refs,
        key=lambda ref: (
            str(ref[0]) not in expanded_order,
            expanded_order.get(str(ref[0]), len(expanded_order)),
        ),
    )


def _make_node(inv: object) -> FamilyTreeNode:
    """Create a FamilyTreeNode from a loaded DistributedInvocation.

    :param inv: A DistributedInvocation instance.
    :return: A FamilyTreeNode with task and status metadata.
    """
    task = inv.task  # type: ignore[attr-defined]
    return FamilyTreeNode(
        invocation_id=str(inv.invocation_id),  # type: ignore[attr-defined]
        module_name=task.task_id.module,
        func_name=task.task_id.func_name,
        status=inv.status.name,  # type: ignore[attr-defined]
    )


def _collect_event_children(
    backend: BaseStateBackend,
    trigger: BaseTrigger | None,
    inv_id: InvocationId,
    seen: set[str],
    already: list[InvocationId],
) -> list[InvocationId]:
    """Find children spawned via trigger events emitted by ``inv_id``."""
    if trigger is None:
        return []
    try:
        emitted = trigger.get_events_emitted_by_invocation(str(inv_id), limit=200)
    except Exception:
        return []
    event_children: list[InvocationId] = []
    for ev in emitted:
        try:
            ev_id = getattr(ev, "event_id", None) or getattr(ev, "id", None)
            if ev_id is None:
                continue
            for ec_id in backend.get_invocations_by_parent_event(str(ev_id)):
                key = str(ec_id)
                if key in seen:
                    continue
                if any(key == str(c) for c in event_children):
                    continue
                event_children.append(ec_id)
        except Exception:
            continue
    return event_children


def _collect_trigger_children(
    trigger: BaseTrigger | None,
    inv_id: InvocationId,
    seen: set[str],
    existing: list[InvocationId],
) -> list[tuple[InvocationId, str]]:
    """Find children triggered by status/result/exception of ``inv_id``.

    Returns ``(child_id, relation_kind)`` pairs. Children already in
    ``seen`` or in ``existing`` are skipped.
    """
    if trigger is None:
        return []
    try:
        trig_runs = trigger.get_trigger_runs_sourced_by_invocation(str(inv_id))
    except Exception:
        return []
    from pynenc.identifiers.invocation_id import InvocationId as _InvId

    existing_ids = {str(c) for c in existing}
    out: list[tuple[InvocationId, str]] = []
    for run in trig_runs:
        tgt = getattr(run, "triggered_invocation_id", None)
        if not tgt:
            continue
        tgt_str = str(tgt)
        relation_kind = _trigger_relation_kind_for_source(run, str(inv_id))
        if not relation_kind:
            continue
        if tgt_str in seen or tgt_str in existing_ids:
            continue
        if any(tgt_str == str(c) for c, _kind in out):
            continue
        out.append((_InvId(tgt_str), relation_kind))
    return out


def _has_unrendered_descendants(
    backend: BaseStateBackend,
    trigger: BaseTrigger | None,
    inv_id: InvocationId,
    seen: set[str],
) -> bool:
    """Return True if ``inv_id`` has children that weren't rendered.

    Used at the depth/budget boundary to decide whether to mark the
    node as truncated. Considers direct children, event-spawned
    children, and trigger-sourced children.
    """
    try:
        child_ids = _unseen_invocation_ids(
            list(backend.get_child_invocations(inv_id)), seen
        )
        if child_ids:
            return True
        if trigger is None:
            return False
        emitted = trigger.get_events_emitted_by_invocation(str(inv_id), limit=1)
        for ev in emitted:
            ev_id = getattr(ev, "event_id", None) or getattr(ev, "id", None)
            if ev_id is None:
                continue
            event_children = _unseen_invocation_ids(
                list(backend.get_invocations_by_parent_event(str(ev_id))), seen
            )
            if event_children:
                return True
        trig_runs = trigger.get_trigger_runs_sourced_by_invocation(str(inv_id))
        for run in trig_runs:
            target = getattr(run, "triggered_invocation_id", None)
            if (
                target
                and str(target) not in seen
                and _trigger_relation_kind_for_source(run, str(inv_id))
            ):
                return True
    except Exception:
        return False
    return False


def _recurse_children(
    backend: BaseStateBackend,
    parent: FamilyTreeNode,
    child_refs: list[tuple[InvocationId, str]],
    depth: int,
    counter: list[int],
    seen: set[str],
    expand_ids: tuple[str, ...] | frozenset[str],
    trigger: BaseTrigger | None,
) -> None:
    """Build child nodes and append them to ``parent.children``.

    Stops as soon as the node budget is exhausted.
    """
    for child_id, relation_kind in _prioritize_expanded_child_refs(
        child_refs, expand_ids
    ):
        if counter[0] <= 0:
            break
        child = _build_node(
            backend, child_id, depth, counter, seen, expand_ids, trigger
        )
        if child:
            child.relation_kind = relation_kind
            parent.children.append(child)


def _build_node(
    backend: BaseStateBackend,
    inv_id: InvocationId,
    max_depth: int,
    counter: list[int],
    seen: set[str],
    expand_ids: tuple[str, ...] | frozenset[str] = (),
    trigger: BaseTrigger | None = None,
) -> FamilyTreeNode | None:
    """Recursively build a FamilyTreeNode, respecting depth and node limits.

    When the depth or node budget is exhausted the node is marked as
    truncated so the UI can show a "load more" indicator.
    Nodes whose IDs appear in *expand_ids* receive a budget boost so
    their subtrees are expanded beyond the normal limits.

    :param backend: State backend used to load invocation data.
    :param inv_id: Invocation ID to build a node for.
    :param max_depth: Remaining levels to expand downward.
    :param counter: Single-element mutable counter of remaining node budget.
    :param seen: Already-visited IDs to prevent cycles.
    :param expand_ids: IDs explicitly expanded by the user (get extra budget).
    :return: FamilyTreeNode, or None if already visited.
    """
    if str(inv_id) in seen:
        return None
    # Budget exhausted — return None WITHOUT marking the node as seen so
    # the parent can correctly detect this as a real truncation (not a
    # duplicate already rendered elsewhere). After this point ``seen``
    # only contains nodes that were actually rendered into the tree.
    if counter[0] <= 0:
        return None
    seen.add(str(inv_id))
    counter[0] -= 1
    try:
        inv = backend.get_invocation(inv_id)
    except Exception:
        return None
    node = _make_node(inv)
    _populate_timestamps(backend, inv_id, node)

    # Budget/depth boost for user-expanded nodes. Boost is additive so each
    # click reliably grows the subtree even when the original depth limit
    # was already higher than the per-click increment.
    expansion_count = _expansion_count(expand_ids, str(inv_id))
    effective_depth = max_depth + 4 * expansion_count if expansion_count else max_depth
    if expansion_count:
        counter[0] += _EXPAND_BUDGET_BOOST * expansion_count

    if effective_depth <= 0 or counter[0] <= 0:
        if _has_unrendered_descendants(backend, trigger, inv_id, seen):
            node.truncated = True
        return node

    direct_children = _unseen_invocation_ids(
        list(backend.get_child_invocations(inv_id)), seen
    )
    event_children = _collect_event_children(
        backend, trigger, inv_id, seen, direct_children
    )
    trigger_children = _collect_trigger_children(
        trigger, inv_id, seen, direct_children + event_children
    )

    child_depth = effective_depth - 1
    _recurse_children(
        backend,
        node,
        [(cid, "event") for cid in event_children],
        child_depth,
        counter,
        seen,
        expand_ids,
        trigger,
    )
    _recurse_children(
        backend,
        node,
        [(cid, "direct") for cid in direct_children],
        child_depth,
        counter,
        seen,
        expand_ids,
        trigger,
    )
    _recurse_children(
        backend, node, trigger_children, child_depth, counter, seen, expand_ids, trigger
    )

    # Mark truncated only when some real children remain unrendered.
    # A child already in ``seen`` was rendered elsewhere via a different
    # path; clicking "load more" would not surface anything new for it,
    # so it must not count as truncation.
    rendered_ids = {child.invocation_id for child in node.children}
    all_child_ids = (
        list(direct_children)
        + list(event_children)
        + [cid for cid, _kind in trigger_children]
    )
    for child_id in all_child_ids:
        cid = str(child_id)
        if cid in rendered_ids or cid in seen:
            continue
        node.truncated = True
        break
    return node


def _populate_timestamps(
    backend: BaseStateBackend,
    inv_id: InvocationId,
    node: FamilyTreeNode,
) -> None:
    """Populate created_at and elapsed_seconds from history timestamps.

    :param backend: State backend for loading history.
    :param inv_id: Invocation to fetch history for.
    :param node: Node to populate.
    """
    try:
        history = backend.get_history(inv_id)
        if history:
            node.created_at = history[0].timestamp
            if len(history) >= 2:
                delta = history[-1].timestamp - history[0].timestamp
                node.elapsed_seconds = delta.total_seconds()
    except Exception:
        pass


def build_family_tree(
    backend: BaseStateBackend,
    focus_id: InvocationId,
    max_depth: int = 8,
    max_nodes: int = 60,
    expand_ids: tuple[str, ...] | frozenset[str] = (),
    trigger: BaseTrigger | None = None,
) -> FamilyTreeNode | None:
    """Build a family tree rooted at the earliest ancestor of *focus_id*.

    Walks up to the root via the parent chain, then expands all
    descendants down to *max_depth* or until the node budget is
    exhausted.  Nodes listed in *expand_ids* receive an additional
    budget boost so their subtrees are expanded beyond the initial
    limits — used for progressive "load more" expansion.

    :param backend: State backend to retrieve invocation data from.
    :param focus_id: The invocation to centre the tree on (will be highlighted).
    :param max_depth: Maximum depth to traverse below the root.
    :param max_nodes: Total node cap to prevent runaway queries.
    :param expand_ids: Set of invocation IDs whose subtrees should be expanded.
    :return: Root FamilyTreeNode, or None if the invocation cannot be loaded.
    """
    root_id = _find_root(backend, focus_id, trigger)
    return _build_node(
        backend,
        root_id,
        max_depth,
        [max_nodes],
        set(),
        expand_ids,
        trigger,
    )
