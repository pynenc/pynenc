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

# Extra budget granted when visiting a node the user explicitly expanded.
_EXPAND_BUDGET_BOOST: int = 30


@dataclass
class FamilyTreeNode:
    """Single node in the invocation family tree.

    :param str invocation_id: The invocation ID string.
    :param str module_name: Task module name.
    :param str func_name: Task function name.
    :param str status: Latest invocation status name.
    :param datetime | None created_at: Timestamp of first history entry (REGISTERED).
    :param float | None elapsed_seconds: Duration from first to last history entry.
    :param list children: Direct child nodes (spawned by this invocation).
    :param bool truncated: True if this node's subtree was cut off by depth/node limits.
    """

    invocation_id: str
    module_name: str
    func_name: str
    status: str
    created_at: datetime | None = None
    elapsed_seconds: float | None = None
    children: list[FamilyTreeNode] = field(default_factory=list)
    truncated: bool = False


def _find_root(backend: BaseStateBackend, inv_id: InvocationId) -> InvocationId:
    """Walk the parent chain up to the root (first invocation with no parent).

    :param backend: State backend for loading invocations.
    :param inv_id: Starting invocation ID.
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
        if inv.parent_invocation_id is None:
            return current
        current = inv.parent_invocation_id
    return current


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


def _build_node(
    backend: BaseStateBackend,
    inv_id: InvocationId,
    max_depth: int,
    counter: list[int],
    seen: set[str],
    expand_ids: frozenset[str] = frozenset(),
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
    seen.add(str(inv_id))
    # Budget exhausted — return None, parent will be marked truncated
    if counter[0] <= 0:
        return None
    counter[0] -= 1
    try:
        inv = backend.get_invocation(inv_id)
    except Exception:
        return None
    node = _make_node(inv)
    _populate_timestamps(backend, inv_id, node)

    # Budget/depth boost for user-expanded nodes
    is_expanded = str(inv_id) in expand_ids
    effective_depth = max(max_depth, 4) if is_expanded else max_depth
    if is_expanded:
        counter[0] += _EXPAND_BUDGET_BOOST

    if effective_depth > 0 and counter[0] > 0:
        child_ids = list(backend.get_child_invocations(inv_id))
        for child_id in child_ids:
            if counter[0] <= 0:
                break
            child = _build_node(
                backend,
                child_id,
                effective_depth - 1,
                counter,
                seen,
                expand_ids,
            )
            if child:
                node.children.append(child)
        # Mark truncated only when some children were not expanded
        if len(node.children) < len(child_ids):
            node.truncated = True
    else:
        # At depth limit or budget edge — check if there are un-expanded children
        try:
            child_ids = list(backend.get_child_invocations(inv_id))
            if child_ids:
                node.truncated = True
        except Exception:
            pass
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
    expand_ids: frozenset[str] = frozenset(),
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
    root_id = _find_root(backend, focus_id)
    return _build_node(
        backend,
        root_id,
        max_depth,
        [max_nodes],
        set(),
        expand_ids,
    )
