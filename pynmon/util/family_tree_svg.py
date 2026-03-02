"""SVG rendering for invocation family trees with time-ordered grid layout.

Converts a FamilyTreeNode tree into an inline SVG using a compact grid
where nodes are sorted **globally by created_at** and packed into columns:

- **Y axis** – temporal ordering.  A node never appears above a node that
  started earlier.  Nodes in different columns may share the same Y row.
- **X axis** – greedy column packing.  Children prefer columns near their
  parent to keep connections short.

Connections use Bézier curves from parent to child, adapting direction
(vertical or horizontal) depending on their relative grid positions.

Design tokens come from pynmon's palette: system fonts, status colours from
``status_colors.py``, subtle borders, and compact spacing.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime

from pynmon.util.family_tree import FamilyTreeNode
from pynmon.util.status_colors import STATUS_COLORS, DEFAULT_STATUS_COLOR

# -- layout constants ----------------------------------------------------------
_NODE_W: int = 300
_NODE_H: int = 66  # 4 text lines: ID, module.func, datetime, elapsed
_COL_GAP: int = 24  # horizontal gap between adjacent columns
_GAP_V: int = 12  # vertical gap between nodes in the same column
_TRUNC_H: int = 22  # height of the "load more" badge below truncated nodes
_PAD: int = 8  # padding around the graph in the viewBox
_MAX_COLS: int = 8  # upper bound on columns
_HIERARCHY_W: float = 28.0  # Y penalty per column of distance from parent

# -- pre-computed light tints per status (10% opacity approximations) ----------
_STATUS_TINTS: dict[str, str] = {
    "SUCCESS": "#e8f5e9",
    "FAILED": "#fdecea",
    "RUNNING": "#e3f2fd",
    "PENDING": "#fff8e1",
    "REGISTERED": "#f5f5f5",
    "RETRY": "#f3e5f5",
    "PAUSED": "#e0f2f1",
    "RESUMED": "#e3f2fd",
    "KILLED": "#fdecea",
    "CONCURRENCY_CONTROLLED": "#fff3e0",
    "CONCURRENCY_CONTROLLED_FINAL": "#fff3e0",
    "REROUTED": "#e0f2f1",
    "PENDING_RECOVERY": "#fff3e0",
    "RUNNING_RECOVERY": "#e3f2fd",
}


# -- internal render context ---------------------------------------------------
@dataclass
class _RenderCtx:
    """Bundles rendering options and mutable SVG element accumulators."""

    focus_id: str
    base_url: str
    edges: list[str] = field(default_factory=list)
    nodes: list[str] = field(default_factory=list)


# -- helpers -------------------------------------------------------------------
def _status_color(status: str) -> str:
    """Return a hex colour for the given status name."""
    return STATUS_COLORS.get(status.upper(), DEFAULT_STATUS_COLOR)


def _status_tint(status: str) -> str:
    """Return a light background tint for the given status."""
    return _STATUS_TINTS.get(status.upper(), "#ffffff")


def _esc(text: str) -> str:
    """Escape characters that are special inside SVG text content."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _format_elapsed(seconds: float | None) -> str:
    """Format elapsed seconds as a human-readable duration string.

    :param seconds: Duration in seconds, or None if unavailable.
    :return: Formatted string like "245ms", "1.2s", "3m 12s", or "".
    """
    if seconds is None:
        return ""
    if seconds < 0.001:
        return f"{seconds * 1_000_000:.0f}\u00b5s"
    if seconds < 1.0:
        ms = seconds * 1000
        return f"{ms:.0f}ms" if ms >= 10 else f"{ms:.1f}ms"
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    secs = seconds % 60
    if minutes < 60:
        return f"{minutes}m {secs:.0f}s"
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours}h {mins}m"


# -- layout: time-ordered grid packing ----------------------------------------


def _flatten_tree(root: FamilyTreeNode) -> list[FamilyTreeNode]:
    """Collect all nodes in the tree via iterative DFS.

    :param root: Root of the family tree.
    :return: Flat list of all nodes (order is arbitrary pre-sort).
    """
    nodes: list[FamilyTreeNode] = []
    stack = [root]
    while stack:
        node = stack.pop()
        nodes.append(node)
        for child in reversed(node.children):
            stack.append(child)
    return nodes


def _build_parent_map(root: FamilyTreeNode) -> dict[str, str]:
    """Build a mapping from each child's invocation_id to its parent's.

    :param root: Root of the family tree.
    :return: Dict mapping child ID to parent ID.
    """
    parent_map: dict[str, str] = {}
    stack = [root]
    while stack:
        node = stack.pop()
        for child in node.children:
            parent_map[child.invocation_id] = node.invocation_id
            stack.append(child)
    return parent_map


def _assign_positions(
    root: FamilyTreeNode,
) -> tuple[dict[str, float], dict[str, float]]:
    """Assign X/Y positions using time-ordered greedy grid packing.

    All nodes are sorted globally by ``created_at``.  Each node is placed
    in the column that minimises a weighted score combining Y position and
    distance from parent's column.  The hierarchy weight means children
    tolerate a slightly higher Y to stay near their parent, producing
    visible parent-child clustering without sacrificing compactness.

    A global floor (``min_y``) ensures no node appears above one created
    earlier.

    :param root: Root of the family tree.
    :return: (x_positions, y_positions) dicts mapping invocation_id to
             cx (horizontal centre) and y (top of node box).
    """
    all_nodes = _flatten_tree(root)
    parent_map = _build_parent_map(root)

    all_nodes.sort(key=lambda n: n.created_at or datetime.max)

    num_nodes = len(all_nodes)
    num_cols = min(_MAX_COLS, max(2, math.ceil(math.sqrt(num_nodes))))

    col_bottoms = [0.0] * num_cols
    min_y = 0.0

    x_positions: dict[str, float] = {}
    y_positions: dict[str, float] = {}
    node_cols: dict[str, int] = {}

    col_step = _NODE_W + _COL_GAP

    for node in all_nodes:
        parent_id = parent_map.get(node.invocation_id)
        pref_col = node_cols.get(parent_id, 0) if parent_id else 0

        # Weighted score: lower is better.  Hierarchy weight penalises
        # distance from parent column so children cluster nearby.
        best_score = float("inf")
        best_y = 0.0
        best_col = 0
        for col in range(num_cols):
            y = max(min_y, col_bottoms[col])
            dist = abs(col - pref_col)
            score = y + dist * _HIERARCHY_W
            if score < best_score or (score == best_score and y < best_y):
                best_score = score
                best_y = y
                best_col = col

        cx = _PAD + best_col * col_step + _NODE_W / 2
        x_positions[node.invocation_id] = cx
        y_positions[node.invocation_id] = best_y
        # Truncated nodes need extra space for the "click to expand" badge
        node_h = _NODE_H + (_TRUNC_H if getattr(node, "truncated", False) else 0)
        col_bottoms[best_col] = best_y + node_h + _GAP_V
        node_cols[node.invocation_id] = best_col
        min_y = best_y

    return x_positions, y_positions


# -- viewport ------------------------------------------------------------------
def _svg_viewport(
    x_positions: dict[str, float],
    y_positions: dict[str, float],
    truncated_ids: set[str] | None = None,
) -> tuple[float, float]:
    """Compute (svg_w, svg_h) from layout positions.

    :param x_positions: invocation_id -> cx.
    :param y_positions: invocation_id -> y (top of node).
    :param truncated_ids: IDs of truncated nodes needing extra vertical space.
    :return: SVG width and height with tight padding.
    """
    trunc = truncated_ids or set()
    cx_vals = list(x_positions.values())
    max_cx = max(cx_vals) + _NODE_W / 2 + _PAD
    # Find the max bottom edge considering truncation badges
    max_bottom = 0.0
    for inv_id, y in y_positions.items():
        bottom = y + _NODE_H + (_TRUNC_H if inv_id in trunc else 0)
        if bottom > max_bottom:
            max_bottom = bottom
    max_y = max_bottom + _PAD
    return max(300.0, max_cx), max_y


# -- connection rendering ------------------------------------------------------
def _render_connection(
    parent_cx: float,
    parent_y: float,
    child_cx: float,
    child_y: float,
) -> str:
    """Render a Bézier curve connecting parent to child.

    Adapts direction based on relative positions: vertical S-curves when
    the child is below, horizontal curves when on the same row.

    :param parent_cx: Parent horizontal centre.
    :param parent_y: Parent top Y.
    :param child_cx: Child horizontal centre.
    :param child_y: Child top Y.
    :return: SVG path element string.
    """
    stroke = 'fill="none" stroke="#78909c" stroke-width="2" opacity="0.7"'
    dot_r = 3
    parent_bottom = parent_y + _NODE_H
    child_top = child_y
    parts = []

    if child_top >= parent_bottom - 2:
        # Child is below parent: vertical S-curve from bottom to top
        mid_y = (parent_bottom + child_top) / 2
        parts.append(
            f'<path d="M{parent_cx:.1f},{parent_bottom:.1f} '
            f"C{parent_cx:.1f},{mid_y:.1f} "
            f"{child_cx:.1f},{mid_y:.1f} "
            f'{child_cx:.1f},{child_top:.1f}" {stroke}/>'
        )
        # Small dot at parent exit point
        parts.append(
            f'<circle cx="{parent_cx:.1f}" cy="{parent_bottom:.1f}" '
            f'r="{dot_r}" fill="#78909c" opacity="0.7"/>'
        )
    else:
        # Child is alongside or overlapping parent: route from side
        if child_cx > parent_cx:
            sx = parent_cx + _NODE_W / 2
            ex = child_cx - _NODE_W / 2
        else:
            sx = parent_cx - _NODE_W / 2
            ex = child_cx + _NODE_W / 2
        sy = parent_y + _NODE_H * 0.7
        ey = child_y + _NODE_H * 0.3
        mid_x = (sx + ex) / 2
        parts.append(
            f'<path d="M{sx:.1f},{sy:.1f} '
            f"C{mid_x:.1f},{sy:.1f} "
            f"{mid_x:.1f},{ey:.1f} "
            f'{ex:.1f},{ey:.1f}" {stroke}/>'
        )
        parts.append(
            f'<circle cx="{sx:.1f}" cy="{sy:.1f}" '
            f'r="{dot_r}" fill="#78909c" opacity="0.7"/>'
        )
    return "\n".join(parts)


def _render_node(
    node: FamilyTreeNode,
    cx: float,
    y: float,
    focus_id: str,
    base_url: str,
) -> str:
    """Build SVG markup for a single node box with 4 info lines.

    Lines: invocation ID (mono) | module.func (bold) | ISO datetime | elapsed.
    Truncated nodes get a dashed border hint and a separate "load more"
    badge rendered below the node (outside the clickable ``<a>``).

    :param node: The node to render.
    :param cx: Horizontal centre of the node.
    :param y: Top Y coordinate.
    :param focus_id: Invocation ID to highlight with a bold border.
    :param base_url: URL prefix for the invocation detail link.
    :return: SVG element string.
    """
    x = cx - _NODE_W / 2
    color = _status_color(node.status)
    bg = _status_tint(node.status)
    is_focus = node.invocation_id == focus_id
    stroke = (
        f'stroke="{color}" stroke-width="3"'
        if is_focus
        else 'stroke="#dee2e6" stroke-width="1"'
    )

    href = f"{base_url}/{node.invocation_id}"
    inv_id = _esc(node.invocation_id)
    mod_func = f"{_esc(node.module_name)}.{_esc(node.func_name)}"
    max_chars = _NODE_W // 7
    if len(mod_func) > max_chars:
        mod_func = mod_func[: max_chars - 1] + "\u2026"
    ts_str = (
        node.created_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] if node.created_at else ""
    )
    elapsed_str = _format_elapsed(getattr(node, "elapsed_seconds", None))
    if elapsed_str:
        elapsed_str = f"\u23f1 {elapsed_str}"

    sans = "font-family='-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif'"
    mono = "font-family='SFMono-Regular,Menlo,Monaco,Consolas,monospace'"

    focus_extra = ""
    if is_focus:
        focus_extra = (
            f'<rect x="{x - 3:.1f}" y="{y - 3:.1f}" '
            f'width="{_NODE_W + 6}" height="{_NODE_H + 6}" '
            f'rx="6" fill="none" stroke="{color}" stroke-width="2" '
            f'stroke-dasharray="6,3" opacity="0.6">'
            f'<animate attributeName="stroke-dashoffset" '
            f'values="0;18" dur="1.5s" repeatCount="indefinite"/>'
            f"</rect>"
        )

    # Truncated nodes get a dashed border hint (but keep ft-node class)
    is_truncated = getattr(node, "truncated", False)
    if is_truncated and not is_focus:
        stroke = 'stroke="#90a4ae" stroke-width="1.5" stroke-dasharray="6,3"'

    node_markup = (
        f'<a href="{href}" class="ft-node" data-inv-id="{node.invocation_id}" '
        f'data-status="{node.status}">'
        f"{focus_extra}"
        f'<rect x="{x:.1f}" y="{y:.1f}" width="{_NODE_W}" height="{_NODE_H}" '
        f'rx="4" fill="{bg}" {stroke}/>'
        f'<rect x="{x:.1f}" y="{y:.1f}" width="4" height="{_NODE_H}" '
        f'rx="2" fill="{color}"/>'
        # Line 1: invocation ID (monospace)
        f'<text x="{cx:.1f}" y="{y + 14:.1f}" text-anchor="middle" '
        f'font-size="9.5" {mono} fill="#495057">{inv_id}</text>'
        # Line 2: module.func_name (bold)
        f'<text x="{cx:.1f}" y="{y + 29:.1f}" text-anchor="middle" '
        f'font-size="11" {sans} fill="#212529" font-weight="600">{mod_func}</text>'
        # Line 3: ISO datetime
        f'<text x="{cx:.1f}" y="{y + 43:.1f}" text-anchor="middle" '
        f'font-size="9" {sans} fill="#6c757d">{ts_str}</text>'
        # Line 4: elapsed time
        f'<text x="{cx:.1f}" y="{y + 56:.1f}" text-anchor="middle" '
        f'font-size="9" {sans} fill="{color}">{elapsed_str}</text>'
        f"</a>"
    )

    # "Load more" badge — separate from the node <a> so clicks are distinct
    load_more = ""
    if is_truncated:
        badge_y = y + _NODE_H + 4
        badge_w = 110
        badge_x = cx - badge_w / 2
        load_more = (
            f'<g class="ft-load-more" data-expand-id="{node.invocation_id}" '
            f'style="cursor:pointer">'
            f'<rect x="{badge_x:.1f}" y="{badge_y:.1f}" '
            f'width="{badge_w}" height="{_TRUNC_H - 4}" '
            f'rx="10" fill="#f0f4f8" stroke="#90caf9" stroke-width="1"/>'
            f'<text x="{cx:.1f}" y="{badge_y + 13:.1f}" text-anchor="middle" '
            f'font-size="10" {sans} fill="#1976d2">'
            f"\u25bc load more</text>"
            f"</g>"
        )

    return node_markup + load_more


def _collect_elements(
    root: FamilyTreeNode,
    x_positions: dict[str, float],
    y_positions: dict[str, float],
    ctx: _RenderCtx,
) -> None:
    """Iteratively collect connection and node SVG elements for the tree.

    :param root: Root node.
    :param x_positions: Horizontal layout.
    :param y_positions: Vertical layout.
    :param ctx: Render context holding options and accumulators.
    """
    stack = [root]
    while stack:
        node = stack.pop()
        cx = x_positions[node.invocation_id]
        y = y_positions[node.invocation_id]
        ctx.nodes.append(_render_node(node, cx, y, ctx.focus_id, ctx.base_url))
        for child in node.children:
            child_cx = x_positions[child.invocation_id]
            child_y = y_positions[child.invocation_id]
            ctx.edges.append(_render_connection(cx, y, child_cx, child_y))
            stack.append(child)


# -- public interface ----------------------------------------------------------
def render_family_tree_svg(
    root: FamilyTreeNode,
    focus_id: str,
    base_url: str = "/invocations",
) -> str:
    """Render the family tree as an inline SVG string.

    Uses a time-ordered grid layout: nodes sorted by ``created_at`` are
    packed into columns.  Bézier curves connect parents to children.
    The SVG uses its natural pixel dimensions; the container handles
    scrolling and zoom.

    :param root: Root of the tree.
    :param focus_id: Invocation ID to highlight as the focused node.
    :param base_url: URL prefix for node detail links.
    :return: Inline SVG markup.
    """
    x_positions, y_positions = _assign_positions(root)

    if not x_positions:
        return '<svg xmlns="http://www.w3.org/2000/svg"><text>No data</text></svg>'

    # Collect truncated node IDs for viewport calculation
    truncated_ids: set[str] = set()
    stack = [root]
    while stack:
        n = stack.pop()
        if getattr(n, "truncated", False):
            truncated_ids.add(n.invocation_id)
        for c in n.children:
            stack.append(c)

    svg_w, svg_h = _svg_viewport(x_positions, y_positions, truncated_ids)
    ctx = _RenderCtx(focus_id=focus_id, base_url=base_url)
    _collect_elements(root, x_positions, y_positions, ctx)

    # Nodes first, then edges on top so connections are always visible
    inner = "\n".join(ctx.nodes + ctx.edges)

    return (
        f'<svg id="ft-svg" xmlns="http://www.w3.org/2000/svg" '
        f'width="{svg_w:.0f}" height="{svg_h:.0f}" '
        f'viewBox="0 0 {svg_w:.0f} {svg_h:.0f}" '
        f'style="cursor:grab">'
        f'<g id="ft-group">{inner}</g></svg>'
    )
