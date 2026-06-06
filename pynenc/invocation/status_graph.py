"""
Render the invocation status state machine from the status configuration.

Key components:
- Layout search: ranks statuses from live transitions and chooses compact rows.
- Straight-edge clipping: attaches arrow endpoints to node boundaries.
- SVG renderer: draws styled nodes, transitions, and legend from status metadata.
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from functools import cache
from html import escape
from itertools import combinations, permutations
from math import hypot
from pathlib import Path
from typing import Final
from collections.abc import Mapping

from pynenc.invocation.status import InvocationStatus, StatusDefinition, _CONFIG

DEFAULT_OUTPUT_PATH: Final[Path] = (
    Path(__file__).resolve().parents[2]
    / "docs"
    / "_static"
    / "invocation_state_machine.svg"
)

NodeKey = InvocationStatus | str

CANVAS_WIDTH: Final[int] = 1180
CANVAS_HEIGHT: Final[int] = 620
START_X: Final[int] = 58
STATUS_LEFT_X: Final[int] = 210
STATUS_RIGHT_X: Final[int] = 1050
GRAPH_TOP: Final[int] = 115
GRAPH_BOTTOM: Final[int] = 485
MAIN_LANE_Y: Final[int] = 300
LEGEND_TITLE_Y: Final[int] = 555
LEGEND_BASELINE_Y: Final[int] = 575
NODE_WIDTH: Final[int] = 176
NODE_HEIGHT: Final[int] = 58
NODE_GAP: Final[int] = 16
START_RADIUS: Final[int] = 22
RELAXATION_PASSES: Final[int] = 8
ANCHOR_FRACTIONS: Final[tuple[float, ...]] = (0.0, 0.25, 0.5, 0.75, 1.0)

PALETTE: Final[dict[str, tuple[str, str, str]]] = {
    "start": ("#f7f1ec", "#b95a3d", "#4f3228"),
    "available": ("#eaf7f0", "#39835a", "#244c36"),
    "owned": ("#fff3e6", "#c66a2e", "#5c351f"),
    "recovery": ("#edf4ff", "#4f75c8", "#273d70"),
    "concurrency": ("#fff0f4", "#c7506e", "#6c2b3d"),
    "final": ("#f1f3f7", "#677084", "#333b4a"),
}


@dataclass(frozen=True)
class Layout:
    """Computed geometry for the status graph."""

    positions: dict[NodeKey, tuple[float, float]]
    width: int = CANVAS_WIDTH
    height: int = CANVAS_HEIGHT


@dataclass(frozen=True)
class Line:
    """Straight edge segment after clipping to source and target nodes."""

    start_x: float
    start_y: float
    end_x: float
    end_y: float


def render_svg() -> str:
    """Return the SVG representation generated from the current status config."""
    layout = _layout()
    parts = [_svg_header(layout), _defs()]
    parts.extend(
        _render_edge(layout, source, target) for source, target in _iter_edges()
    )
    parts.extend(_render_node(layout, node) for node in layout.positions)
    parts.append(_render_legend(layout))
    parts.append("</svg>\n")
    return "".join(parts)


def render_text() -> str:
    """Return a plain-text representation of the invocation status graph."""
    parts = [
        "Invocation status state machine",
        "Generated from pynenc.invocation.status",
        "",
        "Transitions:",
    ]
    for source, target in _iter_edges():
        parts.append(f"  {_node_name(source)} -> {target.name}")
    parts.extend(["", "Statuses:"])
    for status in InvocationStatus:
        definition = _CONFIG.get_definition(status)
        parts.append(f"  {status.name}: {_badge_for(definition)}")
    return "\n".join(parts) + "\n"


@cache
def _layout() -> Layout:
    """Compute and validate the compact graph layout."""
    edges = _iter_edges()
    ranks = _refine_ranks(_rank_nodes(edges), edges)
    columns = _columns_by_rank(ranks)
    lanes = _lane_positions(max(len(column) for column in columns.values()))
    x_positions = _x_positions(max(columns))

    last_error: ValueError | None = None
    for rows in _relaxed_row_sets(columns, lanes, edges):
        layout = Layout(_positions_from_rows(rows, x_positions, ranks))
        try:
            _validate_layout(layout)
        except ValueError as error:
            last_error = error
        else:
            return layout
    raise ValueError("Could not compute invocation status graph layout") from last_error


def _rank_nodes(edges: list[tuple[NodeKey, InvocationStatus]]) -> dict[NodeKey, int]:
    """Rank nodes by shortest transition distance from START."""
    adjacency: dict[NodeKey, list[InvocationStatus]] = defaultdict(list)
    for source, target in edges:
        adjacency[source].append(target)

    ranks: dict[NodeKey, int] = {"START": 0}
    queue: deque[NodeKey] = deque(["START"])
    while queue:
        source = queue.popleft()
        for target in adjacency[source]:
            if target not in ranks:
                ranks[target] = ranks[source] + 1
                queue.append(target)

    fallback_rank = max(ranks.values(), default=0) + 1
    for status in InvocationStatus:
        ranks.setdefault(status, fallback_rank)
    return ranks


def _refine_ranks(
    ranks: dict[NodeKey, int], edges: list[tuple[NodeKey, InvocationStatus]]
) -> dict[NodeKey, int]:
    """Refine shortest-path ranks using ownership semantics from the graph."""
    refined = ranks.copy()
    entry_statuses = _CONFIG.definitions[None].allowed_transitions
    for status in InvocationStatus:
        definition = _CONFIG.get_definition(status)
        if status not in entry_statuses and definition.available_for_run:
            owned_targets = [
                target
                for target in definition.allowed_transitions
                if _CONFIG.get_definition(target).acquires_ownership
            ]
            if owned_targets:
                refined[status] = min(refined[target] for target in owned_targets)
    return refined


def _columns_by_rank(
    ranks: dict[NodeKey, int],
) -> dict[int, tuple[InvocationStatus, ...]]:
    """Group statuses into ranked columns."""
    columns: dict[int, list[InvocationStatus]] = defaultdict(list)
    for node, rank in ranks.items():
        if isinstance(node, InvocationStatus):
            columns[rank].append(node)
    return {
        rank: tuple(sorted(statuses, key=lambda status: status.name))
        for rank, statuses in columns.items()
    }


def _lane_positions(max_column_size: int) -> tuple[int, ...]:
    """Return evenly spaced y lanes for the densest computed column."""
    lane_count = max(6, max_column_size)
    if lane_count == 1:
        return (MAIN_LANE_Y,)
    step = (GRAPH_BOTTOM - GRAPH_TOP) / (lane_count - 1)
    return tuple(round(GRAPH_TOP + index * step) for index in range(lane_count))


def _x_positions(max_rank: int) -> dict[int, float]:
    """Return x coordinate per status rank."""
    if max_rank <= 1:
        return {1: STATUS_LEFT_X}
    step = (STATUS_RIGHT_X - STATUS_LEFT_X) / (max_rank - 1)
    return {
        rank: STATUS_LEFT_X + ((rank - 1) * step) for rank in range(1, max_rank + 1)
    }


def _relaxed_row_sets(
    columns: dict[int, tuple[InvocationStatus, ...]],
    lanes: tuple[int, ...],
    edges: list[tuple[NodeKey, InvocationStatus]],
) -> list[dict[InvocationStatus, int]]:
    """Return deterministic row targets after neighbor relaxation sweeps."""
    row_sets: list[dict[InvocationStatus, int]] = []
    for seed in _seed_rows(columns, lanes):
        rows = seed.copy()
        row_sets.append(rows.copy())
        for _ in range(RELAXATION_PASSES):
            for rank_order in (sorted(columns), sorted(columns, reverse=True)):
                for rank in rank_order:
                    targets = _neighbor_targets(columns[rank], rows, edges)
                    rows.update(_best_column_rows(columns[rank], lanes, targets))
            row_sets.append(rows.copy())
    return row_sets


def _seed_rows(
    columns: dict[int, tuple[InvocationStatus, ...]], lanes: tuple[int, ...]
) -> list[dict[InvocationStatus, int]]:
    """Return row seeds from semantic, inverted, and centered preferences."""
    seeds: list[dict[InvocationStatus, int]] = []
    for target_factory in (
        _preferred_lane,
        lambda status: GRAPH_TOP + GRAPH_BOTTOM - _preferred_lane(status),
        lambda status: MAIN_LANE_Y,
    ):
        rows: dict[InvocationStatus, int] = {}
        for column in columns.values():
            targets = {status: target_factory(status) for status in column}
            rows.update(_best_column_rows(column, lanes, targets))
        seeds.append(rows)
    return seeds


def _neighbor_targets(
    column: tuple[InvocationStatus, ...],
    rows: dict[InvocationStatus, int],
    edges: list[tuple[NodeKey, InvocationStatus]],
) -> dict[InvocationStatus, float]:
    """Return row targets from connected statuses plus semantic bias."""
    targets: dict[InvocationStatus, float] = {}
    for status in column:
        connected_rows: list[int] = []
        for source, target in edges:
            if source == status and target in rows:
                connected_rows.append(rows[target])
            elif target == status and isinstance(source, InvocationStatus):
                connected_rows.append(rows[source])
        preference = _preferred_lane(status)
        targets[status] = (sum(connected_rows) + preference) / (len(connected_rows) + 1)
    return targets


def _best_column_rows(
    column: tuple[InvocationStatus, ...],
    lanes: tuple[int, ...],
    targets: Mapping[InvocationStatus, float],
) -> dict[InvocationStatus, int]:
    """Return the best unique lane assignment for one column."""
    if len(column) == 1:
        status = column[0]
        lane = min(lanes, key=lambda value: abs(value - targets[status]))
        return {status: lane}

    best_score: float | None = None
    best_candidate: dict[InvocationStatus, int] | None = None
    for selected_lanes in combinations(lanes, len(column)):
        for ordered_statuses in permutations(column):
            candidate = dict(zip(ordered_statuses, selected_lanes, strict=True))
            score = sum(
                abs(candidate[status] - targets[status])
                + (abs(candidate[status] - _preferred_lane(status)) * 0.25)
                for status in column
            )
            if best_score is None or score < best_score:
                best_score = score
                best_candidate = candidate
    if best_candidate is None:
        raise ValueError("Could not assign graph lanes")
    return best_candidate


def _preferred_lane(status: InvocationStatus) -> int:
    """Place semantic outliers before the global crossing score takes over."""
    definition = _CONFIG.get_definition(status)
    if status in _CONFIG.definitions[None].allowed_transitions:
        return MAIN_LANE_Y
    if definition.available_for_run and definition.releases_ownership:
        if any(
            target.name.startswith("CONCURRENCY")
            for target in definition.allowed_transitions
        ):
            return GRAPH_TOP
        return GRAPH_BOTTOM
    if status.name.startswith("CONCURRENCY") and not definition.is_final:
        return GRAPH_TOP
    if definition.is_final:
        return GRAPH_BOTTOM if status.name.startswith("CONCURRENCY") else MAIN_LANE_Y
    if definition.overrides_ownership:
        if status.name.startswith("RUNNING"):
            return GRAPH_TOP
        return GRAPH_BOTTOM
    if definition.releases_ownership:
        return round((GRAPH_TOP + MAIN_LANE_Y) / 2)
    if definition.requires_ownership and not definition.acquires_ownership:
        return round((GRAPH_TOP + MAIN_LANE_Y) / 2)
    return MAIN_LANE_Y


def _positions_from_rows(
    rows: dict[InvocationStatus, int],
    x_positions: dict[int, float],
    ranks: dict[NodeKey, int],
) -> dict[NodeKey, tuple[float, float]]:
    """Build node positions from computed row assignments."""
    positions: dict[NodeKey, tuple[float, float]] = {}
    for status, y in rows.items():
        positions[status] = (x_positions[ranks[status]], y)
    registered_y = positions[InvocationStatus.REGISTERED][1]
    positions = {"START": (START_X, registered_y), **positions}
    return positions


def _validate_layout(layout: Layout) -> None:
    """Validate computed layout before rendering."""
    positioned_statuses = {
        node for node in layout.positions if isinstance(node, InvocationStatus)
    }
    missing_statuses = [
        status for status in InvocationStatus if status not in positioned_statuses
    ]
    if missing_statuses:
        missing_names = ", ".join(sorted(status.name for status in missing_statuses))
        raise ValueError(f"Missing diagram positions for: {missing_names}")

    positioned_nodes = list(layout.positions)
    for index, first in enumerate(positioned_nodes):
        for second in positioned_nodes[index + 1 :]:
            if _bounds_overlap(
                _node_bounds(layout.positions, first),
                _node_bounds(layout.positions, second),
                NODE_GAP,
            ):
                raise ValueError(
                    "Overlapping diagram nodes: "
                    f"{_node_name(first)} and {_node_name(second)}"
                )

    for source, target in _iter_edges():
        line = _edge_line(layout.positions, source, target)
        if blocker := _edge_crosses_non_endpoint_node(
            layout.positions, source, target, line
        ):
            source_name = _node_name(source)
            raise ValueError(
                "Diagram edge crosses another node: "
                f"{source_name}->{target.name} crosses {_node_name(blocker)}"
            )


def _node_bounds(
    positions: dict[NodeKey, tuple[float, float]], node: NodeKey
) -> tuple[float, float, float, float]:
    """Return node bounds."""
    x, y = positions[node]
    if node == "START":
        return x - START_RADIUS, y - START_RADIUS, x + START_RADIUS, y + START_RADIUS
    return (
        x - NODE_WIDTH / 2,
        y - NODE_HEIGHT / 2,
        x + NODE_WIDTH / 2,
        y + NODE_HEIGHT / 2,
    )


def _bounds_overlap(
    first: tuple[float, float, float, float],
    second: tuple[float, float, float, float],
    padding: int,
) -> bool:
    """Return whether padded bounds overlap."""
    first_left, first_top, first_right, first_bottom = first
    second_left, second_top, second_right, second_bottom = second
    return not (
        first_right + padding <= second_left
        or second_right + padding <= first_left
        or first_bottom + padding <= second_top
        or second_bottom + padding <= first_top
    )


def _edge_crosses_non_endpoint_node(
    positions: dict[NodeKey, tuple[float, float]],
    source: NodeKey,
    target: InvocationStatus,
    line: Line,
) -> NodeKey | None:
    """Return the first non-endpoint node crossed by an edge."""
    source_name = _node_name(source)
    target_name = target.name
    for node in positions:
        node_name = _node_name(node)
        if node_name in {source_name, target_name}:
            continue
        if _segment_intersects_bounds(line, _node_bounds(positions, node)):
            return node
    return None


def _segment_intersects_bounds(
    line: Line, bounds: tuple[float, float, float, float]
) -> bool:
    """Return whether a line crosses rectangle bounds."""
    left, top, right, bottom = bounds
    if _point_inside_bounds(line.start_x, line.start_y, bounds):
        return True
    if _point_inside_bounds(line.end_x, line.end_y, bounds):
        return True
    bound_segments = (
        Line(left, top, right, top),
        Line(right, top, right, bottom),
        Line(right, bottom, left, bottom),
        Line(left, bottom, left, top),
    )
    return any(_segments_intersect(line, segment) for segment in bound_segments)


def _point_inside_bounds(
    point_x: float, point_y: float, bounds: tuple[float, float, float, float]
) -> bool:
    """Return whether a point lies inside bounds."""
    left, top, right, bottom = bounds
    return left < point_x < right and top < point_y < bottom


def _segments_intersect(first: Line, second: Line) -> bool:
    """Return whether two line segments intersect."""
    first_orientation = _orientation(first, second.start_x, second.start_y)
    second_orientation = _orientation(first, second.end_x, second.end_y)
    third_orientation = _orientation(second, first.start_x, first.start_y)
    fourth_orientation = _orientation(second, first.end_x, first.end_y)
    if (
        first_orientation != second_orientation
        and third_orientation != fourth_orientation
    ):
        return True
    return (
        first_orientation == 0
        and _point_on_segment(first, second.start_x, second.start_y)
        or second_orientation == 0
        and _point_on_segment(first, second.end_x, second.end_y)
        or third_orientation == 0
        and _point_on_segment(second, first.start_x, first.start_y)
        or fourth_orientation == 0
        and _point_on_segment(second, first.end_x, first.end_y)
    )


def _orientation(line: Line, point_x: float, point_y: float) -> int:
    """Return point orientation relative to a line."""
    value = (line.end_y - line.start_y) * (point_x - line.end_x) - (
        line.end_x - line.start_x
    ) * (point_y - line.end_y)
    if abs(value) < 1e-9:
        return 0
    return 1 if value > 0 else 2


def _point_on_segment(line: Line, point_x: float, point_y: float) -> bool:
    """Return whether point lies on a segment."""
    return min(line.start_x, line.end_x) <= point_x <= max(
        line.start_x, line.end_x
    ) and min(line.start_y, line.end_y) <= point_y <= max(line.start_y, line.end_y)


def _iter_edges() -> list[tuple[NodeKey, InvocationStatus]]:
    """Return every rendered transition edge."""
    edges: list[tuple[NodeKey, InvocationStatus]] = [
        ("START", InvocationStatus.REGISTERED)
    ]
    for source, definition in sorted(
        _CONFIG.definitions.items(),
        key=lambda item: "" if item[0] is None else item[0].name,
    ):
        if source is None:
            continue
        for target in sorted(
            definition.allowed_transitions, key=lambda status: status.name
        ):
            edges.append((source, target))
    return edges


def _svg_header(layout: Layout) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{layout.width}" height="{layout.height}" '
        f'viewBox="0 0 {layout.width} {layout.height}" role="img" aria-labelledby="title desc">\n'
        '<title id="title">Pynenc invocation status state machine</title>\n'
        '<desc id="desc">Generated from pynenc.invocation.status. Shows allowed transitions, ownership, recovery, and final states.</desc>\n'
        f'<rect width="{layout.width}" height="{layout.height}" rx="18" fill="#fbf8f5"/>\n'
        '<text x="48" y="48" font-family="Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif" '
        'font-size="24" font-weight="700" fill="#2f221d">Invocation status state machine</text>\n'
        '<text x="48" y="74" font-family="Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif" '
        'font-size="13" fill="#735d52">Generated from pynenc.invocation.status</text>\n'
    )


def _defs() -> str:
    parts = [
        "<defs>\n"
        '<filter id="shadow" x="-15%" y="-25%" width="130%" height="160%">\n'
        '<feDropShadow dx="0" dy="6" stdDeviation="5" flood-color="#3a241a" flood-opacity="0.13"/>\n'
        "</filter>\n"
    ]
    for style, (_, stroke, _) in PALETTE.items():
        parts.append(
            f'<marker id="arrow-{style}" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto" markerUnits="strokeWidth">\n'
            f'<path d="M0,0 L0,6 L8,3 z" fill="{stroke}"/>\n'
            "</marker>\n"
        )
    parts.append("</defs>\n")
    return "".join(parts)


def _render_edge(layout: Layout, source: NodeKey, target: InvocationStatus) -> str:
    line = _edge_line(layout.positions, source, target)
    style = _style_for(target, _CONFIG.get_definition(target))
    _, stroke, _ = PALETTE[style]
    marker = f"url(#arrow-{style})"
    width = 1.85 if style in {"recovery", "concurrency"} else 1.65
    source_name = _node_name(source)
    return (
        f'<g data-edge="{source_name}->{target.name}">\n'
        f'<line x1="{line.start_x:.1f}" y1="{line.start_y:.1f}" x2="{line.end_x:.1f}" y2="{line.end_y:.1f}" '
        f'stroke="#fbf8f5" stroke-width="{width + 1.4}" '
        'stroke-linecap="round" opacity="0.65"/>\n'
        f'<line x1="{line.start_x:.1f}" y1="{line.start_y:.1f}" x2="{line.end_x:.1f}" y2="{line.end_y:.1f}" '
        f'stroke="{stroke}" stroke-width="{width}" '
        f'stroke-linecap="round" marker-end="{marker}" opacity="0.88"/>\n'
        "</g>\n"
    )


def _edge_line(
    positions: dict[NodeKey, tuple[float, float]],
    source: NodeKey,
    target: InvocationStatus,
) -> Line:
    """Return the best straight boundary-to-boundary edge."""
    best_score: tuple[float, ...] | None = None
    best_line: Line | None = None
    for start in _anchor_candidates(positions, source, target):
        for end in _anchor_candidates(positions, target, source):
            line = Line(*start, *end)
            if _line_length(line) < 1:
                continue
            if _edge_crosses_non_endpoint_node(positions, source, target, line):
                continue
            score = _anchor_line_score(positions, source, target, line)
            if best_score is None or score < best_score:
                best_score = score
                best_line = line
    if best_line is not None:
        return best_line
    raise ValueError(
        f"No straight edge available for {_node_name(source)}->{target.name}"
    )


def _anchor_candidates(
    positions: dict[NodeKey, tuple[float, float]], node: NodeKey, other: NodeKey
) -> tuple[tuple[float, float], ...]:
    """Return boundary anchors for a node, ordered toward the other node."""
    center_x, center_y = positions[node]
    other_x, other_y = positions[other]
    if node == "START":
        return (_clip_circle_endpoint((center_x, center_y), (other_x, other_y)),)

    left, top, right, bottom = _node_bounds(positions, node)
    anchors = (
        {(left, top + ((bottom - top) * fraction)) for fraction in ANCHOR_FRACTIONS}
        | {(right, top + ((bottom - top) * fraction)) for fraction in ANCHOR_FRACTIONS}
        | {(left + ((right - left) * fraction), top) for fraction in ANCHOR_FRACTIONS}
        | {
            (left + ((right - left) * fraction), bottom)
            for fraction in ANCHOR_FRACTIONS
        }
    )
    return tuple(
        sorted(
            anchors,
            key=lambda point: _anchor_direction_penalty(
                point, (center_x, center_y), (other_x, other_y)
            ),
        )
    )


def _anchor_direction_penalty(
    anchor: tuple[float, float], center: tuple[float, float], other: tuple[float, float]
) -> float:
    """Score whether an anchor faces the other endpoint."""
    anchor_x, anchor_y = anchor
    center_x, center_y = center
    other_x, other_y = other
    anchor_vector = anchor_x - center_x, anchor_y - center_y
    other_vector = other_x - center_x, other_y - center_y
    dot = (anchor_vector[0] * other_vector[0]) + (anchor_vector[1] * other_vector[1])
    return -dot


def _anchor_line_score(
    positions: dict[NodeKey, tuple[float, float]],
    source: NodeKey,
    target: InvocationStatus,
    line: Line,
) -> tuple[float, ...]:
    """Score a valid anchor line by direction, length, and straightness."""
    source_penalty = _anchor_direction_penalty(
        (line.start_x, line.start_y), positions[source], positions[target]
    )
    target_penalty = _anchor_direction_penalty(
        (line.end_x, line.end_y), positions[target], positions[source]
    )
    return (
        source_penalty + target_penalty,
        abs(line.end_y - line.start_y),
        _line_length(line),
    )


def _clip_circle_endpoint(
    center: tuple[float, float], target: tuple[float, float]
) -> tuple[float, float]:
    """Clip START edge to its circular boundary."""
    center_x, center_y = center
    target_x, target_y = target
    length = hypot(target_x - center_x, target_y - center_y) or 1.0
    return (
        center_x + ((target_x - center_x) / length * START_RADIUS),
        center_y + ((target_y - center_y) / length * START_RADIUS),
    )


def _line_length(line: Line) -> float:
    """Return the length of a line."""
    return hypot(line.end_x - line.start_x, line.end_y - line.start_y)


def _render_node(layout: Layout, node: NodeKey) -> str:
    x, y = layout.positions[node]
    if node == "START":
        fill, stroke, text = PALETTE["start"]
        return (
            f'<circle cx="{x:.0f}" cy="{y:.0f}" r="22" fill="{fill}" stroke="{stroke}" stroke-width="2" filter="url(#shadow)"/>\n'
            f'<text x="{x:.0f}" y="{y + 4:.0f}" text-anchor="middle" font-family="Inter, ui-sans-serif, system-ui" '
            f'font-size="11" font-weight="700" fill="{text}">START</text>\n'
        )
    if not isinstance(node, InvocationStatus):
        raise TypeError(f"Unsupported diagram node: {node}")

    definition = _CONFIG.get_definition(node)
    style = _style_for(node, definition)
    fill, stroke, text_color = PALETTE[style]
    rect_x = x - NODE_WIDTH // 2
    rect_y = y - NODE_HEIGHT // 2
    badge = _badge_for(definition)
    title_lines = _split_label(node.name)
    title_y = y - 7 if len(title_lines) == 2 else y + 2
    parts = [
        f'<g data-status="{node.name}">\n',
        f'<rect x="{rect_x:.0f}" y="{rect_y:.0f}" width="{NODE_WIDTH}" height="{NODE_HEIGHT}" rx="10" '
        f'fill="{fill}" stroke="{stroke}" stroke-width="2" filter="url(#shadow)"/>\n',
    ]
    if definition.is_final:
        parts.append(
            f'<rect x="{rect_x + 5:.0f}" y="{rect_y + 5:.0f}" width="{NODE_WIDTH - 10}" height="{NODE_HEIGHT - 10}" rx="7" '
            f'fill="none" stroke="{stroke}" stroke-width="1.2" opacity="0.55"/>\n'
        )
    for index, line in enumerate(title_lines):
        parts.append(
            f'<text x="{x:.0f}" y="{title_y + (index * 15):.0f}" text-anchor="middle" '
            'font-family="Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif" '
            f'font-size="13" font-weight="700" fill="{text_color}">{escape(line)}</text>\n'
        )
    parts.append(
        f'<text x="{x:.0f}" y="{y + 22:.0f}" text-anchor="middle" '
        'font-family="Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif" '
        f'font-size="10" fill="{text_color}" opacity="0.78">{escape(badge)}</text>\n'
    )
    parts.append("</g>\n")
    return "".join(parts)


def _style_for(status: InvocationStatus, definition: StatusDefinition) -> str:
    if definition.is_final:
        return "final"
    if definition.overrides_ownership or status.name.endswith("_RECOVERY"):
        return "recovery"
    if status.name.startswith("CONCURRENCY"):
        return "concurrency"
    if definition.requires_ownership or definition.acquires_ownership:
        return "owned"
    if definition.available_for_run:
        return "available"
    return "start"


def _badge_for(definition: StatusDefinition) -> str:
    labels: list[str] = []
    if definition.available_for_run:
        labels.append("available")
    if definition.requires_ownership:
        labels.append("owned")
    if definition.overrides_ownership:
        labels.append("recovery")
    if definition.is_final:
        labels.append("final")
    if definition.releases_ownership and not definition.is_final:
        labels.append("releases owner")
    return " / ".join(labels) if labels else "transition"


def _split_label(label: str) -> list[str]:
    if len(label) <= 16:
        return [label]
    parts = label.split("_")
    middle = max(1, len(parts) // 2)
    return ["_".join(parts[:middle]), "_".join(parts[middle:])]


def _render_legend(layout: Layout) -> str:
    items = [
        ("available", "available for runners"),
        ("owned", "runner-owned"),
        ("recovery", "recovery override"),
        ("concurrency", "concurrency control"),
        ("final", "final status"),
    ]
    item_step = (layout.width - 96) / len(items)
    parts = [
        f'<text x="48" y="{LEGEND_TITLE_Y}" font-family="Inter, ui-sans-serif, system-ui" font-size="13" font-weight="700" fill="#4f3228">Legend</text>\n'
    ]
    for index, (style, label) in enumerate(items):
        fill, stroke, text = PALETTE[style]
        item_x = 48 + (index * item_step)
        parts.append(
            f'<rect x="{item_x:.0f}" y="{LEGEND_BASELINE_Y - 13}" width="28" height="18" rx="5" fill="{fill}" stroke="{stroke}" stroke-width="1.5"/>\n'
            f'<text x="{item_x + 38:.0f}" y="{LEGEND_BASELINE_Y}" font-family="Inter, ui-sans-serif, system-ui" font-size="12" fill="{text}">{label}</text>\n'
        )
    return "".join(parts)


def _node_name(node: NodeKey) -> str:
    return node.name if isinstance(node, InvocationStatus) else node
