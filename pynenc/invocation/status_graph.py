"""Render the invocation status state machine from the status configuration."""

from __future__ import annotations

from dataclasses import dataclass
from html import escape
from pathlib import Path
from typing import Final, Literal

from pynenc.invocation.status import InvocationStatus, StatusDefinition, _CONFIG

DEFAULT_OUTPUT_PATH: Final[Path] = (
    Path(__file__).resolve().parents[2]
    / "docs"
    / "_static"
    / "invocation_state_machine.svg"
)

NodeKey = InvocationStatus | str
Side = Literal["left", "right", "top", "bottom"]

NODE_WIDTH: Final[int] = 176
NODE_HEIGHT: Final[int] = 58
NODE_GAP: Final[int] = 16

POSITIONS: Final[dict[NodeKey, tuple[int, int]]] = {
    "START": (70, 300),
    InvocationStatus.REGISTERED: (250, 300),
    InvocationStatus.CONCURRENCY_CONTROLLED: (470, 125),
    InvocationStatus.REROUTED: (690, 125),
    InvocationStatus.PAUSED: (930, 125),
    InvocationStatus.RESUMED: (1160, 125),
    InvocationStatus.PENDING: (470, 300),
    InvocationStatus.RUNNING: (700, 300),
    InvocationStatus.SUCCESS: (1430, 235),
    InvocationStatus.FAILED: (1430, 385),
    InvocationStatus.CONCURRENCY_CONTROLLED_FINAL: (250, 485),
    InvocationStatus.RETRY: (700, 485),
    InvocationStatus.KILLED: (1160, 485),
    InvocationStatus.PENDING_RECOVERY: (470, 610),
    InvocationStatus.RUNNING_RECOVERY: (930, 610),
}

PALETTE: Final[dict[str, tuple[str, str, str]]] = {
    "start": ("#f7f1ec", "#b95a3d", "#4f3228"),
    "available": ("#eaf7f0", "#39835a", "#244c36"),
    "owned": ("#fff3e6", "#c66a2e", "#5c351f"),
    "recovery": ("#edf4ff", "#4f75c8", "#273d70"),
    "concurrency": ("#fff0f4", "#c7506e", "#6c2b3d"),
    "final": ("#f1f3f7", "#677084", "#333b4a"),
}


@dataclass(frozen=True)
class RouteSpec:
    start_side: Side
    end_side: Side
    start_offset: int = 0
    end_offset: int = 0


S = InvocationStatus

EDGE_ROUTES: Final[dict[tuple[str, str], RouteSpec]] = {
    ("START", S.REGISTERED.name): RouteSpec("right", "left"),
    (S.REGISTERED.name, S.CONCURRENCY_CONTROLLED.name): RouteSpec(
        "right", "left", -22, 18
    ),
    (S.REGISTERED.name, S.PENDING.name): RouteSpec("right", "left"),
    (S.REGISTERED.name, S.CONCURRENCY_CONTROLLED_FINAL.name): RouteSpec(
        "bottom", "top"
    ),
    (S.CONCURRENCY_CONTROLLED.name, S.REROUTED.name): RouteSpec(
        "right", "left", -10, -10
    ),
    (S.REROUTED.name, S.CONCURRENCY_CONTROLLED.name): RouteSpec(
        "left", "right", 10, 10
    ),
    (S.PENDING.name, S.REROUTED.name): RouteSpec("top", "bottom", 48, -44),
    (S.REROUTED.name, S.PENDING.name): RouteSpec("bottom", "top", -48, 44),
    (S.PENDING.name, S.RUNNING.name): RouteSpec("right", "left"),
    (S.PENDING.name, S.PENDING_RECOVERY.name): RouteSpec("bottom", "top", -42, -42),
    (S.PENDING.name, S.KILLED.name): RouteSpec("bottom", "left", 54, 18),
    (S.PENDING_RECOVERY.name, S.REROUTED.name): RouteSpec("top", "bottom", 84, -84),
    (S.RUNNING.name, S.PAUSED.name): RouteSpec("top", "bottom", 48, -48),
    (S.RUNNING.name, S.SUCCESS.name): RouteSpec("right", "left", -24, -20),
    (S.RUNNING.name, S.RETRY.name): RouteSpec("bottom", "top", -38, 40),
    (S.RUNNING.name, S.FAILED.name): RouteSpec("right", "left", 24, 20),
    (S.RUNNING.name, S.KILLED.name): RouteSpec("bottom", "left", 52, -18),
    (S.RUNNING.name, S.RUNNING_RECOVERY.name): RouteSpec("bottom", "left", 64, -20),
    (S.RUNNING_RECOVERY.name, S.REROUTED.name): RouteSpec("left", "right", 0, 18),
    (S.PAUSED.name, S.RESUMED.name): RouteSpec("right", "left", -10, -10),
    (S.RESUMED.name, S.PAUSED.name): RouteSpec("left", "right", 10, 10),
    (S.PAUSED.name, S.KILLED.name): RouteSpec("bottom", "top", 52, 42),
    (S.RESUMED.name, S.SUCCESS.name): RouteSpec("right", "left", -24, -20),
    (S.RESUMED.name, S.FAILED.name): RouteSpec("right", "left", 24, 20),
    (S.RESUMED.name, S.RETRY.name): RouteSpec("bottom", "right", -46, 18),
    (S.RESUMED.name, S.KILLED.name): RouteSpec("bottom", "top", 28, -28),
    (S.RETRY.name, S.PENDING.name): RouteSpec("left", "bottom", -18, 8),
    (S.KILLED.name, S.REROUTED.name): RouteSpec("left", "right", 18, 18),
}


def render_svg() -> str:
    """Return the SVG representation generated from the current status config."""
    _validate_layout()
    edges = list(_iter_edges())
    parts = [_svg_header(), _defs()]
    parts.extend(_render_edge(source, target) for source, target in edges)
    parts.extend(_render_node(node) for node in POSITIONS)
    parts.append(_render_legend())
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


def _validate_layout() -> None:
    positioned_statuses: set[InvocationStatus] = {
        node for node in POSITIONS if isinstance(node, InvocationStatus)
    }
    missing_statuses = [
        status for status in InvocationStatus if status not in positioned_statuses
    ]
    if missing_statuses:
        missing_names = ", ".join(sorted(status.name for status in missing_statuses))
        raise ValueError(f"Missing diagram positions for: {missing_names}")

    positioned_nodes = list(POSITIONS)
    for index, first in enumerate(positioned_nodes):
        for second in positioned_nodes[index + 1 :]:
            if _bounds_overlap(_node_bounds(first), _node_bounds(second), NODE_GAP):
                raise ValueError(
                    "Overlapping diagram nodes: "
                    f"{_node_name(first)} and {_node_name(second)}"
                )

    for source, target in _iter_edges():
        if blocker := _edge_crosses_non_endpoint_node(source, target):
            source_name = _node_name(source)
            raise ValueError(
                "Diagram edge crosses another node: "
                f"{source_name}->{target.name} crosses {_node_name(blocker)}"
            )


def _node_bounds(node: NodeKey) -> tuple[float, float, float, float]:
    x, y = POSITIONS[node]
    if node == "START":
        radius = 22
        return x - radius, y - radius, x + radius, y + radius
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
    first_left, first_top, first_right, first_bottom = first
    second_left, second_top, second_right, second_bottom = second
    return not (
        first_right + padding <= second_left
        or second_right + padding <= first_left
        or first_bottom + padding <= second_top
        or second_bottom + padding <= first_top
    )


def _edge_crosses_non_endpoint_node(
    source: NodeKey, target: InvocationStatus
) -> NodeKey | None:
    source_name = _node_name(source)
    target_name = target.name
    start_x, start_y, end_x, end_y = _edge_points(source, target)
    for node in POSITIONS:
        node_name = _node_name(node)
        if node_name in {source_name, target_name}:
            continue
        if _segment_intersects_bounds(
            start_x, start_y, end_x, end_y, _node_bounds(node)
        ):
            return node
    return None


def _segment_intersects_bounds(
    start_x: float,
    start_y: float,
    end_x: float,
    end_y: float,
    bounds: tuple[float, float, float, float],
) -> bool:
    left, top, right, bottom = bounds
    if _point_inside_bounds(start_x, start_y, bounds) or _point_inside_bounds(
        end_x, end_y, bounds
    ):
        return True
    bound_segments = (
        (left, top, right, top),
        (right, top, right, bottom),
        (right, bottom, left, bottom),
        (left, bottom, left, top),
    )
    return any(
        _segments_intersect(start_x, start_y, end_x, end_y, *bound_segment)
        for bound_segment in bound_segments
    )


def _point_inside_bounds(
    point_x: float, point_y: float, bounds: tuple[float, float, float, float]
) -> bool:
    left, top, right, bottom = bounds
    return left < point_x < right and top < point_y < bottom


def _segments_intersect(
    first_start_x: float,
    first_start_y: float,
    first_end_x: float,
    first_end_y: float,
    second_start_x: float,
    second_start_y: float,
    second_end_x: float,
    second_end_y: float,
) -> bool:
    first_orientation = _orientation(
        first_start_x,
        first_start_y,
        first_end_x,
        first_end_y,
        second_start_x,
        second_start_y,
    )
    second_orientation = _orientation(
        first_start_x,
        first_start_y,
        first_end_x,
        first_end_y,
        second_end_x,
        second_end_y,
    )
    third_orientation = _orientation(
        second_start_x,
        second_start_y,
        second_end_x,
        second_end_y,
        first_start_x,
        first_start_y,
    )
    fourth_orientation = _orientation(
        second_start_x,
        second_start_y,
        second_end_x,
        second_end_y,
        first_end_x,
        first_end_y,
    )
    if (
        first_orientation != second_orientation
        and third_orientation != fourth_orientation
    ):
        return True
    return (
        first_orientation == 0
        and _point_on_segment(
            first_start_x,
            first_start_y,
            second_start_x,
            second_start_y,
            first_end_x,
            first_end_y,
        )
        or second_orientation == 0
        and _point_on_segment(
            first_start_x,
            first_start_y,
            second_end_x,
            second_end_y,
            first_end_x,
            first_end_y,
        )
        or third_orientation == 0
        and _point_on_segment(
            second_start_x,
            second_start_y,
            first_start_x,
            first_start_y,
            second_end_x,
            second_end_y,
        )
        or fourth_orientation == 0
        and _point_on_segment(
            second_start_x,
            second_start_y,
            first_end_x,
            first_end_y,
            second_end_x,
            second_end_y,
        )
    )


def _orientation(
    start_x: float,
    start_y: float,
    end_x: float,
    end_y: float,
    point_x: float,
    point_y: float,
) -> int:
    value = (end_y - start_y) * (point_x - end_x) - (end_x - start_x) * (
        point_y - end_y
    )
    if abs(value) < 1e-9:
        return 0
    return 1 if value > 0 else 2


def _point_on_segment(
    start_x: float,
    start_y: float,
    point_x: float,
    point_y: float,
    end_x: float,
    end_y: float,
) -> bool:
    return min(start_x, end_x) <= point_x <= max(start_x, end_x) and min(
        start_y, end_y
    ) <= point_y <= max(start_y, end_y)


def _iter_edges() -> list[tuple[NodeKey, InvocationStatus]]:
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


def _svg_header() -> str:
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" width="1600" height="760" '
        'viewBox="0 0 1600 760" role="img" aria-labelledby="title desc">\n'
        '<title id="title">Pynenc invocation status state machine</title>\n'
        '<desc id="desc">Generated from pynenc.invocation.status. Shows allowed transitions, ownership, recovery, and final states.</desc>\n'
        '<rect width="1600" height="760" rx="18" fill="#fbf8f5"/>\n'
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


def _render_edge(source: NodeKey, target: InvocationStatus) -> str:
    start_x, start_y, end_x, end_y = _edge_points(source, target)
    style = _style_for(target, _CONFIG.get_definition(target))
    _, stroke, _ = PALETTE[style]
    marker = f"url(#arrow-{style})"
    width = 1.85 if style in {"recovery", "concurrency"} else 1.65
    source_name = _node_name(source)
    return (
        f'<g data-edge="{source_name}->{target.name}">\n'
        f'<line x1="{start_x:.1f}" y1="{start_y:.1f}" x2="{end_x:.1f}" y2="{end_y:.1f}" '
        f'stroke="#fbf8f5" stroke-width="{width + 1.4}" '
        'stroke-linecap="round" opacity="0.65"/>\n'
        f'<line x1="{start_x:.1f}" y1="{start_y:.1f}" x2="{end_x:.1f}" y2="{end_y:.1f}" '
        f'stroke="{stroke}" stroke-width="{width}" '
        f'stroke-linecap="round" marker-end="{marker}" opacity="0.88"/>\n'
        "</g>\n"
    )


def _edge_points(
    source: NodeKey, target: InvocationStatus
) -> tuple[float, float, float, float]:
    spec = EDGE_ROUTES.get(
        (_node_name(source), target.name), _default_route(source, target)
    )
    start_x, start_y = _anchor(source, spec.start_side, spec.start_offset)
    end_x, end_y = _anchor(target, spec.end_side, spec.end_offset)
    return start_x, start_y, end_x, end_y


def _default_route(source: NodeKey, target: InvocationStatus) -> RouteSpec:
    source_x, source_y = POSITIONS[source]
    target_x, target_y = POSITIONS[target]
    if abs(target_x - source_x) >= abs(target_y - source_y):
        if target_x >= source_x:
            return RouteSpec("right", "left")
        return RouteSpec("left", "right")
    if target_y >= source_y:
        return RouteSpec("bottom", "top")
    return RouteSpec("top", "bottom")


def _anchor(node: NodeKey, side: Side, offset: int) -> tuple[float, float]:
    x, y = POSITIONS[node]
    if node == "START":
        radius = 22
        if side == "right":
            return x + radius, y + offset
        if side == "left":
            return x - radius, y + offset
        if side == "top":
            return x + offset, y - radius
        return x + offset, y + radius

    if side == "right":
        return x + NODE_WIDTH / 2, y + offset
    if side == "left":
        return x - NODE_WIDTH / 2, y + offset
    if side == "top":
        return x + offset, y - NODE_HEIGHT / 2
    return x + offset, y + NODE_HEIGHT / 2


def _render_node(node: NodeKey) -> str:
    x, y = POSITIONS[node]
    if node == "START":
        fill, stroke, text = PALETTE["start"]
        return (
            f'<circle cx="{x}" cy="{y}" r="22" fill="{fill}" stroke="{stroke}" stroke-width="2" filter="url(#shadow)"/>\n'
            f'<text x="{x}" y="{y + 4}" text-anchor="middle" font-family="Inter, ui-sans-serif, system-ui" '
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
        f'<rect x="{rect_x}" y="{rect_y}" width="{NODE_WIDTH}" height="{NODE_HEIGHT}" rx="10" '
        f'fill="{fill}" stroke="{stroke}" stroke-width="2" filter="url(#shadow)"/>\n',
    ]
    if definition.is_final:
        parts.append(
            f'<rect x="{rect_x + 5}" y="{rect_y + 5}" width="{NODE_WIDTH - 10}" height="{NODE_HEIGHT - 10}" rx="7" '
            f'fill="none" stroke="{stroke}" stroke-width="1.2" opacity="0.55"/>\n'
        )
    for index, line in enumerate(title_lines):
        parts.append(
            f'<text x="{x}" y="{title_y + (index * 15)}" text-anchor="middle" '
            'font-family="Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif" '
            f'font-size="13" font-weight="700" fill="{text_color}">{escape(line)}</text>\n'
        )
    parts.append(
        f'<text x="{x}" y="{y + 22}" text-anchor="middle" '
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


def _render_legend() -> str:
    items = [
        ("available", "available for runners"),
        ("owned", "runner-owned"),
        ("recovery", "recovery override"),
        ("concurrency", "concurrency control"),
        ("final", "final status"),
    ]
    x = 48
    y = 705
    parts = [
        '<text x="48" y="680" font-family="Inter, ui-sans-serif, system-ui" font-size="13" font-weight="700" fill="#4f3228">Legend</text>\n'
    ]
    for index, (style, label) in enumerate(items):
        fill, stroke, text = PALETTE[style]
        item_x = x + index * 270
        parts.append(
            f'<rect x="{item_x}" y="{y - 18}" width="28" height="18" rx="5" fill="{fill}" stroke="{stroke}" stroke-width="1.5"/>\n'
            f'<text x="{item_x + 38}" y="{y - 5}" font-family="Inter, ui-sans-serif, system-ui" font-size="12" fill="{text}">{label}</text>\n'
        )
    return "".join(parts)


def _node_name(node: NodeKey) -> str:
    return node.name if isinstance(node, InvocationStatus) else node
