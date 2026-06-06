"""Phase 6 unit test: ``relation_kind="trigger"`` styling in family tree SVG."""

from __future__ import annotations

from pynmon.util.family_tree_svg import _render_connection


def test_trigger_relation_renders_with_teal_dashed_edge() -> None:
    svg = _render_connection(
        parent_cx=10.0,
        parent_y=0.0,
        child_cx=10.0,
        child_y=80.0,
        parent_id="p",
        child_id="c",
        relation_kind="trigger",
    )

    assert 'class="ft-edge ft-edge-trigger"' in svg
    assert 'data-relation-kind="trigger"' in svg
    assert 'stroke="#0d9488"' in svg
    assert "stroke-dasharray=" in svg


def test_event_relation_unchanged() -> None:
    svg = _render_connection(
        parent_cx=10.0,
        parent_y=0.0,
        child_cx=10.0,
        child_y=80.0,
        parent_id="p",
        child_id="c",
        relation_kind="event",
    )

    assert 'class="ft-edge ft-edge-event"' in svg
    assert 'stroke="#7c3aed"' in svg


def test_direct_relation_uses_timeline_legend_format() -> None:
    svg = _render_connection(
        parent_cx=10.0,
        parent_y=0.0,
        child_cx=10.0,
        child_y=80.0,
        parent_id="p",
        child_id="c",
        relation_kind="direct",
    )

    assert 'class="ft-edge ft-edge-direct"' in svg
    assert 'stroke="#78909c"' in svg
    assert 'stroke-dasharray="2,3"' in svg
