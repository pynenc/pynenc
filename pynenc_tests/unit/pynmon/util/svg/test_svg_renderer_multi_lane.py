"""
Tests for rendering with multiple lanes.
"""

from pynmon.util.svg import TimelineData, TimelineSVGRenderer


def test_render_multiple_lanes(multi_lane_data: TimelineData) -> None:
    """Test rendering with multiple lanes."""
    renderer = TimelineSVGRenderer()
    svg = renderer.render(multi_lane_data)

    assert "runner-0" in svg
    assert "runner-1" in svg
    assert "runner-2" in svg


def test_render_multiple_bars(multi_lane_data: TimelineData) -> None:
    """Test rendering with multiple bars per lane."""
    renderer = TimelineSVGRenderer()
    svg = renderer.render(multi_lane_data)

    bar_count = svg.count('class="invocation-bar"')
    assert bar_count == 6


def test_lanes_have_alternating_backgrounds(multi_lane_data: TimelineData) -> None:
    """Test lanes have tinted background colors based on runner color."""
    renderer = TimelineSVGRenderer()
    svg = renderer.render(multi_lane_data)

    assert "#ffffff" in svg
    assert 'opacity="0.08"' in svg or 'opacity="0.15"' in svg
