"""
Tests for TimelineSVGRenderer.
"""

from pynmon.util.svg import (
    SVGStyle,
    TimelineData,
    TimelineSVGRenderer,
)


def test_renderer_init_default_style() -> None:
    """Test renderer initializes with default style."""
    renderer = TimelineSVGRenderer()

    assert renderer.style is not None
    assert isinstance(renderer.style, SVGStyle)


def test_renderer_init_custom_style() -> None:
    """Test renderer accepts custom style."""
    style = SVGStyle(font_size=16)
    renderer = TimelineSVGRenderer(style=style)

    assert renderer.style.font_size == 16


def test_renderer_produces_svg(sample_timeline_data: TimelineData) -> None:
    """Test render produces valid SVG string."""
    renderer = TimelineSVGRenderer()
    svg = renderer.render(sample_timeline_data)

    assert isinstance(svg, str)
    assert svg.startswith('<svg xmlns="http://www.w3.org/2000/svg"')
    assert svg.endswith("</svg>")


def test_renderer_includes_dimensions(sample_timeline_data: TimelineData) -> None:
    """Test SVG includes correct dimensions via viewBox (no explicit height attribute).

    The explicit height attribute was removed so that browsers derive the SVG
    height from the viewBox aspect ratio when width="100%", preventing blank
    space at the bottom that grew with the number of invocations.
    """
    renderer = TimelineSVGRenderer()
    svg = renderer.render(sample_timeline_data)

    # Width is responsive (100%) and height is absent; dimensions live in viewBox
    assert 'width="100%"' in svg
    assert 'height="' not in svg.split(">")[0]  # no height attr in opening tag
    expected_viewbox = f'viewBox="0 0 {sample_timeline_data.config.width} {sample_timeline_data.total_height}"'
    assert expected_viewbox in svg


def test_renderer_includes_defs(sample_timeline_data: TimelineData) -> None:
    """Test SVG includes defs section with filters."""
    renderer = TimelineSVGRenderer()
    svg = renderer.render(sample_timeline_data)

    assert "<defs>" in svg
    assert 'id="shadow"' in svg
    assert "</defs>" in svg


def test_renderer_includes_background(sample_timeline_data: TimelineData) -> None:
    """Test SVG includes background rectangle."""
    renderer = TimelineSVGRenderer()
    svg = renderer.render(sample_timeline_data)

    assert 'x="0" y="0"' in svg
    assert 'fill="#ffffff"' in svg


def test_renderer_includes_lane_labels(sample_timeline_data: TimelineData) -> None:
    """Test SVG includes lane labels."""
    renderer = TimelineSVGRenderer()
    svg = renderer.render(sample_timeline_data)

    assert "runner-1" in svg
    assert "host1" in svg


def test_renderer_includes_bars(sample_timeline_data: TimelineData) -> None:
    """Test SVG includes invocation bars."""
    renderer = TimelineSVGRenderer()
    svg = renderer.render(sample_timeline_data)

    assert 'class="invocation-bar"' in svg
    assert 'data-invocation-id="inv-001"' in svg
    assert 'fill="#3498db"' in svg


def test_renderer_includes_tooltip(sample_timeline_data: TimelineData) -> None:
    """Test SVG bars include tooltips."""
    renderer = TimelineSVGRenderer()
    svg = renderer.render(sample_timeline_data)

    assert "<title>" in svg
    assert "Test invocation" in svg


def test_renderer_includes_time_axis(sample_timeline_data: TimelineData) -> None:
    """Test SVG includes time axis."""
    renderer = TimelineSVGRenderer()
    svg = renderer.render(sample_timeline_data)

    assert "<line" in svg
    assert "<text" in svg


def test_renderer_empty_timeline(empty_timeline_data: TimelineData) -> None:
    """Test rendering empty timeline doesn't crash."""
    renderer = TimelineSVGRenderer()
    svg = renderer.render(empty_timeline_data)

    assert isinstance(svg, str)
    assert '<svg xmlns="http://www.w3.org/2000/svg"' in svg
    assert "</svg>" in svg
