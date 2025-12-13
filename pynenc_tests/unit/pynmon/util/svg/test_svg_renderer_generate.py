"""
Tests for generate_svg convenience function.
"""

from pynmon.util.svg import (
    SVGStyle,
    TimelineData,
    TimelineSVGRenderer,
    generate_svg,
)


def test_generate_svg_returns_string(sample_timeline_data: TimelineData) -> None:
    """Test generate_svg returns SVG string."""
    svg = generate_svg(sample_timeline_data)

    assert isinstance(svg, str)
    assert svg.startswith('<svg xmlns="http://www.w3.org/2000/svg"')


def test_generate_svg_with_custom_style(sample_timeline_data: TimelineData) -> None:
    """Test generate_svg accepts custom style."""
    style = SVGStyle(background_color="#f0f0f0")
    svg = generate_svg(sample_timeline_data, style=style)

    assert 'fill="#f0f0f0"' in svg


def test_generate_svg_equivalent_to_renderer(
    sample_timeline_data: TimelineData,
) -> None:
    """Test generate_svg produces same output as renderer."""
    style = SVGStyle()
    renderer = TimelineSVGRenderer(style=style)

    svg_from_function = generate_svg(sample_timeline_data, style=style)
    svg_from_renderer = renderer.render(sample_timeline_data)

    assert svg_from_function == svg_from_renderer
