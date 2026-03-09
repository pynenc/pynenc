"""
Tests for SVGStyle configuration.
"""

from pynmon.util.svg import SVGStyle


def test_svg_style_default_values() -> None:
    """Test SVGStyle has sensible defaults."""
    style = SVGStyle()

    assert style.font_family == "system-ui, -apple-system, sans-serif"
    assert style.font_size == 12
    assert style.background_color == "#ffffff"
    assert style.bar_opacity == 0.9
    assert style.bar_rx == 3


def test_svg_style_custom_values() -> None:
    """Test SVGStyle accepts custom values."""
    style = SVGStyle(
        font_family="Arial",
        font_size=14,
        background_color="#f0f0f0",
        bar_opacity=0.8,
    )

    assert style.font_family == "Arial"
    assert style.font_size == 14
    assert style.background_color == "#f0f0f0"
    assert style.bar_opacity == 0.8
