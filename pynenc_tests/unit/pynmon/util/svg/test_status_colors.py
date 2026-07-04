"""
Unit tests for status color mappings.

Tests that status colors are correctly defined for all common statuses.
"""

from pynmon.util.status_colors import (
    DEFAULT_STATUS_STYLE,
    STATUS_BADGE_TEXT_COLOR,
    STATUS_COLOR_STYLES,
    STATUS_COLORS,
    get_hex_color,
    get_status_colors,
)


def test_all_common_statuses_have_colors() -> None:
    """Common statuses have defined colors."""
    common_statuses = ["REGISTERED", "PENDING", "RUNNING", "SUCCESS", "FAILED", "RETRY"]
    for status in common_statuses:
        assert status in STATUS_COLORS
        assert STATUS_COLORS[status].startswith("#")


def test_retry_uses_the_shared_purple_palette() -> None:
    """Retry stays purple and gets readable foreground text."""
    assert STATUS_COLORS["RETRY"] == "#9b59b6"
    assert STATUS_COLOR_STYLES["RETRY"]["background"] == "#9b59b6"
    assert DEFAULT_STATUS_STYLE["background"] == "#7f8c8d"
    assert STATUS_BADGE_TEXT_COLOR == "#ffffff"
    assert get_hex_color("retry") == "#9b59b6"
    assert get_hex_color("retrying") == "#9b59b6"
    assert get_status_colors("retry").hex_color == "#9b59b6"
    assert get_status_colors("retrying").hex_color == "#9b59b6"
    assert get_status_colors("retry").text_color == "#ffffff"
    assert get_status_colors("pending").text_color == "#ffffff"
