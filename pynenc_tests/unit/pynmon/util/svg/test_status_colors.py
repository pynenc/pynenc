"""
Unit tests for status color mappings.

Tests that status colors are correctly defined for all common statuses.
"""

from pynmon.util.status_colors import STATUS_COLORS


def test_all_common_statuses_have_colors() -> None:
    """Common statuses have defined colors."""
    common_statuses = ["REGISTERED", "PENDING", "RUNNING", "SUCCESS", "FAILED", "RETRY"]
    for status in common_statuses:
        assert status in STATUS_COLORS
        assert STATUS_COLORS[status].startswith("#")
