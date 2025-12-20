"""
Centralized validation of all all_tests.py files for plugin test coverage.

This module ensures that every test file in folders with all_tests.py is either:
1. Imported in all_tests.py (to be run by plugins), or
2. Listed in IGNORED_FILES (intentionally not exported to plugins)

This prevents accidentally forgetting to add new tests to all_tests.py files.
"""

from pathlib import Path

import pytest

from pynenc_tests.util.all_tests_validator import (
    discover_all_tests_files,
    validate_all_tests_completeness,
)


# Root of the test suite
TESTS_ROOT = Path(__file__).parent


def get_all_tests_folders() -> list[Path]:
    """Get all folders containing all_tests.py files."""
    return [p.parent for p in discover_all_tests_files(TESTS_ROOT)]


@pytest.mark.parametrize(
    "folder_path",
    get_all_tests_folders(),
    ids=lambda p: str(p.relative_to(TESTS_ROOT)),
)
def test_all_tests_completeness(folder_path: Path) -> None:
    """
    Verify that all test files are accounted for in all_tests.py.

    Each test folder with an all_tests.py must have every test_*.py file either:
    - Imported via `from ... import *` in all_tests.py
    - Listed in the IGNORED_FILES set with a reason comment

    :param Path folder_path: The folder containing all_tests.py to validate
    """
    missing, extra = validate_all_tests_completeness(folder_path)

    error_messages = []
    if missing:
        error_messages.append(
            f"Test files not in all_tests.py or IGNORED_FILES: {missing}. "
            f"Add them to all_tests.py imports or IGNORED_FILES set."
        )
    if extra:
        error_messages.append(
            f"Files in IGNORED_FILES that don't exist: {extra}. "
            f"Remove them from IGNORED_FILES."
        )

    assert not error_messages, "\n".join(error_messages)
