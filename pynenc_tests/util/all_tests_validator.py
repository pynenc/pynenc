"""
Utility for validating all_tests.py completeness in test folders.

This module provides functions to ensure that all test files in a folder
are either imported in all_tests.py or explicitly marked as ignored.

Key components:
- get_test_files_in_folder: Discover test_*.py files in a folder
- get_imported_modules: Parse all_tests.py to find imported modules
- validate_all_tests_completeness: Verify all tests are accounted for
"""

import ast
from pathlib import Path


def get_test_files_in_folder(folder_path: Path) -> set[str]:
    """
    Get all test_*.py files in a folder (excluding all_tests.py).

    :param Path folder_path: The folder to scan for test files
    :return: Set of test file stems (without .py extension)
    """
    test_files = set()
    for file_path in folder_path.glob("test_*.py"):
        test_files.add(file_path.stem)
    return test_files


def get_imported_modules_from_all_tests(all_tests_path: Path) -> set[str]:
    """
    Parse all_tests.py to extract the imported module names.

    Handles both:
    - `from pynenc_tests.unit.xxx.test_yyy import *`
    - Regular imports

    :param Path all_tests_path: Path to all_tests.py file
    :return: Set of imported module stems (e.g., 'test_yyy')
    """
    if not all_tests_path.exists():
        return set()

    with open(all_tests_path) as f:
        source = f.read()

    imported_modules = set()

    try:
        tree = ast.parse(source)
    except SyntaxError:
        return set()

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            # Extract last part of module path: pynenc_tests.unit.xxx.test_yyy -> test_yyy
            module_parts = node.module.split(".")
            if module_parts[-1].startswith("test_"):
                imported_modules.add(module_parts[-1])

    return imported_modules


def get_ignored_files_from_all_tests(all_tests_path: Path) -> set[str]:
    """
    Parse all_tests.py to extract the IGNORED_FILES set.

    Expects a module-level assignment like:
    IGNORED_FILES = {"test_local_stuff", "test_specific_impl"}

    :param Path all_tests_path: Path to all_tests.py file
    :return: Set of ignored file stems
    """
    if not all_tests_path.exists():
        return set()

    with open(all_tests_path) as f:
        source = f.read()

    try:
        tree = ast.parse(source)
    except SyntaxError:
        return set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "IGNORED_FILES":
                    if isinstance(node.value, ast.Set):
                        return {
                            elt.value
                            for elt in node.value.elts
                            if isinstance(elt, ast.Constant)
                            and isinstance(elt.value, str)
                        }
    return set()


def validate_all_tests_completeness(
    folder_path: Path,
    all_tests_path: Path | None = None,
) -> tuple[set[str], set[str]]:
    """
    Validate that all test files are accounted for in all_tests.py.

    Returns two sets:
    - missing: Test files not imported and not in IGNORED_FILES
    - extra: Files in IGNORED_FILES but don't exist

    :param Path folder_path: The test folder to validate
    :param Path | None all_tests_path: Path to all_tests.py (defaults to folder_path/all_tests.py)
    :return: Tuple of (missing_files, extra_ignored_files)
    """
    if all_tests_path is None:
        all_tests_path = folder_path / "all_tests.py"

    test_files = get_test_files_in_folder(folder_path)
    imported_modules = get_imported_modules_from_all_tests(all_tests_path)
    ignored_files = get_ignored_files_from_all_tests(all_tests_path)

    # Files that should be accounted for
    accounted_for = imported_modules | ignored_files

    # Missing = files that exist but aren't accounted for
    missing = test_files - accounted_for

    # Extra = files in IGNORED_FILES that don't exist
    extra = ignored_files - test_files

    return missing, extra


def discover_all_tests_files(root_path: Path) -> list[Path]:
    """
    Discover all all_tests.py files recursively from a root path.

    :param Path root_path: The root directory to search from
    :return: List of paths to all_tests.py files
    """
    return sorted(root_path.rglob("all_tests.py"))


def validate_all_all_tests_files(
    root_path: Path,
) -> dict[Path, tuple[set[str], set[str]]]:
    """
    Validate all all_tests.py files under a root path.

    Returns a dict mapping each all_tests.py folder to (missing, extra) tuple.
    Only includes folders with issues.

    :param Path root_path: The root directory to search from
    :return: Dict of folder_path -> (missing_files, extra_ignored_files)
    """
    issues: dict[Path, tuple[set[str], set[str]]] = {}

    for all_tests_path in discover_all_tests_files(root_path):
        folder_path = all_tests_path.parent
        missing, extra = validate_all_tests_completeness(folder_path, all_tests_path)
        if missing or extra:
            issues[folder_path] = (missing, extra)

    return issues
