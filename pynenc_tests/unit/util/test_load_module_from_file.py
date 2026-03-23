"""Tests for the internal _load_module_from_file helper."""

from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from pynenc.util import import_app


def test_nonexistent_file_raises() -> None:
    with pytest.raises(ValueError, match="File not found"):
        import_app._load_module_from_file("/no/such/file.py")


def test_none_spec_raises(tmp_path: Path) -> None:
    f = tmp_path / "dummy.py"
    f.write_text("x = 1\n")

    with patch("importlib.util.spec_from_file_location", return_value=None):
        with pytest.raises(ValueError, match="Could not create module spec"):
            import_app._load_module_from_file(str(f))


def test_none_loader_raises(tmp_path: Path) -> None:
    f = tmp_path / "dummy.py"
    f.write_text("x = 1\n")

    mock_spec = Mock()
    mock_spec.loader = None
    with patch("importlib.util.spec_from_file_location", return_value=mock_spec):
        with pytest.raises(ValueError, match="Could not create module spec"):
            import_app._load_module_from_file(str(f))


def test_missing_dependency_raises_module_not_found(tmp_path: Path) -> None:
    f = tmp_path / "bad.py"
    f.write_text("import nonexistent_package_xyz\n")

    with pytest.raises(ModuleNotFoundError, match="Failed to load"):
        import_app._load_module_from_file(str(f))
