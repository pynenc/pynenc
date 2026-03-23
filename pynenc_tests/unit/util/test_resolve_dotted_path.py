"""Tests for dotted path resolution (importlib -> file fallback)."""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from pynenc.app import Pynenc
from pynenc.util import import_app


def test_fallback_to_file_when_import_fails(tmp_path: Path) -> None:
    """When importlib fails, loads <stem>.py from cwd."""
    tasks_file = tmp_path / "tasks.py"
    tasks_file.write_text("from pynenc import Pynenc\napp = Pynenc()\n")

    with patch("os.getcwd", return_value=str(tmp_path)):
        with patch("importlib.import_module", side_effect=ModuleNotFoundError):
            result = import_app.find_app_instance("tasks.app")

    assert isinstance(result, Pynenc)
    # Cleanup
    sys.modules.pop("tasks", None)


def test_single_component_no_dot_raises() -> None:
    with patch("importlib.import_module", side_effect=ModuleNotFoundError):
        with pytest.raises(ValueError, match="no dot separator"):
            import_app.find_app_instance("nonexistent")


def test_file_not_found_after_import_fails() -> None:
    with patch("os.getcwd", return_value="/nonexistent"):
        with patch("importlib.import_module", side_effect=ModuleNotFoundError):
            with pytest.raises(ValueError, match="Could not import"):
                import_app.find_app_instance("missing.module")


def test_module_registers_in_sys_modules(tmp_path: Path) -> None:
    """The loaded module should be registered in sys.modules for child processes."""
    tasks_file = tmp_path / "mymod.py"
    tasks_file.write_text("from pynenc import Pynenc\napp = Pynenc()\n")

    with patch("os.getcwd", return_value=str(tmp_path)):
        with patch("importlib.import_module", side_effect=ModuleNotFoundError):
            import_app.find_app_instance("mymod.app")

    assert "mymod" in sys.modules
    # Cleanup
    sys.modules.pop("mymod", None)


def test_file_dir_added_to_sys_path(tmp_path: Path) -> None:
    """The file's directory should be added to sys.path."""
    tasks_file = tmp_path / "pathmod.py"
    tasks_file.write_text("from pynenc import Pynenc\napp = Pynenc()\n")
    original_path = sys.path.copy()

    with patch("os.getcwd", return_value=str(tmp_path)):
        with patch("importlib.import_module", side_effect=ModuleNotFoundError):
            import_app.find_app_instance("pathmod.app")

    assert str(tmp_path) in sys.path
    # Cleanup
    sys.modules.pop("pathmod", None)
    sys.path[:] = original_path
