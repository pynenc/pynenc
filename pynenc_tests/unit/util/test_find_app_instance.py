"""Tests for the find_app_instance entry point."""

import sys
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from pynenc.app import Pynenc
from pynenc.util import import_app


def test_success_via_importlib() -> None:
    mock_app = Pynenc()
    mock_module = Mock()
    mock_module.app = mock_app

    with patch("importlib.import_module", return_value=mock_module):
        result = import_app.find_app_instance("mock_module")

    assert result is mock_app


def test_none_auto_discovers_single_app(tmp_path: Path) -> None:
    tasks_file = tmp_path / "tasks.py"
    tasks_file.write_text("from pynenc import Pynenc\napp = Pynenc()\n")

    with patch("os.getcwd", return_value=str(tmp_path)):
        result = import_app.find_app_instance(None)

    assert isinstance(result, Pynenc)
    sys.modules.pop("tasks", None)


def test_empty_string_auto_discovery_requires_an_app(tmp_path: Path) -> None:
    with patch("os.getcwd", return_value=str(tmp_path)):
        with pytest.raises(ValueError, match="No Pynenc.*instance found"):
            import_app.find_app_instance("")


def test_auto_discovery_rejects_multiple_apps(tmp_path: Path) -> None:
    (tmp_path / "tasks.py").write_text("from pynenc import Pynenc\napp = Pynenc()\n")
    (tmp_path / "worker.py").write_text("from pynenc import Pynenc\napp = Pynenc()\n")

    with patch("os.getcwd", return_value=str(tmp_path)):
        with pytest.raises(ValueError, match="Multiple Pynenc.*instances found"):
            import_app.find_app_instance(None)

    sys.modules.pop("tasks", None)
    sys.modules.pop("worker", None)


def test_auto_discovery_skips_unrelated_scripts(tmp_path: Path) -> None:
    (tmp_path / "tasks.py").write_text("from pynenc import Pynenc\napp = Pynenc()\n")
    (tmp_path / "sample.py").write_text("raise RuntimeError('should not import')\n")

    with patch("os.getcwd", return_value=str(tmp_path)):
        result = import_app.find_app_instance(None)

    assert isinstance(result, Pynenc)
    assert "sample" not in sys.modules
    sys.modules.pop("tasks", None)


def test_no_pynenc_instance_in_module() -> None:
    mock_module = Mock()
    mock_module.__name__ = "my_module"

    with patch("importlib.import_module", return_value=mock_module):
        with pytest.raises(ValueError, match="No Pynenc.*instance found"):
            import_app.find_app_instance("my_module")


def test_colon_format_rejected_with_suggestion() -> None:
    with pytest.raises(
        ValueError, match="Use dot notation instead: '--app mymodule.app'"
    ):
        import_app.find_app_instance("mymodule:app")


def test_colon_format_complex() -> None:
    with pytest.raises(ValueError, match="Use dot notation instead"):
        import_app.find_app_instance("pkg.mod:app")
