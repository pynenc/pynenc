"""Integration-style tests for --app file-path resolution using real fixture files."""

import os
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from pynenc import Pynenc
from pynenc.util.import_app import find_app_instance

# Fixture files live next to this test file
SOME_APP_PATH = str(Path(__file__).parent / "some_app.py")
NO_INSTANCE_PATH = str(Path(__file__).parent / "no_instance.py")


def test_loads_pynenc_from_file() -> None:
    app = find_app_instance(SOME_APP_PATH)
    assert isinstance(app, Pynenc)
    assert app.conf.app_id == "pynenc"


def test_loads_without_py_extension() -> None:
    path_no_ext = SOME_APP_PATH.removesuffix(".py")
    app = find_app_instance(path_no_ext)
    assert isinstance(app, Pynenc)


def test_nonexistent_file_raises() -> None:
    bad_path = "/path/to/nonexistent/file.py"
    with pytest.raises(ValueError, match="File not found"):
        find_app_instance(bad_path)


def test_no_pynenc_instance_raises() -> None:
    module_name = os.path.basename(NO_INSTANCE_PATH).replace(".py", "")
    with pytest.raises(
        ValueError, match=f"No Pynenc.*instance found in module '{module_name}'"
    ):
        find_app_instance(NO_INSTANCE_PATH)


def test_none_spec_raises() -> None:
    with patch("importlib.util.spec_from_file_location", return_value=None):
        with pytest.raises(ValueError, match="Could not create module spec"):
            find_app_instance(SOME_APP_PATH)


def test_none_loader_raises() -> None:
    mock_spec = Mock()
    mock_spec.loader = None
    with patch("importlib.util.spec_from_file_location", return_value=mock_spec):
        with pytest.raises(ValueError, match="Could not create module spec"):
            find_app_instance(SOME_APP_PATH)


def test_exec_module_failure_propagates() -> None:
    mock_spec = Mock()
    mock_spec.loader.exec_module.side_effect = RuntimeError("Exec failed")
    with patch("importlib.util.spec_from_file_location", return_value=mock_spec):
        with pytest.raises(RuntimeError, match="Exec failed"):
            find_app_instance(SOME_APP_PATH)


def test_empty_string_raises() -> None:
    with pytest.raises(ValueError, match="No --app value provided"):
        find_app_instance("")


def test_none_input_raises() -> None:
    with pytest.raises(ValueError, match="No --app value provided"):
        find_app_instance(None)  # type: ignore[arg-type]
