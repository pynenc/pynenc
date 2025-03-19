import os
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from pynenc import Pynenc
from pynenc.util.import_app import find_app_instance

NO_INSTANCE_PATH = str(Path(__file__).parent / "no_instance.py")
SOME_APP_PATH = str(Path(__file__).parent / "some_app.py")


def test_file_path_success() -> None:
    """Test loading a Pynenc instance from a valid file path."""
    app_instance = find_app_instance(SOME_APP_PATH)
    assert isinstance(app_instance, Pynenc)
    assert app_instance.conf.app_id == "pynenc"  # Default app_id


def test_file_path_without_py_extension() -> None:
    """Test loading a Pynenc instance from a file path without .py extension."""
    file_path_no_ext = SOME_APP_PATH.replace(".py", "")
    app_instance = find_app_instance(file_path_no_ext)
    assert isinstance(app_instance, Pynenc)


def test_file_path_nonexistent() -> None:
    """Test loading from a nonexistent file path raises ValueError."""
    nonexistent_path = "/path/to/nonexistent/file.py"
    with pytest.raises(ValueError) as exc_info:
        find_app_instance(nonexistent_path)
    assert f"File not found: {os.path.abspath(nonexistent_path)}" in str(exc_info.value)


def test_file_path_invalid_spec() -> None:
    """Test handling of invalid module spec (e.g., no loader)."""
    from pynenc.util import (  # Import the module where find_app_instance is defined
        import_app,
    )

    with patch.object(import_app, "spec_from_file_location", return_value=None):
        with pytest.raises(ValueError) as exc_info:
            find_app_instance(SOME_APP_PATH)
        expected_message = f"Could not create module spec for {SOME_APP_PATH}"
        assert expected_message in str(
            exc_info.value
        ), f"Expected '{expected_message}', got '{str(exc_info.value)}'"


def test_file_path_no_pynenc_instance() -> None:
    """Test file path with no Pynenc instance raises ValueError."""
    with pytest.raises(ValueError) as exc_info:
        find_app_instance(NO_INSTANCE_PATH)
    module_name = os.path.basename(NO_INSTANCE_PATH).replace(".py", "")
    assert f"No Pynenc app instance found in '{module_name}'" in str(exc_info.value)


def test_file_path_with_py_extension_already() -> None:
    """Test file path already ending with .py works correctly."""
    app_instance = find_app_instance(SOME_APP_PATH + ".py")  # Double .py
    assert isinstance(app_instance, Pynenc)


def test_file_path_empty_string() -> None:
    """Test empty string raises ValueError."""
    with pytest.raises(ValueError) as exc_info:
        find_app_instance("")
    assert "No application spec provided" in str(exc_info.value)


def test_file_path_none_input() -> None:
    """Test None input raises ValueError."""
    with pytest.raises(ValueError) as exc_info:
        find_app_instance(None)  # type: ignore[arg-type]
    assert "No application spec provided" in str(exc_info.value)


def test_file_path_module_spec_loader_none() -> None:
    """Test module spec with no loader raises ValueError."""
    from pynenc.util import import_app

    # Debugging: Check original function
    print(f"Original spec_from_file_location: {import_app.spec_from_file_location}")

    mock_spec = Mock()
    mock_spec.loader = None
    with patch.object(
        import_app, "spec_from_file_location", return_value=mock_spec
    ) as mock_patch:
        # Debugging: Confirm patch is applied
        print(f"Patched spec_from_file_location: {import_app.spec_from_file_location}")
        print(f"Mock patch object: {mock_patch}")

        with pytest.raises(ValueError) as exc_info:
            find_app_instance(SOME_APP_PATH)
        expected_message = f"Could not create module spec for {SOME_APP_PATH}"
        assert expected_message in str(
            exc_info.value
        ), f"Expected '{expected_message}', got '{str(exc_info.value)}'"


def test_file_path_execution_fails() -> None:
    """Test module execution failure raises an exception."""
    from pynenc.util import import_app

    # Debugging: Check original function
    print(f"Original spec_from_file_location: {import_app.spec_from_file_location}")

    mock_spec = Mock()
    mock_loader = Mock()
    mock_spec.loader = mock_loader
    with patch.object(
        import_app, "spec_from_file_location", return_value=mock_spec
    ) as mock_patch:
        # Debugging: Confirm patch is applied
        print(f"Patched spec_from_file_location: {import_app.spec_from_file_location}")
        print(f"Mock patch object: {mock_patch}")

        with patch.object(
            mock_loader, "exec_module", side_effect=Exception("Exec failed")
        ):
            with pytest.raises(Exception) as exc_info:
                find_app_instance(SOME_APP_PATH)
            assert "Exec failed" in str(
                exc_info.value
            ), f"Expected 'Exec failed', got '{str(exc_info.value)}'"
