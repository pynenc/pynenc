import os
import sys
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from pynenc import Pynenc
from pynenc.util.import_app import find_app_instance


@pytest.fixture
def temp_app_file(tmp_path: Path) -> str:
    """Create a temporary Python file with a Pynenc instance."""
    file_path = tmp_path / "test_app.py"
    file_path.write_text("from pynenc import Pynenc\napp = Pynenc()")
    return str(file_path)


@pytest.fixture
def temp_app_file_no_instance(tmp_path: Path) -> str:
    """Create a temporary Python file without a Pynenc instance."""
    file_path = tmp_path / "no_instance.py"
    file_path.write_text("x = 42")
    return str(file_path)


def test_file_path_success(temp_app_file: str) -> None:
    """Test loading a Pynenc instance from a valid file path."""
    app_instance = find_app_instance(temp_app_file)
    assert isinstance(app_instance, Pynenc)
    assert app_instance.conf.app_id == "pynenc"  # Default app_id


def test_file_path_without_py_extension(temp_app_file: str) -> None:
    """Test loading a Pynenc instance from a file path without .py extension."""
    file_path_no_ext = temp_app_file.replace(".py", "")
    app_instance = find_app_instance(file_path_no_ext)
    assert isinstance(app_instance, Pynenc)


def test_file_path_nonexistent() -> None:
    """Test loading from a nonexistent file path raises ValueError."""
    nonexistent_path = "/path/to/nonexistent/file.py"
    with pytest.raises(ValueError) as exc_info:
        find_app_instance(nonexistent_path)
    assert f"File not found: {os.path.abspath(nonexistent_path)}" in str(exc_info.value)


def test_file_path_invalid_spec(temp_app_file: str) -> None:
    """Test handling of invalid module spec (e.g., no loader)."""
    from pynenc.util import (  # Import the module where find_app_instance is defined
        import_app,
    )

    with patch.object(import_app, "spec_from_file_location", return_value=None):
        with pytest.raises(ValueError) as exc_info:
            find_app_instance(temp_app_file)
        expected_message = f"Could not create module spec for {temp_app_file}"
        assert expected_message in str(
            exc_info.value
        ), f"Expected '{expected_message}', got '{str(exc_info.value)}'"


def test_file_path_no_pynenc_instance(temp_app_file_no_instance: str) -> None:
    """Test file path with no Pynenc instance raises ValueError."""
    with pytest.raises(ValueError) as exc_info:
        find_app_instance(temp_app_file_no_instance)
    module_name = os.path.basename(temp_app_file_no_instance).replace(".py", "")
    assert f"No Pynenc app instance found in '{module_name}'" in str(exc_info.value)


def test_file_path_relative_imports(temp_app_file: str) -> None:
    """Test that sys.path modification allows imports (adjusted to avoid relative import issue)."""
    dir_path = os.path.dirname(temp_app_file)
    sibling_file = os.path.join(dir_path, "sibling.py")
    with open(sibling_file, "w") as f:
        f.write("value = 123")

    # Use absolute import instead of relative to avoid package context issue
    with open(temp_app_file, "w") as f:
        f.write(
            "from pynenc import Pynenc\n"
            "from sibling import value\n"
            "app = Pynenc()\n"
            "app.custom_value = value"
        )

    original_sys_path = sys.path.copy()
    try:
        app_instance = find_app_instance(temp_app_file)
        assert isinstance(app_instance, Pynenc)
        assert getattr(app_instance, "custom_value", None) == 123
    finally:
        sys.path = original_sys_path


def test_file_path_with_py_extension_already(temp_app_file: str) -> None:
    """Test file path already ending with .py works correctly."""
    app_instance = find_app_instance(temp_app_file + ".py")  # Double .py
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


def test_file_path_module_spec_loader_none(temp_app_file: str) -> None:
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
            find_app_instance(temp_app_file)
        expected_message = f"Could not create module spec for {temp_app_file}"
        assert expected_message in str(
            exc_info.value
        ), f"Expected '{expected_message}', got '{str(exc_info.value)}'"


def test_file_path_execution_fails(temp_app_file: str) -> None:
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
                find_app_instance(temp_app_file)
            assert "Exec failed" in str(
                exc_info.value
            ), f"Expected 'Exec failed', got '{str(exc_info.value)}'"
