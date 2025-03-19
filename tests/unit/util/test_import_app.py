import os
import sys
import types
from unittest.mock import Mock, patch

import pytest

from pynenc.app import Pynenc
from pynenc.util import import_app


def test_find_app_instance_success() -> None:
    module_name = "mock_module_with_app"
    mock_pynenc_instance = Pynenc()
    mock_module = Mock()
    mock_module.app = mock_pynenc_instance

    with patch("importlib.import_module", return_value=mock_module):
        app_instance = import_app.find_app_instance(module_name)
    assert app_instance is mock_pynenc_instance


def test_find_app_instance_no_module_name() -> None:
    with pytest.raises(ValueError) as exc_info:
        import_app.find_app_instance(None)  # type: ignore
    assert "No application spec provided" in str(exc_info.value)


def test_find_app_instance_no_pynenc_instance() -> None:
    module_name = "mock_module_without_app"
    mock_module_without_app = Mock()
    mock_module_without_app.__name__ = module_name

    with patch("importlib.import_module", return_value=mock_module_without_app):
        with pytest.raises(ValueError) as exc_info:
            import_app.find_app_instance(module_name)

    assert f"No Pynenc app instance found in '{module_name}'" in str(exc_info.value)


def test_find_app_instance_module_not_found() -> None:
    module_name = "nonexistent.module"
    with patch("importlib.import_module", side_effect=ModuleNotFoundError):
        with pytest.raises(ValueError) as exc_info:
            import_app.find_app_instance(module_name)
    expected_error_msg = "Module file not found for spec"
    assert expected_error_msg in str(exc_info.value)


def test_module_not_found_no_file_path() -> None:
    module_name = "nonexistentmodule"
    with patch("importlib.import_module", side_effect=ModuleNotFoundError):
        with pytest.raises(ValueError) as exc_info:
            import_app.find_app_instance(module_name)
    assert "Module name 'nonexistentmodule' does not specify a path." in str(
        exc_info.value
    )


def test_module_not_found_nonexistent_file() -> None:
    module_name = "nonexistent.module"

    # Mock the function and specify a custom return value or side effect
    with patch(
        "pynenc.util.import_app.build_file_path_from_module_name"
    ) as mock_function:
        mock_function.side_effect = ValueError(
            "Module name 'nonexistent.module' does not specify a path."
        )

        with pytest.raises(ValueError) as exc_info:
            import_app.find_app_instance(module_name)

        # Assert that the mocked function was called with the expected arguments
        mock_function.assert_called_once_with(module_name)

        # Check the error message
        assert "Module name 'nonexistent.module' does not specify a path." in str(
            exc_info.value
        )


def test_module_not_found_with_file_not_found() -> None:
    module_name = "nonexistent.module"
    with patch(
        "pynenc.util.import_app.load_module_from_spec", side_effect=FileNotFoundError
    ):
        with pytest.raises(ValueError) as exc_info:
            import_app.find_app_instance(module_name)
    assert "Module file not found" in str(exc_info.value)


def test_create_module_spec_with_no_spec_or_loader() -> None:
    app_instance_name = "test_app"
    file_location = "/path/to/nonexistent/file.py"

    # Patch 'spec_from_file_location' to return a spec with None loader
    mock_spec = Mock()
    mock_spec.loader = None
    with patch("importlib.util.spec_from_file_location", return_value=mock_spec):
        with pytest.raises(ValueError) as exc_info:
            import_app.create_module_spec(app_instance_name, file_location)

    # Assert that the error message is as expected
    assert f"Module spec could not be created for location '{file_location}'." in str(
        exc_info.value
    )

    # Alternatively, patch 'spec_from_file_location' to return None
    with patch("importlib.util.spec_from_file_location", return_value=None):
        with pytest.raises(ValueError) as exc_info:
            import_app.create_module_spec(app_instance_name, file_location)

    # Assert that the error message is as expected
    assert f"Module spec could not be created for location '{file_location}'." in str(
        exc_info.value
    )


def test_load_module_from_spec_no_loader() -> None:
    # Mock a ModuleSpec with None loader
    mock_spec = Mock(spec=import_app.importlib.machinery.ModuleSpec)
    mock_spec.loader = None

    # Test that load_module_from_spec raises ValueError when loader is None
    with pytest.raises(ValueError) as exc_info:
        import_app.load_module_from_spec(mock_spec)

    # Assert that the error message is as expected
    assert f"Loader not found for spec '{mock_spec}'." in str(exc_info.value)


def test_find_app_instance_import_error_custom_message() -> None:
    """
    Test that when both importing as a module and as a file fail,
    find_app_instance raises a ValueError with the expected error message.
    """
    module_name = "nonexistent.module"
    dummy_error_message = "dummy error"

    with patch("importlib.import_module", side_effect=ModuleNotFoundError):
        with patch.object(
            import_app,
            "import_module_as_file",
            side_effect=ModuleNotFoundError(dummy_error_message),
        ):
            with pytest.raises(ValueError) as exc_info:
                import_app.find_app_instance(module_name)

    expected_message = (
        f"Could not import module '{module_name}'. Ensure it’s a valid module path "
        "or provide a file path (e.g., 'path/to/backtes.py').\n"
        f"Error: {dummy_error_message}"
    )
    assert expected_message in str(exc_info.value)


def test_file_path_syspath_insertion(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Test that when find_app_instance is given a file path (e.g., 'fake_module/dummy.py'),
    the computed project_root and module_dir are inserted into sys.path.
    """
    app_spec = "fake_module/dummy.py"
    dummy_file_path = "/fake/path/fake_module/dummy.py"
    project_root = "/fake/project/root"
    module_dir = "/fake/path/fake_module"

    dummy_pynenc = Pynenc()
    dummy_spec = Mock()
    dummy_loader = Mock()
    dummy_spec.loader = dummy_loader

    # Create a dummy module that contains the app instance.
    dummy_module = types.ModuleType("dummy")
    dummy_module.app = dummy_pynenc  # type: ignore

    # Simulate successful module loading.
    dummy_loader.exec_module.side_effect = lambda module: setattr(
        module, "app", dummy_pynenc
    )

    # Patch os functions to simulate file resolution.
    monkeypatch.setattr(
        os.path, "abspath", lambda x: dummy_file_path if x == app_spec else project_root
    )
    monkeypatch.setattr(os.path, "isfile", lambda x: True)
    monkeypatch.setattr(os.path, "dirname", lambda x: module_dir)

    # Patch spec and module creation functions.
    monkeypatch.setattr(
        import_app,
        "spec_from_file_location",
        lambda name, location: dummy_spec,
    )
    monkeypatch.setattr(import_app, "module_from_spec", lambda spec: dummy_module)
    monkeypatch.setattr(
        import_app,
        "find_pynenc_instance_in_module",
        lambda module: dummy_pynenc,
    )

    # Ensure sys.path does not contain project_root or module_dir beforehand.
    original_sys_path = sys.path.copy()
    try:
        if project_root in sys.path:
            sys.path.remove(project_root)
        if module_dir in sys.path:
            sys.path.remove(module_dir)

        result = import_app.find_app_instance(app_spec)

        # Validate the sys.path order.
        assert sys.path[0] == module_dir
        assert sys.path[1] == project_root
        assert result is dummy_pynenc

    finally:
        sys.path[:] = original_sys_path


def test_file_path_module_loading_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Test that when spec.loader.exec_module fails (e.g. due to missing dependency),
    find_app_instance raises a ModuleNotFoundError with the expected error message.
    """
    app_spec = "fake_module/dummy.py"
    dummy_file_path = "/fake/path/fake_module/dummy.py"
    project_root = "/fake/project/root"
    module_dir = "/fake/path/fake_module"

    dummy_spec = Mock()
    dummy_loader = Mock()
    dummy_spec.loader = dummy_loader

    # Force exec_module to raise a ModuleNotFoundError.
    dummy_loader.exec_module.side_effect = ModuleNotFoundError("missing dependency")

    monkeypatch.setattr(
        os.path, "abspath", lambda x: dummy_file_path if x == app_spec else project_root
    )
    monkeypatch.setattr(os.path, "isfile", lambda x: True)
    monkeypatch.setattr(os.path, "dirname", lambda x: module_dir)

    monkeypatch.setattr(
        import_app,
        "spec_from_file_location",
        lambda name, location: dummy_spec,
    )
    monkeypatch.setattr(
        import_app,
        "module_from_spec",
        lambda spec: types.ModuleType("dummy"),
    )
    monkeypatch.setattr(
        import_app, "find_pynenc_instance_in_module", lambda module: None
    )

    with pytest.raises(ModuleNotFoundError) as exc_info:
        import_app.find_app_instance(app_spec)

    assert f"Failed to load '{dummy_file_path}' due to missing module:" in str(
        exc_info.value
    )
