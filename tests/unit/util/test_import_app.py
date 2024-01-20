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
        import_app.find_app_instance(None)
    assert "No module name provided" in str(exc_info.value)


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
