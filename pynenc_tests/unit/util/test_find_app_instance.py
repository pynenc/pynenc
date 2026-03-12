"""Tests for the find_app_instance entry point."""

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


def test_none_raises_value_error() -> None:
    with pytest.raises(ValueError, match="No --app value provided"):
        import_app.find_app_instance(None)  # type: ignore[arg-type]


def test_empty_string_raises_value_error() -> None:
    with pytest.raises(ValueError, match="No --app value provided"):
        import_app.find_app_instance("")


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
