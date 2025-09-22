from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

from pynenc.plugin_loader import load_all_plugins

if TYPE_CHECKING:
    pass


def test_load_all_plugins_success() -> None:
    """
    Test that load_all_plugins successfully imports plugin modules.
    """
    mock_ep1 = MagicMock()
    mock_ep1.module = "test_plugin1"
    mock_ep2 = MagicMock()
    mock_ep2.module = "test_plugin2"

    with patch(
        "importlib.metadata.entry_points", return_value=[mock_ep1, mock_ep2]
    ) as mock_eps, patch("builtins.__import__") as mock_import:
        load_all_plugins()

        mock_eps.assert_called_once_with(group="pynenc.plugins")
        mock_import.assert_any_call("test_plugin1")
        mock_import.assert_any_call("test_plugin2")


def test_load_all_plugins_import_error(caplog: pytest.LogCaptureFixture) -> None:
    """
    Test that load_all_plugins logs a warning on import errors but continues.
    """
    mock_ep = MagicMock()
    mock_ep.name = "failing_plugin"
    mock_ep.module = "failing_module"

    with patch("importlib.metadata.entry_points", return_value=[mock_ep]), patch(
        "builtins.__import__", side_effect=ImportError("Test error")
    ):
        load_all_plugins()

        assert (
            "Failed to load plugin failing_plugin from failing_module: Test error"
            in caplog.text
        )
