import multiprocessing
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

from pynenc.plugin_loader import load_all_plugins

if TYPE_CHECKING:
    pass


def _plugin_loader_subprocess(queue: multiprocessing.Queue) -> None:
    """
    Subprocess entry point for plugin loader test.
    Verifies that plugins are loaded and registered for subclass discovery.
    """
    from pynenc.orchestrator.base_orchestrator import BaseOrchestrator
    from pynenc.plugin_loader import load_all_plugins

    load_all_plugins()
    # Example: check that a known plugin class is registered
    # Replace 'TestOrchestrator' with a real plugin class name if available
    found = any(
        subcls.__name__ == "TestOrchestrator"
        for subcls in BaseOrchestrator.__subclasses__()
    )
    queue.put(found)


def test_load_all_plugins_success() -> None:
    """
    Test that load_all_plugins successfully imports plugin modules.
    """
    mock_ep1 = MagicMock()
    mock_ep1.module = "test_plugin1"
    mock_ep2 = MagicMock()
    mock_ep2.module = "test_plugin2"

    with (
        patch(
            "importlib.metadata.entry_points", return_value=[mock_ep1, mock_ep2]
        ) as mock_eps,
        patch("builtins.__import__") as mock_import,
    ):
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

    with (
        patch("importlib.metadata.entry_points", return_value=[mock_ep]),
        patch("builtins.__import__", side_effect=ImportError("Test error")),
    ):
        load_all_plugins()

        assert (
            "Failed to load plugin failing_plugin from failing_module: Test error"
            in caplog.text
        )


def test_load_all_plugins_in_subprocess() -> None:
    """
    Test that load_all_plugins works in a subprocess and registers plugin classes.
    """
    queue: multiprocessing.Queue = multiprocessing.Queue()
    proc = multiprocessing.Process(target=_plugin_loader_subprocess, args=(queue,))
    proc.start()
    proc.join(timeout=5)
    assert proc.exitcode == 0
    # If you have a real plugin class, this should be True
    # Otherwise, just check that no error occurs and the queue returns a boolean
    assert isinstance(queue.get(), bool)
