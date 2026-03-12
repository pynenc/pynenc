"""
Tests for CLI runner start with SQLite builder configuration.

Verifies proper error handling for common builder mistakes and correct
runner startup with SQLite + PersistentProcessRunner.
"""

from unittest.mock import MagicMock, patch

import pytest

from pynenc import PynencBuilder
from pynenc.cli.namespace import PynencCLINamespace
from pynenc.cli.runner_cli import start_runner_command
from pynenc.runner.persistent_process_runner import PersistentProcessRunner


def test_builder_sqlite_should_raise_when_called_without_instantiation() -> None:
    """Test that PynencBuilder.sqlite('path') without () raises a clear AttributeError."""
    with pytest.raises(AttributeError, match="'str' object has no attribute"):
        PynencBuilder.sqlite("/tmp/test.db")  # type: ignore[arg-type]


@pytest.mark.usefixtures("temp_sqlite_db_path")
def test_start_runner_should_succeed_with_sqlite_persistent_process_runner(
    temp_sqlite_db_path: str,
) -> None:
    """Test that runner starts correctly with SQLite + PersistentProcessRunner via CLI."""
    app = (
        PynencBuilder()
        .sqlite(sqlite_db_path=temp_sqlite_db_path)
        .persistent_process_runner()
        .build()
    )

    assert app.conf.runner_cls == "PersistentProcessRunner"
    assert isinstance(app.runner, PersistentProcessRunner)

    args = PynencCLINamespace()
    args.app = "test_app"
    args.app_instance = app

    with patch.object(app.runner, "run", new_callable=MagicMock) as mock_run:
        start_runner_command(args)
        mock_run.assert_called_once()
