import logging
from io import StringIO
from unittest.mock import patch

import pytest

from pynenc import Pynenc
from pynenc.cli.main_cli import main

app = Pynenc()


def test_cli_help() -> None:
    """Test CLI Exits normally"""
    with patch("sys.argv", ["pynenc", "--help"]):
        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            with pytest.raises(SystemExit) as e:
                main()
    assert e.value.code == 0  # Exits normally
    output = mock_stdout.getvalue()
    assert "--app APP" in output


def test_cli_missing_app() -> None:
    """Test Exits with error due to missing --app"""
    with patch("sys.argv", ["pynenc"]):
        with patch("sys.stderr", new_callable=StringIO) as mock_stdout:
            with pytest.raises(SystemExit) as e:
                main()
            assert e.value.code != 0
    output = mock_stdout.getvalue()
    assert "error: the following arguments are required" in output
    for line in output.splitlines():
        if "error: the following arguments are required" in line:
            assert "command" in line


def test_cli_with_app() -> None:
    """Test CLI with specified app but no command shows help message"""
    with patch("sys.argv", ["pynenc", "--app", "tests.unit.cli.test_main_cli.app"]):
        with patch("sys.stderr", new_callable=StringIO) as mock_stdout:
            with pytest.raises(SystemExit) as e:
                main()
    assert e.value.code != 0
    output = mock_stdout.getvalue()
    assert "error: the following arguments are required" in output
    for line in output.splitlines():
        if "error: the following arguments are required" in line:
            assert "--app" not in line
            assert "command" in line


def test_cli_value_error() -> None:
    """Test CLI handles ValueError from find_app_instance"""
    with patch("sys.argv", ["pynenc", "--app", "nonexistent.app", "runner", "start"]):
        with patch("pynenc.cli.main_cli.find_app_instance") as mock_find_app:
            mock_find_app.side_effect = ValueError("Invalid application module")
            log_capture = StringIO()
            handler = logging.StreamHandler(log_capture)
            logging.getLogger().addHandler(handler)
            try:
                with pytest.raises(SystemExit) as e:
                    main()
                assert e.value.code == 1
                output = log_capture.getvalue()
                # Adjust expectation based on default WARNING level format
                assert (
                    "Failed to load application: Invalid application module" in output
                )
            finally:
                logging.getLogger().removeHandler(handler)


def test_cli_unexpected_exception() -> None:
    """Test CLI handles unexpected Exception from func execution"""
    with patch(
        "sys.argv",
        ["pynenc", "--app", "tests.unit.cli.test_main_cli.app", "runner", "start"],
    ):
        with patch("pynenc.cli.main_cli.find_app_instance", return_value=app):
            # Mock the actual start_runner_command function
            with patch("pynenc.cli.runner_cli.start_runner_command") as mock_runner:
                mock_runner.side_effect = Exception("Unexpected error")
                log_capture = StringIO()
                handler = logging.StreamHandler(log_capture)
                logging.getLogger().addHandler(handler)
                try:
                    with pytest.raises(SystemExit) as e:
                        main()
                    assert e.value.code == 1
                    output = log_capture.getvalue()
                    assert "An unexpected error occurred: Unexpected error" in output
                finally:
                    logging.getLogger().removeHandler(handler)
