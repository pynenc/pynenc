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
            assert "--app" in line
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
