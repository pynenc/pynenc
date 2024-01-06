import os
from io import StringIO
from typing import TYPE_CHECKING, Type
from unittest.mock import patch

import pytest

from pynenc import Pynenc
from pynenc.cli import config_cli
from pynenc.cli.main_cli import main
from pynenc.conf.config_base import ConfigBase
from pynenc.conf.config_runner import ConfigThreadRunner
from pynenc.conf.config_task import ConfigTask
from pynenc.util.subclasses import get_all_subclasses

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest
    from _pytest.python import Metafunc


def pytest_generate_tests(metafunc: "Metafunc") -> None:
    subclasses = get_all_subclasses(ConfigBase)  # type: ignore # mypy issue #4717
    if "config_cls" in metafunc.fixturenames:
        metafunc.parametrize("config_cls", subclasses, indirect=True)


@pytest.fixture
def config_cls(request: "FixtureRequest") -> Type[ConfigBase]:
    return request.param


app = Pynenc()


def test_parse_config_docstring(config_cls: Type["ConfigBase"]) -> None:
    """
    Test that the docstring of a ConfigBase subclass follows the expected format.

    Docstring Format Guidelines for ConfigBase Subclasses:
    - Class Description: Start with a brief description of the class's purpose.

    - Attributes Section:
        - Header: Use "Attributes" as a section header, followed by an underline of dashes on the next line.
        - Field Format: Document each configuration field as follows:
            - Field Name and Type: Start with the field name followed by its type in the format 'field_name : ConfigField[type]'. Indent this line with four spaces.
            - Description: Start the description on the next line, indented with eight spaces. Provide a clear and concise explanation of the field's purpose and behavior.
            - Multiline Descriptions: If the description spans multiple lines, ensure each subsequent line is also indented with eight spaces.
        - Consistency: Maintain consistent indentation and formatting for all fields and descriptions.

    Example:
    Here's an example docstring for a ConfigBase subclass following these guidelines:

    class ConfigExample(ConfigBase):
        \"""
        Description of what ConfigExample does and its role in the system.

        Attributes
        ----------
        sample_field : ConfigField[str]
            A short description of what 'sample_field' represents. This might include
            its purpose, how it's used, and any default behavior or values. If the
            description is long, continue on the next line with the same indentation.

        another_field : ConfigField[int]
            Explanation of 'another_field'. Describe what it controls, its impact,
            and any default settings or important notes. Continue with additional
            lines as needed, all indented consistently.
        \"""

        sample_field = ConfigField("default_value")
        another_field = ConfigField(10)

    The test implementation should verify that the docstring of each ConfigBase subclass
    adheres to these guidelines.
    """
    field_docs = config_cli.extract_descriptions_from_docstring(config_cls)
    if issubclass(config_cls, ConfigTask):
        config: ConfigBase = config_cls("module.task")
    else:
        config = config_cls()
    for key in config.all_fields:
        assert key in field_docs
        assert field_docs[key] != ""


def test_show_config_missing_app_error() -> None:
    """Test Exits with error due to missing --app"""
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


def test_cli_show_config_app() -> None:
    """Test show_config command for app configuration."""
    with patch(
        "sys.argv",
        ["pynenc", "--app", "tests.unit.cli.test_config_cli.app", "show_config"],
    ):
        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            main()

    output = mock_stdout.getvalue()
    assert "Showing configuration for Pynenc instance:" in output
    assert "location: tests.unit.cli.test_config_cli.app" in output
    assert f"id: {app.conf.app_id}" in output
    assert "Config ConfigPynenc:" in output
    check_fields_in_output(output, app.conf)


def test_cli_show_config_runner() -> None:
    """Test show_config command for runner configuration."""
    with patch(
        "sys.argv",
        [
            "pynenc",
            "--app",
            "tests.unit.cli.test_config_cli.app",
            "runner",
            "show_config",
        ],
    ):
        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            main()

    output = mock_stdout.getvalue()
    assert "Showing configuration for Pynenc instance:" in output
    assert "location: tests.unit.cli.test_config_cli.app" in output
    assert f"id: {app.conf.app_id}" in output
    # standard configuration for all the runners
    assert "Config ConfigRunner:" in output
    check_fields_in_output(output, app.runner.conf)


def test_cli_show_config_mem_runner() -> None:
    """Check that modifying env var for RUNNER_CLS affects show_config command."""
    with patch.dict(os.environ, {"PYNENC__RUNNER_CLS": "ThreadRunner"}):
        with patch(
            "sys.argv",
            [
                "pynenc",
                "--app",
                "tests.unit.cli.test_config_cli.app",
                "runner",
                "show_config",
            ],
        ):
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                main()

    output = mock_stdout.getvalue()
    assert "Showing configuration for Pynenc instance:" in output
    assert "location: tests.unit.cli.test_config_cli.app" in output
    assert f"id: {app.conf.app_id}" in output
    # environment variable changed the runner class and ThreadRunner has different config
    assert "Config ConfigThreadRunner:" in output
    check_fields_in_output(output, ConfigThreadRunner())


def check_fields_in_output(output: str, config: ConfigBase) -> None:
    """Check that all the fields are present in the output."""
    field_docs = config_cli.extract_descriptions_from_docstring(config.__class__)
    for field in config.all_fields:
        assert f"{field}:" in output
        assert f"Default: {getattr(config, field)}" in output
        assert f"Description: {field_docs[field]}" in output
