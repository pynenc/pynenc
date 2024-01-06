import os
from io import StringIO
from typing import TYPE_CHECKING, Type
from unittest.mock import patch

import pytest

from pynenc.cli import config_cli
from pynenc.cli.main_cli import main
from pynenc.conf.config_base import ConfigBase
from pynenc.conf.config_pynenc import ConfigPynenc
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


def test_cli_help() -> None:
    """Test CLI Exits normally"""
    # clear environment variables that could affect ConfigPynenc default values
    with patch.dict(os.environ, clear=True):
        with patch("sys.argv", ["pynenc", "--help"]):
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                with pytest.raises(SystemExit) as e:
                    main()
        assert e.value.code == 0  # Exits normally
        output = mock_stdout.getvalue()
        for field in ConfigPynenc().all_fields:
            assert f"--{field}" in output
            assert f"{field} (default: {getattr(ConfigPynenc(), field)})" in output
