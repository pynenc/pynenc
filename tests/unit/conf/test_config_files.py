import json
import os
import tempfile
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest
import yaml

from pynenc.conf.config_base import ConfigBase, ConfigField

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest
    from _pytest.python import Metafunc


class ConfigGrandpa(ConfigBase):
    test_field = ConfigField("grandpa_value")


class ConfigParent(ConfigGrandpa):
    test_field = ConfigField("parent_value")


class ConfigChild(ConfigParent):
    test_field = ConfigField("child_value")


@dataclass
class TestCase:
    id: str
    content: dict
    expected_value: str


def pytest_generate_tests(metafunc: "Metafunc") -> None:
    default_content = {"test_field": "file_default_value"}
    grandpa_content = {
        **default_content,
        "grandpa": {"test_field": "file_grandpa_value"},
    }
    parent_content = {**grandpa_content, "parent": {"test_field": "file_parent_value"}}
    child_content = {**parent_content, "child": {"test_field": "file_child_value"}}
    test_cases = [
        TestCase("default", default_content, "file_default_value"),
        TestCase("grandpa", grandpa_content, "file_grandpa_value"),
        TestCase("parent", parent_content, "file_parent_value"),
        TestCase("child", child_content, "file_child_value"),
    ]
    ids = [x.id for x in test_cases]
    if "test_case" in metafunc.fixturenames:
        metafunc.parametrize("test_case", test_cases, ids=ids, indirect=True)


@pytest.fixture
def test_case(request: "FixtureRequest") -> TestCase:
    return request.param


def create_temp_file_with_content(content: str, file_extension: str) -> str:
    fd, path = tempfile.mkstemp(suffix=file_extension)
    with os.fdopen(fd, "w") as tmp:
        tmp.write(content)
    return path


def check_temp_file_content(
    file_content: str, file_extension: str, expected_value: str
) -> None:
    filepath = create_temp_file_with_content(file_content, file_extension)
    config = ConfigChild(config_filepath=filepath)
    assert config.test_field == expected_value
    os.remove(filepath)


def test_yaml(test_case: TestCase) -> None:
    yaml_content = yaml.dump(test_case.content)
    check_temp_file_content(yaml_content, ".yaml", test_case.expected_value)


def test_json(test_case: TestCase) -> None:
    json_content = json.dumps(test_case.content)
    check_temp_file_content(json_content, ".json", test_case.expected_value)


def test_toml(test_case: TestCase) -> None:
    toml_content = ""
    for key, value in test_case.content.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                toml_content += f'{key}.{sub_key} = "{sub_value}"\n'
        else:
            toml_content += f'{key} = "{value}"\n'
    check_temp_file_content(toml_content, ".toml", test_case.expected_value)


def test_pyproject_toml() -> None:
    PYPROJECT_TOML = """
[tool.pynenc]
test_field = "toml_value"

[tool.pynenc.grandpa]
test_field = "toml_grandpa_value"

[tool.pynenc.parent]
test_field = "toml_parent_value"
"""
    check_temp_file_content(PYPROJECT_TOML, ".pyproject.toml", "toml_parent_value")
