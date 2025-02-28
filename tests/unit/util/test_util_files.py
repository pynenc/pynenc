import json
import os
from typing import Any
from unittest.mock import patch

import pytest
import yaml
from _pytest.tmpdir import TempPathFactory

from pynenc.util import files


# Helper function to create temporary files for testing with type hints
def create_temp_file(tmpdir: TempPathFactory, filename: str, content: str) -> str:
    file_path = os.path.join(tmpdir, filename)
    with open(file_path, "w") as f:
        f.write(content)
    return file_path


# Test for loading JSON file
def test_load_json_file(tmpdir: TempPathFactory) -> None:
    content: dict[str, Any] = {"key": "value"}
    file_path: str = create_temp_file(tmpdir, "config.json", json.dumps(content))
    assert files.load_file(file_path) == content


# Test for loading YAML file
def test_load_yaml_file(tmpdir: TempPathFactory) -> None:
    content: dict[str, Any] = {"key": "value"}
    file_path: str = create_temp_file(tmpdir, "config.yaml", yaml.dump(content))
    assert files.load_file(file_path) == content


# Test for loading TOML file
def test_load_toml_file(tmpdir: TempPathFactory) -> None:
    content: str = '[tool.pynenc]\nkey = "value"'
    file_path: str = create_temp_file(tmpdir, "pyproject.toml", content)
    assert files.load_config_from_toml(file_path) == {"key": "value"}


# Test for loading TOML file not specific to pynenc
def test_load_generic_toml_file(tmpdir: TempPathFactory) -> None:
    content: str = '[tool.other]\nkey = "value"'
    file_path: str = create_temp_file(tmpdir, "config.toml", content)
    assert files.load_config_from_toml(file_path) == {
        "tool": {"other": {"key": "value"}}
    }


# Test for unsupported file extension
def test_load_unsupported_file_extension(tmpdir: TempPathFactory) -> None:
    file_path: str = create_temp_file(tmpdir, "config.unsupported", "some content")
    with pytest.raises(ValueError):
        files.load_file(file_path)


# Test that load file will call load_config_from_toml for TOML extension
def test_load_file_calls_load_config_from_toml(tmpdir: TempPathFactory) -> None:
    """Test that load_file delegates TOML loading to load_config_from_toml."""
    content: str = '[tool.pynenc]\nkey = "value"'
    file_path: str = create_temp_file(tmpdir, "config.toml", content)

    with patch("pynenc.util.files.load_config_from_toml") as mock_load_toml:
        mock_load_toml.return_value = {"mocked": "data"}
        result = files.load_file(file_path)

        # Verify load_config_from_toml was called with correct path
        mock_load_toml.assert_called_once_with(file_path)
        # Verify the result is what load_config_from_toml returned
        assert result == {"mocked": "data"}
