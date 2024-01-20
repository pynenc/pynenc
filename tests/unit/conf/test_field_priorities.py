import json
import os
import tempfile
from unittest.mock import patch

from pynenc.conf import config_base


class SomeConfig(config_base.ConfigBase):
    field = config_base.ConfigField(0)


def test_default() -> None:
    """Test that returns the default"""
    conf = SomeConfig()
    assert conf.field == 0


def test_config_values_map() -> None:
    """Test that specifying the config by value overwrites defaul"""
    conf = SomeConfig(config_values={"field": 1})
    assert conf.field == 1


def test_config_specific_values_map() -> None:
    """Test that within a value map,
    specific Config class values overwrite general ones"""
    conf = SomeConfig(config_values={"field": 1, "some": {"field": 2}})
    assert conf.field == 2


def test_config_file() -> None:
    """Test that filepath would overwrite previous values"""
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "config.json")
        with open(filepath, mode="w") as _file:
            _file.write(json.dumps({"field": 3}))
        conf = SomeConfig(
            config_values={"field": 1, "some": {"field": 2}},
            config_filepath=filepath,
        )
        assert conf.field == 3


def test_config_file_specific_value() -> None:
    """Test that filepath would overwrite previous values"""
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "config.json")
        with open(filepath, mode="w") as _file:
            _file.write(json.dumps({"field": 3, "some": {"field": 4}}))
        conf = SomeConfig(
            config_values={"field": 1, "some": {"field": 2}},
            config_filepath=filepath,
        )
        assert conf.field == 4


def test_config_filepath_environ() -> None:
    """Test filepath specified in environ will overwrite filepath by argument"""
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "config.json")
        with open(filepath, mode="w") as _file:
            _file.write(json.dumps({"field": 3, "some": {"field": 4}}))
        env_filepath = os.path.join(tmpdir, "config_env.json")
        with open(env_filepath, mode="w") as _file:
            _file.write(json.dumps({"field": 5}))
        with patch.dict(os.environ, {"PYNENC__FILEPATH": env_filepath}):
            conf = SomeConfig(
                config_values={"field": 1, "some": {"field": 2}},
                config_filepath=filepath,
            )
        assert conf.field == 5


def test_config_filepath_specific_environ() -> None:
    """Test filepath for specific config class
    will overwrite general filepath specified in environ"""
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "config.json")
        with open(filepath, mode="w") as _file:
            _file.write(json.dumps({"field": 3, "some": {"field": 4}}))
        env_filepath = os.path.join(tmpdir, "config_env.json")
        with open(env_filepath, mode="w") as _file:
            _file.write(json.dumps({"field": 5}))
        specific_env_filepath = os.path.join(tmpdir, "config_specific_env.json")
        with open(specific_env_filepath, mode="w") as _file:
            _file.write(json.dumps({"field": 6}))
        with patch.dict(
            os.environ,
            {
                "PYNENC__FILEPATH": env_filepath,
                "PYNENC__SOMECONFIG__FILEPATH": specific_env_filepath,
            },
        ):
            conf = SomeConfig(
                config_values={"field": 1, "some": {"field": 2}},
                config_filepath=filepath,
            )
        assert conf.field == 6


def test_config_environ_variables() -> None:
    """Test environ config var will overwrite all previous"""
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "config.json")
        with open(filepath, mode="w") as _file:
            _file.write(json.dumps({"field": 3, "some": {"field": 4}}))
        env_filepath = os.path.join(tmpdir, "config_env.json")
        with open(env_filepath, mode="w") as _file:
            _file.write(json.dumps({"field": 5}))
        specific_env_filepath = os.path.join(tmpdir, "config_specific_env.json")
        with open(specific_env_filepath, mode="w") as _file:
            _file.write(json.dumps({"field": 6}))
        with patch.dict(
            os.environ,
            {
                "PYNENC__FILEPATH": env_filepath,
                "PYNENC__SOMECONFIG__FILEPATH": specific_env_filepath,
                "PYNENC__FIELD": "7",
            },
        ):
            conf = SomeConfig(
                config_values={"field": 1, "some": {"field": 2}},
                config_filepath=filepath,
            )
        assert conf.field == 7


def test_config_specific_environ_variables() -> None:
    """Test environ config var will overwrite all previous"""
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = os.path.join(tmpdir, "config.json")
        with open(filepath, mode="w") as _file:
            _file.write(json.dumps({"field": 3, "some": {"field": 4}}))
        env_filepath = os.path.join(tmpdir, "config_env.json")
        with open(env_filepath, mode="w") as _file:
            _file.write(json.dumps({"field": 5}))
        specific_env_filepath = os.path.join(tmpdir, "config_specific_env.json")
        with open(specific_env_filepath, mode="w") as _file:
            _file.write(json.dumps({"field": 6}))
        with patch.dict(
            os.environ,
            {
                "PYNENC__FILEPATH": env_filepath,
                "PYNENC__SOMECONFIG__FILEPATH": specific_env_filepath,
                "PYNENC__FIELD": "7",
                "PYNENC__SOMECONFIG__FIELD": "8",
            },
        ):
            conf = SomeConfig(
                config_values={"field": 1, "some": {"field": 2}},
                config_filepath=filepath,
            )
        assert conf.field == 8
