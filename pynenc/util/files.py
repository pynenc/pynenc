import json
import tomllib
from typing import Any

import yaml


def load_config_from_toml(file_path: str) -> dict:
    with open(file_path, "rb") as toml_file:
        if config_data := tomllib.load(toml_file):
            if "pyproject.toml" in file_path:
                return config_data.get("tool", {}).get("pynenc", {})
        return config_data or {}


def load_file(filepath: str) -> dict[str, Any]:
    with open(filepath) as _file:
        if filepath.lower().endswith(".yaml") or filepath.lower().endswith(".yml"):
            # TODO fix yaml
            return yaml.load(_file, Loader=yaml.SafeLoader)
        if filepath.lower().endswith(".json"):
            return json.load(_file)
        if filepath.lower().endswith(".toml"):
            return load_config_from_toml(filepath)
    raise ValueError(f"Unexpected file extension {filepath=}")
