from typing import Any
import yaml
import json


def load_file(filepath: str) -> dict[str, Any]:
    with open(filepath) as _file:
        if filepath.lower().endswith(".yaml") or filepath.lower().endswith(".yml"):
            # TODO fix yaml
            return yaml.load(_file)
        if filepath.lower().endswith(".json"):
            return json.load(_file)
    raise ValueError(f"Unexpected file extension {filepath=}")
