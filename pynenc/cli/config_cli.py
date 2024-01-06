import argparse
import re
from functools import wraps
from typing import Any, Callable, NamedTuple, Type, TypeVar

from ..conf.config_base import ConfigBase, ConfigField

T = TypeVar("T", bound="ConfigBase")


def config_cls_cache(
    func: Callable[[Type[T]], dict[str, str]]
) -> Callable[[Type[T]], dict[str, str]]:
    cache: dict[str, dict[str, str]] = {}

    @wraps(func)
    def wrapper(config_cls: Type[T]) -> dict[str, str]:
        class_name = config_cls.__name__
        if class_name not in cache:
            cache[class_name] = func(config_cls)
        return cache[class_name]

    return wrapper


@config_cls_cache
def extract_descriptions_from_docstring(
    config_cls: Type["ConfigBase"],
) -> dict[str, str]:
    """Extract description for a specific field from a class docstring."""
    if config_cls == ConfigBase:
        return {}
    field_docs = {}
    for parent in config_cls.__bases__:
        if parent != ConfigBase and issubclass(parent, ConfigBase):
            field_docs.update(extract_descriptions_from_docstring(parent))

    if not (docstring := config_cls.__doc__):
        return field_docs

    # Regular expression pattern to capture field names and multiline descriptions
    pattern = r"\n\s{4}(\w+)\s*:\s*.*?\n(\s{6}.*?(?=\n\s{4}\w+\s*:|\Z))"

    matches = re.finditer(pattern, docstring, re.DOTALL)
    for match in matches:
        field_name = match.group(1)
        # Process multiline description
        description_lines = match.group(2).split("\n")
        description = " ".join(line.strip() for line in description_lines)
        field_docs[field_name] = description

    return field_docs


class ConfigFieldInfo(NamedTuple):
    attribute_name: str
    default_value: Any
    description: str


def extract_config_fields(config_class: Type["ConfigBase"]) -> list[ConfigFieldInfo]:
    """Extract configuration fields from a config class."""
    fields = []
    field_docs = extract_descriptions_from_docstring(config_class)
    for key, value in vars(config_class).items():
        if isinstance(value, ConfigField):
            # Extract description from docstring if available
            description = field_docs.get(key, "")
            fields.append(ConfigFieldInfo(key, value._default_value, description))
    return fields


def add_config_fields_to_parser(
    parser: argparse.ArgumentParser, config_class: Type["ConfigBase"]
) -> None:
    """Add configuration fields to an argparse parser."""
    for field_info in extract_config_fields(config_class):
        parser.add_argument(
            f"--{field_info.attribute_name}",
            default=field_info.default_value,
            help=f"{field_info.attribute_name} (default: {field_info.default_value})",
        )
