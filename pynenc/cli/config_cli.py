import argparse
import re
from functools import wraps
from typing import Callable, Type, TypeVar

from ..app import Pynenc
from ..conf.config_base import ConfigBase

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


def add_config_subparser(subparsers: argparse._SubParsersAction) -> None:
    show_config_parser = subparsers.add_parser(
        "show_config", help="Show app configuration"
    )
    show_config_parser.set_defaults(func=show_config_command)


def show_config_command(args: argparse.Namespace) -> None:
    """Show the configuration for a Pynenc instance."""
    # if not args.app:
    #     raise ValueError("app must be specified")
    if not isinstance(args.app_instance, Pynenc):
        raise TypeError("app_instance must be an instance of ConfigBase")

    if hasattr(args, "runner_command"):
        config: ConfigBase = args.app_instance.runner.conf
    else:
        config = args.app_instance.conf

    print("Showing configuration for Pynenc instance:")
    print(f"  - location: {args.app}")
    print(f"  - id: {args.app_instance.app_id}")
    print(f"Config {config.__class__.__name__}:")
    for field, description in extract_descriptions_from_docstring(
        config.__class__
    ).items():
        # Format and print each field
        print("-" * 50)
        print(f"{field}: ")
        print(f"  Default: {getattr(config, field)}")
        # Format multiline description
        formatted_description = "  ".join(description.splitlines())
        print(f"  Description: {formatted_description}")
    print("-" * 50)  # Closing separator after the last field
