import argparse
import re
from functools import wraps
from typing import Callable, Type, TypeVar

from pynenc.app import Pynenc
from pynenc.conf.config_base import ConfigBase

T = TypeVar("T", bound="ConfigBase")


def config_cls_cache(
    func: Callable[[Type[T]], dict[str, str]]
) -> Callable[[Type[T]], dict[str, str]]:
    """
    Decorator for caching the output of functions that extract field descriptions.

    This decorator is designed to be applied to functions that process and return
    descriptions of configuration fields from class docstrings. It caches the results
    based on the class name to optimize performance by avoiding redundant processing.

    param Callable[[Type[T]], dict[str, str]] func: The function to be decorated.
    :return: A wrapped function with caching capability.
    """
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
    """
    Extract field descriptions from the docstring of a configuration class.

    Parses the docstring of the given configuration class (and its parent classes) to
    extract descriptions for each configuration field. The descriptions are expected to
    be formatted in a specific way within the docstring.

    :param Type[ConfigBase] config_cls: The configuration class to extract descriptions from.
    :return: A dictionary mapping field names to their descriptions.
    """
    if config_cls == ConfigBase:
        return {}
    field_docs = {}
    for parent in config_cls.__bases__:
        if parent != ConfigBase and issubclass(parent, ConfigBase):
            field_docs.update(extract_descriptions_from_docstring(parent))

    if not (docstring := config_cls.__doc__):
        return field_docs

    # Updated regular expression pattern to match :cvar style
    pattern = r":cvar\s+(\w+\[?.*?\]?)\s+(\w+):\s*(.*?)(?=\n\s*:cvar|$)"

    matches = re.finditer(pattern, docstring, re.DOTALL)
    for match in matches:
        field_type, field_name, description = match.groups()
        # Process multiline description
        description_lines = description.split("\n")
        description = " ".join(line.strip() for line in description_lines)
        field_docs[field_name] = description.strip()

    return field_docs


def add_config_subparser(subparsers: argparse._SubParsersAction) -> None:
    """
    Add a subparser for the 'show_config' command to the main argparse parser.

    This function is responsible for setting up the CLI structure for the 'show_config'
    command, including defining its help message and setting the default function to be
    executed when this command is selected.

    :param argparse._SubParsersAction subparsers: The subparsers object from the main parser.
    """
    show_config_parser = subparsers.add_parser(
        "show_config", help="Show app configuration"
    )
    show_config_parser.set_defaults(func=show_config_command)


def show_config_command(args: argparse.Namespace) -> None:
    """
    Execute the 'show_config' command for the Pynenc CLI.

    This command displays the current configuration of the Pynenc application instance.
    It uses the configuration class's docstring to provide detailed information about
    each configuration field, including its current value and description.

    :param argparse.Namespace args: The parsed CLI arguments, including the Pynenc application instance.
    """
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
