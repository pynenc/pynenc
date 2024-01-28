import importlib
import os
from typing import Any, Type, TypeVar

import pytest

from pynenc.broker.base_broker import BaseBroker
from pynenc.orchestrator.base_orchestrator import BaseOrchestrator
from pynenc.runner.base_runner import BaseRunner
from pynenc.serializer.base_serializer import BaseSerializer
from pynenc.state_backend.base_state_backend import BaseStateBackend
from pynenc.util.subclasses import get_all_subclasses

T = TypeVar("T")


def get_module_subclasses(module_base_path: str, base_class: Type[T]) -> set[Type[T]]:
    subclasses: set[Type[T]] = set()
    module_dir_path = os.path.join(os.getcwd(), module_base_path.replace("/", os.sep))

    for file in os.listdir(module_dir_path):
        if file.endswith(".py") and not file.startswith("__"):
            module_name = (
                module_base_path.replace("/", ".") + "." + file[:-3]
            )  # Convert to module path and remove '.py'
            module = importlib.import_module(module_name)
            subclasses.update(
                cls
                for _, cls in module.__dict__.items()
                if isinstance(cls, type)
                and issubclass(cls, base_class)
                and cls is not base_class
            )

    return subclasses


def filter_test_subclasses(subclasses: set[Type]) -> set[Type]:
    return {subclass for subclass in subclasses if "tests" not in subclass.__module__}


@pytest.mark.parametrize(
    "module_base_path, base_class",
    [
        ("pynenc/orchestrator", BaseOrchestrator),
        ("pynenc/runner", BaseRunner),
        ("pynenc/state_backend", BaseStateBackend),
        ("pynenc/broker", BaseBroker),
        ("pynenc/serializer", BaseSerializer),
    ],
)
def test_get_all_subclasses(module_base_path: str, base_class: Type[Any]) -> None:
    """Test that all the relevant subclasses are discovered by get_all_subclasses."""
    # To ensure that the __init__.py of the module need to import all the subclasses.
    # This test will check for any subclass in submodules manually
    # and then compare it with the subclasses discovered by get_all_subclasses.
    discovered_by_function = set(get_all_subclasses(base_class))
    manually_discovered_subclasses = get_module_subclasses(module_base_path, base_class)

    filtered_manual = filter_test_subclasses(manually_discovered_subclasses)
    filtered_function = filter_test_subclasses(discovered_by_function)

    missing_subclasses = filtered_manual - filtered_function
    assert not missing_subclasses, (
        f"Missing subclasses in {module_base_path}/__init__.py: "
        f"{', '.join([subclass.__name__ for subclass in missing_subclasses])}. "
        f"Please add them to {module_base_path}/__init__.py."
    )
