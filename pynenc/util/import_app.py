import importlib
import logging
import os
import types

from ..app import Pynenc


def build_file_path_from_module_name(module_name: str) -> str:
    """Build a file path from the module name."""
    filepath_arr = module_name.split(".")
    if len(filepath_arr) == 1:
        raise ValueError(f"Module name '{module_name}' does not specify a path.")
    *filepath_arr, _ = filepath_arr
    return os.path.join(*filepath_arr) + ".py"


def create_module_spec(
    app_instance_name: str, file_location: str
) -> importlib.machinery.ModuleSpec:
    """Create a module spec from file location."""
    spec = importlib.util.spec_from_file_location(app_instance_name, file_location)
    if not spec or not spec.loader:
        raise ValueError(
            f"Module spec could not be created for location '{file_location}'."
        )
    return spec


def load_module_from_spec(spec: importlib.machinery.ModuleSpec) -> types.ModuleType:
    """Load a module from a spec."""
    if spec.loader is None:
        raise ValueError(f"Loader not found for spec '{spec}'.")

    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except FileNotFoundError as ex:
        raise ValueError(f"Module file not found for spec '{spec}'.") from ex
    return module


def import_module_as_file(module_name: str, ex: Exception) -> types.ModuleType:
    """Attempt to import a module as a file."""
    filepath = build_file_path_from_module_name(module_name)
    *_, app_instance_name = module_name.split(".")
    spec = create_module_spec(app_instance_name, filepath)
    try:
        return load_module_from_spec(spec)
    except FileNotFoundError as file_not_found_ex:
        raise ValueError(
            f"Module file not found for spec '{spec}'."
        ) from file_not_found_ex


def find_pynenc_instance_in_module(module: types.ModuleType) -> Pynenc:
    """Find and return Pynenc app instance from a module."""
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if isinstance(attr, Pynenc):
            return attr
    raise ValueError(f"No Pynenc app instance found in '{module.__name__}'")


def find_app_instance(module_name: str | None) -> Pynenc:
    """Find the Pynenc app instance in the specified module."""
    if not module_name:
        raise ValueError("No module name provided")
    logging.debug(f"Attempting to find app instance in module: {module_name}")

    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError:
        module = import_module_as_file(module_name, ModuleNotFoundError())

    return find_pynenc_instance_in_module(module)
