import importlib
import logging
import os
import types

from pynenc.app import Pynenc


def build_file_path_from_module_name(module_name: str) -> str:
    """
    Builds a file path from a module name.

    :param str module_name: The name of the module.
    :return: The file path corresponding to the module.
    :raises ValueError: If the module name does not specify a path.
    """
    filepath_arr = module_name.split(".")
    if len(filepath_arr) == 1:
        raise ValueError(f"Module name '{module_name}' does not specify a path.")
    *filepath_arr, _ = filepath_arr
    return os.path.join(*filepath_arr) + ".py"


def create_module_spec(
    app_instance_name: str, file_location: str
) -> importlib.machinery.ModuleSpec:
    """
    Creates a module specification from a file location.

    :param str app_instance_name: The name of the app instance.
    :param str file_location: The location of the file.
    :return: A ModuleSpec object.
    :raises ValueError: If the module spec could not be created.
    """
    spec = importlib.util.spec_from_file_location(app_instance_name, file_location)
    if not spec or not spec.loader:
        raise ValueError(
            f"Module spec could not be created for location '{file_location}'."
        )
    return spec


def load_module_from_spec(spec: importlib.machinery.ModuleSpec) -> types.ModuleType:
    """
    Loads a module from a given specification.

    :param importlib.machinery.ModuleSpec spec: The module specification.
    :return: The loaded module.
    :raises ValueError: If the loader is not found for the given spec.
    """
    if spec.loader is None:
        raise ValueError(f"Loader not found for spec '{spec}'.")

    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except FileNotFoundError as ex:
        raise ValueError(f"Module file not found for spec '{spec}'.") from ex
    return module


def import_module_as_file(module_name: str, ex: Exception) -> types.ModuleType:
    """
    Attempts to import a module as a file.

    :param str module_name: The name of the module.
    :param Exception ex: The exception to be raised in case of failure.
    :return: The imported module.
    :raises ValueError: If the module file cannot be found.
    """
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
    """
    Finds and returns the Pynenc app instance from a given module.

    :param types.ModuleType module: The module to search in.
    :return: The found Pynenc app instance.
    :raises ValueError: If no Pynenc app instance is found.
    """
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if isinstance(attr, Pynenc):
            return attr
    raise ValueError(f"No Pynenc app instance found in '{module.__name__}'")


def find_app_instance(module_name: str | None) -> Pynenc:
    """
    Finds the Pynenc app instance in the specified module.

    :param str | None module_name: The name of the module.
    :return: The Pynenc app instance.
    :raises ValueError: If no module name is provided.
    """
    if not module_name:
        raise ValueError("No module name provided")
    logging.debug(f"Attempting to find app instance in module: {module_name}")

    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError:
        module = import_module_as_file(module_name, ModuleNotFoundError())

    return find_pynenc_instance_in_module(module)
