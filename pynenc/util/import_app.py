import importlib
import importlib.util
import logging
import os

from ..app import Pynenc


def find_app_instance(module_name: str | None) -> Pynenc:
    if not module_name:
        raise ValueError("No module name provided")
    logging.debug(f"Attempting to find app instance in module: {module_name}")
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as ex:
        logging.debug(f"Module not found: {module_name}, attempting to import as file")
        *filepath_arr, app_instance_name = module_name.split(".")
        filepath = os.path.join(*filepath_arr)
        file_location = filepath + ".py"
        spec = importlib.util.spec_from_file_location(app_instance_name, file_location)
        if not spec or not spec.loader:
            raise ValueError(f"Module not found: {module_name}") from ex
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        logging.debug(f"Module from file imported successfully: {module}")

    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if isinstance(attr, Pynenc):
            logging.debug(f"Found Pynenc app instance: {attr}")
            return attr

    raise ValueError(f"No Pynenc app instance found in '{module_name}'")
