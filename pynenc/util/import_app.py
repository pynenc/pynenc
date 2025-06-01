import importlib
import logging
import os
import sys
import types
from importlib.util import module_from_spec, spec_from_file_location
from typing import Optional

from pynenc.app import AppInfo, Pynenc


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
    module_name = getattr(module, "__name__", "unknown")
    raise ValueError(f"No Pynenc app instance found in '{module_name}'")


def find_app_instance(app_spec: str | None) -> Pynenc:
    """
    Find and load a Pynenc application instance from a module or file path.

    :param str | None module_name: A string that can be a module path (e.g., 'core.src.api.backtes')
                  or a file path (e.g., 'core/src/api/backtes.py').
    :return: The Pynenc app instance.
    :raises ValueError: If the app cannot be loaded or no Pynenc instance is found.
    """
    if not app_spec:
        raise ValueError("No application spec provided")
    logging.debug(f"Attempting to find app instance for: {app_spec}")
    # Normalize the input (remove .py if present)
    app_spec = app_spec.replace(".py", "")

    # Check if it’s a file path
    if os.path.sep in app_spec or app_spec.endswith(".py"):
        file_path = os.path.abspath(
            app_spec if app_spec.endswith(".py") else f"{app_spec}.py"
        )
        if not os.path.isfile(file_path):
            raise ValueError(f"File not found: {file_path}")

        # Add the project root (assuming 3 levels up from file: core/src/api -> project root)
        project_root = os.path.abspath(
            os.path.join(os.path.dirname(file_path), "../../..")
        )
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
        module_dir = os.path.dirname(file_path)
        if module_dir not in sys.path:
            sys.path.insert(0, module_dir)

        module_name = os.path.basename(file_path).replace(".py", "")
        spec = spec_from_file_location(module_name, file_path)
        if spec is None or spec.loader is None:
            raise ValueError(f"Could not create module spec for {file_path}")

        module = module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
        except ModuleNotFoundError as e:
            raise ModuleNotFoundError(
                f"Failed to load '{file_path}' due to missing module: {str(e)}. "
                f"Ensure all imports are valid. Current sys.path: {sys.path}"
            ) from e
        return find_pynenc_instance_in_module(module)
    else:
        # Try to import as a module
        try:
            module = importlib.import_module(app_spec)
        except ModuleNotFoundError:
            try:
                module = import_module_as_file(app_spec, ModuleNotFoundError())
            except ModuleNotFoundError as e:
                raise ValueError(
                    f"Could not import module '{app_spec}'. Ensure it’s a valid module path "
                    f"or provide a file path (e.g., 'path/to/backtes.py').\n"
                    f"Error: {str(e)}"
                ) from e
        return find_pynenc_instance_in_module(module)


def extract_module_info(app: "Pynenc") -> tuple[str | None, str | None]:
    """
    Extract module filepath and app variable name from a Pynenc instance.

    :param app: Pynenc application instance
    :return: Tuple of (module_filepath, app_variable_name)
    """
    import inspect
    import sys

    module_filepath = None
    app_variable = None

    try:
        if app.__module__ != "__main__":
            module = sys.modules.get(app.__module__)
            if module and hasattr(module, "__file__"):
                module_filepath = module.__file__

                # Find the variable name holding the app instance
                for var_name, var_val in inspect.getmembers(module):
                    if var_val is app and not var_name.startswith("_"):
                        app_variable = var_name
                        break
    except Exception:
        # Ignore errors when trying to discover module information
        pass

    return module_filepath, app_variable


def create_app_from_info(app_info: AppInfo) -> Optional[Pynenc]:
    """
    Create a Pynenc app instance from AppInfo.

    This function delegates to Pynenc.from_info class method.

    :param app_info: Application metadata
    :return: Re-hydrated Pynenc app instance
    :raises ValueError: If app can't be re-hydrated
    """
    import logging

    logger = logging.getLogger(__name__)

    # Strategy 1: Try to import the module and get the variable if available
    if (
        app_info.module != "__main__"
        and app_info.module_filepath
        and app_info.app_variable
    ):
        try:
            # Ensure module directory is in sys.path
            module_dir = os.path.dirname(app_info.module_filepath)
            if module_dir not in sys.path:
                sys.path.insert(0, module_dir)

            # Import the module by name
            module = importlib.import_module(app_info.module)

            # Get the app instance from the module
            if hasattr(module, app_info.app_variable):
                app = getattr(module, app_info.app_variable)
                if isinstance(app, Pynenc) and app.app_id == app_info.app_id:
                    logger.info(
                        f"Found app instance {app_info.app_id} in module {app_info.module}"
                    )
                    return app
        except (ImportError, AttributeError) as e:
            logger.debug(f"Could not import module {app_info.module}: {e}")

    # Strategy 2: Search for the app in already imported modules
    if app_info.module != "__main__":
        for module_name, module in list(sys.modules.items()):
            if module_name.startswith("_"):
                continue

            try:
                for attr_name in dir(module):
                    if attr_name.startswith("_"):
                        continue

                    try:
                        attr = getattr(module, attr_name)
                        if isinstance(attr, Pynenc) and attr.app_id == app_info.app_id:
                            logger.info(
                                f"Found app instance {app_info.app_id} in module {module_name}"
                            )
                            return attr
                    except (AttributeError, TypeError):
                        continue
            except (AttributeError, ImportError):
                continue
    return None
