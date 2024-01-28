"""
.. _import_tools:

import_tools.py
===============

Overview
--------
The `import_tools.py` module was developed to address a specific challenge in
task execution within the Pynenc framework. This challenge arises when a task
is defined in a Python module that is executed as a standalone script. Such a
module, when executed directly (via `python script.py` or `python -m module`),
has its `__name__` attribute set to `"__main__"`. This scenario presents a
limitation in standard task creation practices, where `func.__module__` is used.
The module name being `"__main__"` disrupts task instantiation and serialization,
as the task identifier `__main__.task_name` in the initiator script does not match
the distributed environment, where `__main__` refers to the worker itself.

Approach
--------
`import_tools.py` employs an alternative strategy to address this limitation.
It utilizes the relative path from the file where the Pynenc application
is instantiated to the file where the task is defined. This method ensures
accurate location and retrieval of the task file, independent of local
environment differences (filepath may differe from local environment to
Docker images, however relative path between app and task should be
the same).

Current Status
--------------
Currently, `import_tools.py` is not in use due to an unforeseen side effect
of its approach. Importing tasks using relative paths leads to the creation
of parallel task instances in the system, separate from the standard instances.
This results in loss of context information and challenges in maintaining
task consistency and reliability. To preserve simplicity and effectiveness
in task management, and to avoid such complications, the current implementation
does not support importing tasks from modules that can run as scripts.

Note
----
This documentation outlines the design decisions and evolution of the
`import_tools.py` module within the Pynenc framework.
"""


import importlib
import importlib.util
import inspect
import os
import sys
from functools import lru_cache
from typing import Callable, NamedTuple


def get_object_filepath(obj: object) -> str | None:
    """
    Retrieve the file path of the module where a given object was defined.

    :param object obj:
        The object whose module file path is to be found.

    :return: The file path of the module if found, otherwise None.

    ```{note}
    This function is currently not in use in the main Pynenc workflow.
    ```
    """
    return sys.modules[obj.__module__].__file__


def get_pynenc_instance_filepath() -> str:
    """
    Get the file path of the Pynenc and Task instances.

    This method retrieves the file path by inspecting the call stack and finding
    the frame after the last occurrence of a frame that includes 'pynenc' or 'app.py'.

    :return: The file path of the Pynenc or Task instance.
    :raises RuntimeError: If the instance file path cannot be found.

    ```{note}
    This function is currently not in use in the main Pynenc workflow.
    ```
    """
    if frame := get_frame_after(["pynenc", "app.py"]):
        return frame.filename
    raise RuntimeError("Could not find instance filepath")


def get_frame_after(ends_with: list[str]) -> inspect.FrameInfo | None:
    """
    Get the first frame in the call stack after the last occurrence of a frame ending with a specified path.

    :param list[str] ends_with:
        The paths to search for in the call stack.

    :return: The frame information after the last occurrence of the specified path, or None if not found.

    ```{note}
    This function is currently not in use in the main Pynenc workflow.
    ```
    """
    stack = inspect.stack()
    last_occurrence_index = None

    for i, frame_info in enumerate(stack):
        if frame_info.filename.endswith(os.path.join(*ends_with)):
            last_occurrence_index = i

    if last_occurrence_index is not None and last_occurrence_index + 1 < len(stack):
        return stack[last_occurrence_index + 1]

    return None


def get_module_from_path(relative_path: str) -> str:
    """
    Convert a relative file path to a module path format.

    :param str relative_path:
        The relative file path to be converted.

    :return: The converted module path in dot-separated format.

    ```{note}
    This function is currently not in use in the main Pynenc workflow.
    ```
    """
    if relative_path == ".":
        return ""
    return relative_path.replace(os.path.sep, ".").replace(".py", "")


class TaskModules(NamedTuple):
    app_module: str
    task_module: str


@lru_cache(maxsize=None)
def get_task_modules(app_filepath: str, task_filepath: str) -> TaskModules:
    """
    Determine the module names for the application and task based on their file paths.

    This function calculates the common path between the application and task file paths
    and then derives the relative module names for both.

    :param str app_filepath:
        The file path of the application.
    :param str task_filepath:
        The file path of the task.

    :return: A named tuple containing the module names for the application and task.

    ```{note}
    This function is currently not in use in the main Pynenc workflow.
    ```
    """
    # the module of the taask requires
    # self.app.instance_filepath and self.instance_filepath
    # 1st determine common path between the two files
    common_path = os.path.commonpath([app_filepath, task_filepath])
    # 2nd determine the relative paths from the common path to app and task
    app_relative_path = os.path.relpath(app_filepath, common_path)
    task_relative_path = os.path.relpath(task_filepath, common_path)
    # 3rd return the module name (path without os separators)
    app_module = get_module_from_path(app_relative_path)
    task_module = get_module_from_path(task_relative_path)
    return TaskModules(app_module, task_module)


def get_base_path(absolute_path: str, relative_path: str) -> str:
    """
    Extract the base path from an absolute path given a relative path segment.

    :param str absolute_path:
        The absolute path from which the base path is to be extracted.
    :param str relative_path:
        The relative path segment used for extraction.

    :return: The base path extracted from the absolute path.

    ```{note}
    This function is currently not in use in the main Pynenc workflow.
    ```
    """
    # Normalize paths
    absolute_path = os.path.normpath(absolute_path)
    relative_path = os.path.normpath(relative_path)

    # Find the start index of the relative path in the absolute path
    start_index = absolute_path.rfind(relative_path)

    # Extract the base path
    base_path = absolute_path[:start_index] if start_index != -1 else absolute_path

    return base_path.rstrip(os.sep)  # Remove any trailing separators


class ImportedTask(NamedTuple):
    callable: Callable
    instance_filepath: str


def import_task(
    app_filepath: str, app_module: str, task_module: str, task_name: str
) -> ImportedTask:
    """
    Import a task based on the application and task module paths and the task name.

    This function handles the dynamic import of a task from its module path,
    considering the relative paths of the application and task modules.

    :param str app_filepath:
        The file path of the application.
    :param str app_module:
        The module path of the application.
    :param str task_module:
        The module path of the task.
    :param str task_name:
        The name of the task to be imported.

    :return: A named tuple containing the callable task and its instance file path.
    :raises ImportError: If the task cannot be imported from the specified file path.

    ```{note}
    This function is currently not in use in the main Pynenc workflow.
    ```
    """
    # Transform modules to relative path
    app_relative_path = app_module.replace(".", os.path.sep)
    task_relative_path = task_module.replace(".", os.path.sep)

    # Determine common path
    common_path = get_base_path(app_filepath, app_relative_path)

    # Determine the absolute path to the task
    if task_relative_path:
        task_filepath = os.path.join(common_path, task_relative_path + ".py")
    else:
        task_filepath = common_path + ".py"

    # Import the task using the absolute path
    module_name = (
        task_module or app_module
    )  # Fallback to app_module if task_module is empty
    spec = importlib.util.spec_from_file_location(module_name, task_filepath)
    if not spec or not spec.loader:
        raise ImportError(f"Could not import task {task_name} from {task_filepath}")
    _task_module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = _task_module  # Optionally add to sys.modules
    spec.loader.exec_module(_task_module)
    return ImportedTask(getattr(task_module, task_name), task_filepath)
