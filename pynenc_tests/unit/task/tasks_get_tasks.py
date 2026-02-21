"""
Helper module for testing Task.from_id resolution.

This module defines tasks at module level using both @app.task and @app.direct_task
decorators. It simulates the real-world pattern where tasks are defined in
application modules that may be imported by different app instances.

Key components:
- helper_app: A Pynenc app instance used for module-level decoration
- helper_task_func: A function decorated with @app.task (module attr is a Task)
- helper_direct_func: A function decorated with @app.direct_task (module attr is a wrapper)
"""

from pynenc import Pynenc

helper_app = Pynenc(app_id="helper_module")


@helper_app.task
def helper_task_func(a: int, b: int) -> int:
    """Simple addition task decorated with @app.task."""
    return a + b


@helper_app.direct_task
def helper_direct_func(a: int, b: int) -> int:
    """Simple addition task decorated with @app.direct_task."""
    return a + b
