"""
Helper module for testing Task.from_id resolution.

This module defines tasks at module level using both @app.task and @app.direct_task
decorators. It simulates the real-world pattern where tasks are defined in
application modules that may be imported by different app instances.

Key components:
- helper_app: A Pynenc app instance used for module-level decoration
- helper_task_func: A function decorated with @app.task (module attr is a Task)
- helper_direct_func: A function decorated with @app.direct_task (module attr is a wrapper)
- helper_wrapped_func: A function wrapped in a StatusBase-like object (has pynenc_task attr)
"""

from pynenc import Pynenc, Task

helper_app = Pynenc(app_id="helper_module")


@helper_app.task
def helper_task_func(a: int, b: int) -> int:
    """Simple addition task decorated with @app.task."""
    return a + b


@helper_app.direct_task
def helper_direct_func(a: int, b: int) -> int:
    """Simple addition task decorated with @app.direct_task."""
    return a + b


class _TaskWrapper:
    """Minimal StatusBase-like wrapper: holds a pynenc_task attribute."""

    def __init__(self, pynenc_task: "Task") -> None:
        self.pynenc_task = pynenc_task


@helper_app.task
def helper_wrapped_func(a: int, b: int) -> int:
    """Function wrapped in a StatusBase-like object."""
    return a + b


# Replace the module attribute with a wrapper (simulates StatusBase).
# The Task is still reachable via wrapper.pynenc_task.
helper_wrapped_func = _TaskWrapper(pynenc_task=helper_wrapped_func)  # type: ignore[assignment]
