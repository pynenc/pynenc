"""
This module maintains the context of invocations and runners within the Pynenc application.

It stores the current invocation context and runner arguments,
facilitating the management and tracking of nested or sub-invocations within different execution environments.
"""
import threading
from typing import TYPE_CHECKING, Any, Optional

# Create a thread-local data storage
thread_local = threading.local()

if TYPE_CHECKING:
    from pynenc.invocation.conc_invocation import ConcurrentInvocation
    from pynenc.invocation.dist_invocation import DistributedInvocation
    from pynenc.runner.base_runner import BaseRunner

# invocation_context keeps the current invocation, so it can be referenced as a parent from any sub-invocation
# - It is a dictionary with the format {app_id: invocation}
sync_inv_context: dict[str, Optional["ConcurrentInvocation"]] = {}
# Global runner dictionary keyed by app_id
_current_runner: dict[str, "BaseRunner"] = {}


def get_dist_invocation_context(app_id: str) -> Optional["DistributedInvocation"]:
    """
    Get the current invocation context for the given app.

    :param str app_id: The app identifier.
    :result: The current invocation context for the given app.
    """
    if not hasattr(thread_local, "dist_inv_context"):
        thread_local.dist_inv_context = {}
    return thread_local.dist_inv_context.get(app_id)


def swap_dist_invocation_context(
    app_id: str, invocation: Optional["DistributedInvocation"]
) -> Optional["DistributedInvocation"]:
    """
    Set the current invocation context for the given app and returns the previous.

    :param str app_id: The app identifier.
    :param DistributedInvocation invocation: The invocation to set as the current context.
    """
    previous_invocation = get_dist_invocation_context(app_id)
    thread_local.dist_inv_context[app_id] = invocation
    return previous_invocation


# Runner arguments passed from the runner to a distributed invocation.
runner_args: Optional[dict[str, Any]] = None


def get_current_runner(app_id: str) -> Optional["BaseRunner"]:
    """
    Retrieve the current runner for the given app_id from thread-local storage.

    This function allows each thread or process to access its own runner instance,
    which is critical in multi-process environments like MultiThreadRunner where
    each process runs a ThreadRunner and needs to reference its own runner instance
    without conflicting with others.

    :param app_id: The application identifier.
    :return: The current runner instance if set in the current thread/process, else None.
    """
    return _current_runner.get(app_id)


def set_current_runner(app_id: str, runner: "BaseRunner") -> None:
    """
    Set the current runner for the given app_id in thread-local storage.

    This is used to associate a runner with the current thread or process context,
    enabling isolated execution environments. It's particularly essential for
    MultiThreadRunner, where each spawned process needs to set its own ThreadRunner
    to avoid cross-process interference.

    :param app_id: The application identifier.
    :param runner: The runner instance to set.
    """
    _current_runner[app_id] = runner
