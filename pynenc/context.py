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
    from pynenc.invocation.dist_invocation import DistributedInvocation
    from pynenc.invocation.sync_invocation import SynchronousInvocation

# invocation_context keeps the current invocation, so it can be referenced as a parent from any sub-invocation
# - It is a dictionary with the format {app_id: invocation}
sync_inv_context: dict[str, Optional["SynchronousInvocation"]] = {}


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


# runner_args keeps the arguments passed from the runner to a distributed invocation:
# - this is necessary for parent-subprocess communication on ProcessRunner
runner_args: Optional[dict[str, Any]] = None
