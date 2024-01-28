"""
This module maintains the context of invocations and runners within the Pynenc application.

It stores the current invocation context and runner arguments,
facilitating the management and tracking of nested or sub-invocations within different execution environments.
"""
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from pynenc.invocation.dist_invocation import DistributedInvocation
    from pynenc.invocation.sync_invocation import SynchronousInvocation

# invocation_context keeps the current invocation, so it can be referenced as a parent from any sub-invocation
sync_inv_context: dict[str, Optional["SynchronousInvocation"]] = {}
dist_inv_context: dict[str, Optional["DistributedInvocation"]] = {}

# runner_args keeps the arguments passed from the runner to a distributed invocation:
# - this is necessary for parent-subprocess communication on ProcessRunner
runner_args: Optional[dict[str, Any]] = None
