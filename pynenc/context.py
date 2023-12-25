from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from .invocation import DistributedInvocation, SynchronousInvocation

# invocation_context keeps the current invocation, so it can be referenced as a parent from any sub-invocation
sync_inv_context: dict[str, Optional["SynchronousInvocation"]] = {}
dist_inv_context: dict[str, Optional["DistributedInvocation"]] = {}
# runner_args keeps the arguments passed from the runner to a distributed invocation:
# - this is necessary for parent-subprocess communication on ProcessRunner
runner_args: Optional[dict[str, Any]] = None
