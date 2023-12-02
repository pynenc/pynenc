from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from .invocation.dist_invocation import DistributedInvocation

# invocation_context keeps the current invocation, so it can be referenced as a parent from any sub-invocation
invocation_context: dict[str, Optional["DistributedInvocation"]] = {}
# runner_args keeps the arguments passed from the runner to a distributed invocation:
# - this is necessary for parent-subprocess communication on ProcessRunner
runner_args: Optional[dict[str, Any]] = None
