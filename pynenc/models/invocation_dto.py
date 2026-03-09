from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pynenc.identifiers.call_id import CallId
    from pynenc.identifiers.invocation_id import InvocationId
    from pynenc.workflow.workflow_identity import WorkflowIdentity


@dataclass(frozen=True)
class InvocationDTO:
    """Data transfer object for a distributed invocation.

    Carries only invocation-level identity fields. References the call
    by ``call_id`` / ``task_id`` rather than embedding a ``CallDTO``.
    Parent invocations are referenced by ID only (flat, no recursion).

    The state backend pairs this with a ``CallDTO`` when persisting;
    the two DTOs are independent so backends can store them in separate
    tables or collections if desired.

    :param str invocation_id: Unique invocation identifier
    :param CallId call_id: Call identifier (links to CallDTO)
    :param WorkflowIdentity workflow: Workflow identity
    :param InvocationReference | None parent_reference: Optional reference to parent invocation
    """

    invocation_id: "InvocationId"
    call_id: "CallId"
    workflow: "WorkflowIdentity"
    parent_invocation_id: "InvocationId | None"
