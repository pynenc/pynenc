from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pynenc.identifiers.call_id import CallId


@dataclass(frozen=True)
class CallDTO:
    """Data transfer object for a task call.

    Serialized arguments may contain either inline serialized values
    or ClientDataStore reference keys (decided by the state backend
    during ``upsert_invocations``).

    :param CallId call_id: Structured identifier for the call (task_id + args_id)
    :param dict[str, str] serialized_arguments: Argument name → serialized value
    """

    call_id: "CallId"
    serialized_arguments: dict[str, str] = field(default_factory=dict)
