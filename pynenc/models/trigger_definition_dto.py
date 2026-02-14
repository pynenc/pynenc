from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pynenc.identifiers.task_id import TaskId
    from pynenc.trigger.conditions import CompositeLogic


@dataclass(frozen=True)
class TriggerDefinitionDTO:
    """Data transfer object for a trigger definition."""

    trigger_id: str
    task_id: "TaskId"
    condition_ids: list[str]
    logic: "CompositeLogic"
    argument_provider_json: str | None
