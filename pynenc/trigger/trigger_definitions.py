"""
Trigger definition classes for the Pynenc system.

This module defines the classes used to specify when and how tasks should be triggered.
Each trigger definition links a task to one or more conditions that determine when it executes.
"""

import hashlib
import json
import logging
from typing import TYPE_CHECKING, Any

from pynenc.models.trigger_definition_dto import TriggerDefinitionDTO
from pynenc.trigger.arguments.argument_providers import ArgumentProvider
from pynenc.trigger.conditions import CompositeLogic
from pynenc.trigger.trigger_context import TriggerContext

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.identifiers.task_id import TaskId

logger = logging.getLogger(__name__)


class TriggerDefinition:
    """
    Defines when and how a task should be triggered.

    A trigger definition combines a task with one or more conditions
    and specifies when the task should be executed.
    """

    def __init__(
        self,
        task_id: "TaskId",
        condition_ids: list[str],
        logic: CompositeLogic = CompositeLogic.AND,
        argument_provider: ArgumentProvider | None = None,
    ):
        """
        Create a new trigger definition.

        :param task_id: ID of the task to trigger
        :param condition_ids: IDs of conditions that determine when to trigger the task
        :param logic: Logic to apply when evaluating multiple conditions
        :param argument_provider: Provider for generating arguments dynamically
        """
        self.task_id = task_id
        self.condition_ids = condition_ids
        self.logic = logic
        self.argument_provider = argument_provider
        self.trigger_id = self._generate_trigger_id()

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, TriggerDefinition):
            return False
        return (
            self.task_id == value.task_id
            and self.condition_ids == value.condition_ids
            and self.logic == value.logic
            and self.argument_provider == value.argument_provider
        )

    def _generate_trigger_id(self) -> str:
        """
        Generate a deterministic ID based on the trigger content.

        This ensures the same trigger definitions across different
        runner instances have the same ID.

        :return: A deterministic trigger ID as a hex string
        """
        # Create a hash of the essential trigger content
        hasher = hashlib.sha256()

        # Add task_id, sorted condition_ids, and logic
        hasher.update(self.task_id.key.encode("utf-8"))
        hasher.update(",".join(sorted(self.condition_ids)).encode("utf-8"))
        hasher.update(self.logic.name.encode("utf-8"))

        return hasher.hexdigest()

    def generate_trigger_run_ids(self, trigger_context: TriggerContext) -> list[str]:
        """
        Generate unique IDs for this trigger execution based on the triggering conditions.

        For AND logic: Returns a single ID representing all conditions collectively
        For OR logic: Returns one ID per satisfied condition

        These IDs are used to ensure each valid condition triggers a task exactly once
        across multiple workers.

        :param trigger_context: Context with valid conditions
        :return: List of unique trigger run IDs
        """
        valid_condition_ids = [
            vc.valid_condition_id
            for vc in trigger_context.valid_conditions.values()
            if vc.condition.condition_id in self.condition_ids
        ]
        if not valid_condition_ids:
            raise ValueError(
                "A trigger without valid conditions should not be executed"
            )
        if not self.should_trigger(trigger_context):
            raise ValueError("A trigger that cannot run cannot get a any run ID")

        if self.logic == CompositeLogic.AND:
            hasher = hashlib.sha256()
            hasher.update(self.trigger_id.encode("utf-8"))
            hasher.update(",".join(sorted(valid_condition_ids)).encode("utf-8"))
            return [hasher.hexdigest()]
        else:
            run_ids = []
            for valid_condition_id in valid_condition_ids:
                hasher = hashlib.sha256()
                hasher.update(self.trigger_id.encode("utf-8"))
                hasher.update(valid_condition_id.encode("utf-8"))
                run_ids.append(hasher.hexdigest())
            return run_ids

    def should_trigger(self, trigger_context: TriggerContext) -> bool:
        """
        Determine if the task should be triggered based on the context.

        :param trigger_context: Context with valid conditions
        :return: True if the task should be triggered, False otherwise
        """
        if not self.condition_ids:
            return False

        # Check each condition against the trigger context
        results = []
        for condition_id in self.condition_ids:
            # If condition is satisfied (present in trigger context)
            has_condition = trigger_context.has_condition(condition_id)
            results.append(has_condition)

        # Apply appropriate logic
        if self.logic == CompositeLogic.AND:
            return all(results)
        return any(results)  # OR logic

    def get_arguments(self, trigger_context: TriggerContext) -> dict[str, Any]:
        """
        Get the arguments to pass to the task when triggered.

        Uses the argument provider to generate arguments based on the trigger context.

        :param trigger_context: Context that triggered this definition
        :return: Arguments to pass to the task
        """
        if not self.argument_provider:
            return {}
        return self.argument_provider.get_arguments(trigger_context)

    def to_dto(self, app: "Pynenc") -> TriggerDefinitionDTO:
        """
        Serialize this trigger definition to a DTO.

        :param app: Pynenc application instance
        :return: DTO representation of this trigger definition
        """
        return TriggerDefinitionDTO(
            trigger_id=self.trigger_id,
            task_id=self.task_id,
            condition_ids=self.condition_ids,
            logic=self.logic,
            argument_provider_json=self.argument_provider.to_json(app)
            if self.argument_provider
            else None,
        )

    @classmethod
    def from_dto(cls, dto: TriggerDefinitionDTO, app: "Pynenc") -> "TriggerDefinition":
        """
        Create a trigger definition instance from a DTO.

        :param dto: DTO containing serialized trigger definition
        :param app: Pynenc application instance for deserializing conditions
        :return: A new TriggerDefinition instance
        :raises ValueError: If the JSON data is invalid
        """
        try:
            argument_provider = (
                ArgumentProvider.from_json(dto.argument_provider_json, app)
                if dto.argument_provider_json
                else None
            )
            return cls(
                task_id=dto.task_id,
                condition_ids=dto.condition_ids,
                logic=dto.logic,
                argument_provider=argument_provider,
            )

        except json.JSONDecodeError as e:
            raise ValueError("Invalid JSON for trigger definition") from e
