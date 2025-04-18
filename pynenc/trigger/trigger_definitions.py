"""
Trigger definition classes for the Pynenc system.

This module defines the classes used to specify when and how tasks should be triggered.
Each trigger definition links a task to one or more conditions that determine when it executes.
"""

import hashlib
import json
import logging
from typing import TYPE_CHECKING, Any, Optional

from pynenc.trigger.arguments.argument_providers import ArgumentProvider
from pynenc.trigger.conditions import CompositeLogic
from pynenc.trigger.trigger_context import TriggerContext

if TYPE_CHECKING:
    from pynenc.app import Pynenc

logger = logging.getLogger(__name__)


class TriggerDefinition:
    """
    Defines when and how a task should be triggered.

    A trigger definition combines a task with one or more conditions
    and specifies when the task should be executed.
    """

    def __init__(
        self,
        task_id: str,
        condition_ids: list[str],
        logic: CompositeLogic = CompositeLogic.AND,
        argument_provider: Optional[ArgumentProvider] = None,
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
        hasher.update(self.task_id.encode("utf-8"))
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

    def to_json(self, app: "Pynenc") -> str:
        """
        Serialize this trigger definition to a JSON string.

        :param app: Pynenc application instance
        :return: JSON string representation of this trigger definition
        """
        data = {
            "trigger_id": self.trigger_id,
            "task_id": self.task_id,
            "condition_ids": self.condition_ids,
            "logic": self.logic.value,
        }
        if self.argument_provider:
            data["argument_provider"] = self.argument_provider.to_json(app)
        return json.dumps(data)

    @classmethod
    def from_json(cls, json_str: str, app: "Pynenc") -> "TriggerDefinition":
        """
        Create a trigger definition instance from a JSON string.

        :param json_str: JSON string containing serialized trigger definition
        :param app: Pynenc application instance for deserializing conditions
        :return: A new TriggerDefinition instance
        :raises ValueError: If the JSON data is invalid
        """
        try:
            data = json.loads(json_str)

            task_id = data.get("task_id")
            trigger_id = data.get("trigger_id")
            if not task_id:
                raise ValueError("Missing task_id in trigger definition")
            if not trigger_id:
                raise ValueError("Missing trigger_id in trigger definition")
            condition_ids = data.get("condition_ids", [])
            logic = CompositeLogic(data.get("logic"))
            argument_provider = None
            if "argument_provider" in data:
                argument_provider = ArgumentProvider.from_json(
                    data["argument_provider"], app
                )

            return cls(
                task_id=task_id,
                condition_ids=condition_ids,
                logic=logic,
                argument_provider=argument_provider,
            )

        except json.JSONDecodeError as e:
            raise ValueError("Invalid JSON for trigger definition") from e
