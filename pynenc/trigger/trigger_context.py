"""
Context for trigger evaluation in Pynenc.

This module defines the TriggerContext class which collects valid conditions
and provides access to their contexts for trigger evaluation.
"""

from datetime import datetime, timezone
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from pynenc.trigger.conditions import TriggerCondition, ValidCondition
    from pynenc.trigger.conditions.base import ConditionContext


C = TypeVar("C", bound="ConditionContext")


class TriggerContext:
    """
    Context for trigger evaluation.

    Contains a collection of valid conditions (satisfied conditions with their contexts),
    which are used to evaluate whether a trigger definition should execute.
    """

    def __init__(
        self,
        timestamp: datetime | None = None,
        valid_conditions: dict[str, "ValidCondition"] | None = None,
    ) -> None:
        """
        Initialize the trigger context.

        :param timestamp: Time of context creation, defaults to current time
        :param valid_conditions: Dictionary of initial valid conditions, if any
        """
        self.timestamp = timestamp or datetime.now(timezone.utc)
        self.valid_conditions = valid_conditions or {}
        self.conditions = self.build_conditions_dict(self.valid_conditions)

    @staticmethod
    def build_conditions_dict(
        valid_conditions: dict[str, "ValidCondition"]
    ) -> dict[str, "TriggerCondition"]:
        """
        Build a mapping from condition IDs to condition objects.

        :param valid_conditions: Dictionary of valid conditions
        :return: Dictionary mapping condition IDs to their condition objects
        """
        return {
            valid_condition.condition.condition_id: valid_condition.condition
            for valid_condition in valid_conditions.values()
        }

    def add_valid_condition(self, valid_condition: "ValidCondition") -> None:
        """
        Add a valid condition to this context.

        :param valid_condition: The valid condition to add
        """
        self.valid_conditions[valid_condition.valid_condition_id] = valid_condition
        self.conditions[
            valid_condition.condition.condition_id
        ] = valid_condition.condition

    def has_condition(self, condition_id: str) -> bool:
        """
        Check if a specific condition is valid in this context.

        :param condition_id: ID of the condition to check
        :return: True if the condition is valid
        """
        return condition_id in self.conditions
