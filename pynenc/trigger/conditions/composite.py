"""
Composite condition that combines multiple trigger conditions.

This condition allows combining multiple conditions with either AND or OR logic,
enabling complex triggering scenarios where tasks are triggered only when
all conditions are met (AND) or when any condition is met (OR).
"""

import hashlib
from enum import StrEnum, auto
from typing import TYPE_CHECKING, Any, ClassVar, cast

from pynenc.trigger.conditions.base import ConditionContext, TriggerCondition

if TYPE_CHECKING:
    from ...app import Pynenc


class CompositeLogic(StrEnum):
    """Logic operators for combining multiple conditions."""

    AND = auto()
    OR = auto()


class CompositeCondition(TriggerCondition[ConditionContext]):
    """
    Combines multiple trigger conditions with logic operators.

    This condition allows for complex trigger expressions by combining
    multiple conditions using AND/OR logic.
    """

    # Cast to concrete type to satisfy mypy
    context_type: ClassVar[type[ConditionContext]] = cast(
        type[ConditionContext], ConditionContext
    )

    def __init__(
        self,
        conditions: list[TriggerCondition],
        logic: CompositeLogic = CompositeLogic.AND,
    ) -> None:
        """
        Create a composite condition that combines multiple trigger conditions.

        :param conditions: List of trigger conditions to combine
        :param logic: Logic operator to use (AND or OR)
        :raises ValueError: If no conditions are provided
        """
        if not conditions:
            raise ValueError("CompositeCondition requires at least one condition")

        self.conditions = conditions
        self.logic = logic

    @property
    def condition_id(self) -> str:
        """
        Generate a unique ID for this composite condition.

        :return: A string ID based on the logic type and component conditions
        """
        sorted_condition_ids = sorted(cond.condition_id for cond in self.conditions)
        components_str = f"{self.logic.name}:" + ",".join(sorted_condition_ids)
        hash_value = hashlib.md5(components_str.encode()).hexdigest()

        return f"composite:{hash_value}"

    def get_source_task_ids(self) -> set[str]:
        """
        Get the task IDs of all source tasks for this composite condition.

        :return: Set of task IDs
        """
        return {
            task_id
            for cond in self.conditions
            for task_id in cond.get_source_task_ids()
        }

    def _to_json(self, app: "Pynenc") -> dict[str, Any]:
        """
        Create a serializable representation of this composite condition.

        :param app: Pynenc application instance for serializing complex arguments
        :return: Dictionary with serialized condition data
        """
        # Serialize each child condition
        serialized_conditions = []
        for condition in self.conditions:
            serialized_conditions.append(
                {
                    "condition_type": condition.__class__.__name__,
                    "condition_data": condition._to_json(app),
                }
            )

        return {
            "logic": self.logic.name,
            "conditions": serialized_conditions,
        }

    @classmethod
    def _from_json(cls, data: dict[str, Any], app: "Pynenc") -> "CompositeCondition":
        """
        Create a CompositeCondition from parsed JSON data.

        :param data: Dictionary with condition data
        :param app: Pynenc application instance for deserializing complex arguments
        :return: A new CompositeCondition instance
        :raises ValueError: If the data is invalid for this condition type
        """
        logic_str = data.get("logic")
        condition_data_list = data.get("conditions", [])

        # Validate required fields
        if not logic_str:
            raise ValueError("Missing required logic in CompositeCondition data")

        # Parse logic enum
        try:
            logic = CompositeLogic[logic_str]
        except KeyError as e:
            raise ValueError(f"Invalid logic value: {logic_str}") from e

        # Deserialize each child condition
        conditions = []
        for condition_item in condition_data_list:
            condition_type = condition_item.get("condition_type")
            condition_data = condition_item.get("condition_data", {})

            # Find and instantiate the condition class
            condition_class = cls.get_condition_class(condition_type)
            if condition_class is None:
                raise ValueError(f"Unknown condition type: {condition_type}")

            # Create and add the condition
            condition = condition_class._from_json(condition_data, app)
            conditions.append(condition)

        if not conditions:
            raise ValueError("CompositeCondition requires at least one condition")

        return cls(conditions=conditions, logic=logic)

    def _is_satisfied_by(self, context: ConditionContext) -> bool:
        """
        Check if this composite condition is satisfied by the context.

        For AND logic, all child conditions must be satisfied.
        For OR logic, at least one child condition must be satisfied.

        :param context: Context to evaluate conditions against
        :return: True if the composite condition is satisfied
        """
        if not self.conditions:
            return False

        if self.logic == CompositeLogic.AND:
            return all(cond.is_satisfied_by(context) for cond in self.conditions)
        else:  # CompositeLogic.OR
            return any(cond.is_satisfied_by(context) for cond in self.conditions)

    def affects_task(self, task_id: str) -> bool:
        """
        Check if this condition is affected by a specific task.

        A composite condition is affected if any of its child conditions are affected.

        :param task_id: ID of the task to check
        :return: True if any child condition is affected by the task
        """
        return any(cond.affects_task(task_id) for cond in self.conditions)
