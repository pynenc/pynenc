"""
Builder classes for constructing trigger definitions.

This module provides a fluent interface for building trigger definitions,
making it easier to define complex triggering conditions for tasks.
"""

from typing import TYPE_CHECKING, Any, Callable, TypeVar, cast

from pynenc.invocation.status import InvocationStatus
from pynenc.trigger.arguments.argument_filters import create_argument_filter
from pynenc.trigger.arguments.argument_providers import (
    ArgumentProvider,
    CompositeArgumentProvider,
    ContextCallable,
    ContextTypeArgumentProvider,
    DirectArgumentProvider,
    StaticArgumentProvider,
)
from pynenc.trigger.arguments.result_filter import NoResultFilter, create_result_filter
from pynenc.trigger.conditions import (
    CompositeLogic,
    CronCondition,
    EventCondition,
    EventContext,
    StatusCondition,
    StatusContext,
)
from pynenc.trigger.conditions.base import ConditionContext
from pynenc.trigger.conditions.exception import ExceptionCondition, ExceptionContext
from pynenc.trigger.conditions.result import ResultCondition, ResultContext
from pynenc.trigger.trigger_definitions import TriggerDefinition

if TYPE_CHECKING:
    from pynenc.task import Task
    from pynenc.trigger.conditions.base import TriggerCondition
    from pynenc.trigger.trigger_context import TriggerContext

C = TypeVar("C", bound="ConditionContext")


class TriggerBuilder:
    """
    Builder class for creating trigger definitions.

    Provides a fluent interface for defining when and how tasks should be triggered,
    with methods for different trigger types and condition combinations.
    """

    def __init__(self) -> None:
        """Initialize an empty trigger builder."""
        self.conditions: list[TriggerCondition] = []
        self.logic: CompositeLogic = CompositeLogic.AND
        self.argument_providers: list[ArgumentProvider] = []

    def on_cron(self, cron_expression: str) -> "TriggerBuilder":
        """
        Add a cron-based condition to trigger the task.

        :param cron_expression: Standard cron expression (e.g., "0 0 * * *" for daily at midnight)
        :return: This builder for method chaining
        """
        self.conditions.append(CronCondition(cron_expression))
        return self

    def on_event(
        self,
        event_code: str,
        payload_filter: dict[str, Any] | Callable[[dict[str, Any]], bool] | None = None,
    ) -> "TriggerBuilder":
        """
        Add an event-based condition to trigger the task.

        :param event_code: Type of event to listen for
        :param payload_filter: Optional payload arguments to match specific events or boolean callable
        :return: This builder for method chaining
        """
        self.conditions.append(
            EventCondition(event_code, create_argument_filter(payload_filter))
        )
        return self

    def on_status(
        self,
        task: "Task",
        statuses: str
        | list[str]
        | InvocationStatus
        | list[InvocationStatus] = InvocationStatus.SUCCESS,
        call_arguments: dict[str, Any] | Callable[[dict[str, Any]], bool] | None = None,
    ) -> "TriggerBuilder":
        """
        Add a task status condition to trigger after another task completes.

        :param task: The task to monitor
        :param statuses: Status(es) that trigger execution (default: InvocationStatus.SUCCESS)
        :param call_arguments: Optional arguments to match specific calls
        :return: This builder for method chaining
        """
        # Handle single status as list to standardize
        if not isinstance(statuses, list):
            statuses = [statuses]

        _statuses: list[InvocationStatus] = []
        for status in statuses:
            if isinstance(status, str):
                status = InvocationStatus(status.lower())
            _statuses.append(status)

        self.conditions.append(
            StatusCondition(
                task.task_id, _statuses, create_argument_filter(call_arguments)
            )
        )
        return self

    def on_any_result(
        self,
        task: "Task",
        call_arguments: dict[str, Any] | Callable[[dict[str, Any]], bool] | None = None,
    ) -> "TriggerBuilder":
        """
        Add a result condition to trigger when a task finishes successfully.

        :param task: The task to monitor for results
        :param call_arguments: Optional filter for task arguments to match specific invocations
        :return: This builder for method chaining
        """
        self.conditions.append(
            ResultCondition(
                task.task_id,
                create_argument_filter(call_arguments),
                NoResultFilter(),
            )
        )
        return self

    def on_result(
        self,
        task: "Task",
        filter_result: Any | Callable[[Any], bool],
        call_arguments: dict[str, Any] | Callable[[dict[str, Any]], bool] | None = None,
    ) -> "TriggerBuilder":
        """
        Add a result condition to trigger when a task returns a specific result or matches a filter.

        This method allows defining conditions that trigger based on task results in several ways:
        - Exact matching: Pass any value (including None) to match exactly
        - Custom filtering: Pass a function that evaluates the result and returns a boolean
        - Ignore results: Set ignore_result=True to accept any result value

        :param task: The task to monitor for results
        :param filter_result: Either a specific value to match or a callable that filters results
        :param call_arguments: Optional filter for task arguments to match specific invocations
        :return: This builder for method chaining
        """
        self.conditions.append(
            ResultCondition(
                task.task_id,
                create_argument_filter(call_arguments),
                create_result_filter(filter_result),
            )
        )
        return self

    def on_exception(
        self,
        task: "Task",
        exception_types: list[str] | str | None = None,
        call_arguments: dict[str, Any] | Callable[[dict[str, Any]], bool] | None = None,
    ) -> "TriggerBuilder":
        """
        Add an exception condition to trigger when a task fails with specific exceptions.

        This method creates a condition that triggers when a monitored task fails with
        a specific exception type or any exception if none specified.

        :param task: The task to monitor for exceptions
        :param exception_types: Exception type name(s) to match, None to match any exception
        :param call_arguments: Optional filter for task arguments to match specific invocations
        :return: This builder for method chaining
        """
        if exception_types is None:
            exception_list = []
        elif isinstance(exception_types, str):
            exception_list = [exception_types]
        else:
            exception_list = exception_types
        self.conditions.append(
            ExceptionCondition(
                task.task_id,
                create_argument_filter(call_arguments),
                exception_list,
            )
        )
        return self

    def with_logic(self, logic: CompositeLogic | str) -> "TriggerBuilder":
        """
        Set the logic for combining multiple conditions.

        :param logic: Logic to use (AND or OR)
        :return: This builder for method chaining
        """
        if isinstance(logic, str):
            logic = CompositeLogic(logic.lower())
        self.logic = logic
        return self

    def with_arguments(self, args: dict[str, Any]) -> "TriggerBuilder":
        """
        Set static arguments to pass to the task when triggered.

        This is a legacy method - prefer with_args_static for new code.

        :param args: Dictionary of static arguments to pass to the task
        :return: This builder for method chaining
        """
        return self.with_args_static(args)

    def with_args_static(self, args: dict[str, Any]) -> "TriggerBuilder":
        """
        Set static arguments to pass to the task when triggered.

        :param args: Dictionary of static arguments to pass to the task
        :return: This builder for method chaining
        """
        self.argument_providers.append(StaticArgumentProvider(args))
        return self

    def with_args_from_event(
        self,
        callback: Callable[[EventContext], dict[str, Any]],
    ) -> "TriggerBuilder":
        """
        Define arguments based on the event context that triggered the task.

        :param callback: Function that receives an EventContext and returns arguments
        :return: This builder for method chaining
        """
        self.argument_providers.append(
            ContextTypeArgumentProvider(
                EventContext, cast(ContextCallable[EventContext], callback)
            )
        )
        return self

    def with_args_from_status(
        self,
        callback: Callable[[StatusContext], dict[str, Any]],
    ) -> "TriggerBuilder":
        """
        Define arguments based on the task status context that triggered this task.

        :param callback: Function that receives a StatusContext and returns arguments
        :return: This builder for method chaining
        """
        self.argument_providers.append(
            ContextTypeArgumentProvider(
                StatusContext, cast(ContextCallable[StatusContext], callback)
            )
        )
        return self

    def with_args_from_result(
        self,
        callback: Callable[[ResultContext], dict[str, Any]],
    ) -> "TriggerBuilder":
        """
        Define arguments based on the result context that triggered this task.

        :param callback: Function that receives a ResultContext and returns arguments
        :return: This builder for method chaining
        """
        self.argument_providers.append(
            ContextTypeArgumentProvider(
                ResultContext, cast(ContextCallable[ResultContext], callback)
            )
        )
        return self

    def with_args_from_exception(
        self,
        callback: Callable[[ExceptionContext], dict[str, Any]],
    ) -> "TriggerBuilder":
        """
        Define arguments based on the exception context that triggered this task.

        The context type is inferred from the callback's type annotation.

        :param callback: Function that receives a specific ConditionContext and returns arguments
        :return: This builder for method chaining
        """
        # Create a provider that knows what context type to use based on the callback type
        self.argument_providers.append(
            ContextTypeArgumentProvider(
                ExceptionContext, cast(ContextCallable[ExceptionContext], callback)
            )
        )
        return self

    def with_args_from_trigger_context(
        self,
        callback: Callable[["TriggerContext"], dict[str, Any]],
    ) -> "TriggerBuilder":
        """
        Define arguments with direct access to the trigger context.

        Provides maximum flexibility by giving direct access to all valid conditions.

        :param callback: Function that receives the TriggerContext and returns arguments
        :param fallback: If True, ignores failures and continues with other providers
        :return: This builder for method chaining
        """
        self.argument_providers.append(DirectArgumentProvider(callback))
        return self

    def with_args_provider(self, provider: ArgumentProvider) -> "TriggerBuilder":
        """
        Add a custom argument provider.

        :param provider: The argument provider to add
        :return: This builder for method chaining
        """
        self.argument_providers.append(provider)
        return self

    def add_condition(self, condition: "TriggerCondition") -> "TriggerBuilder":
        """
        Add a custom condition to the builder.

        :param condition: The condition to add
        :return: This builder for method chaining
        """
        self.conditions.append(condition)
        return self

    def build(self, task_id: str) -> TriggerDefinition:
        """
        Build the trigger definition for a task.

        :param task_id: ID of the task to trigger
        :return: A trigger definition with the configured conditions
        :raises ValueError: If no conditions are defined
        """
        # Handle case with no conditions
        if not self.conditions:
            raise ValueError("Cannot create a trigger definition with no conditions")

        # Extract condition IDs from conditions
        condition_ids = [condition.condition_id for condition in self.conditions]

        # Create a composite provider if there are multiple providers
        argument_provider: ArgumentProvider | None = None
        if len(self.argument_providers) > 1:
            argument_provider = CompositeArgumentProvider(self.argument_providers)
        elif len(self.argument_providers) == 1:
            argument_provider = self.argument_providers[0]
        # If no providers, a default empty provider will be created in TriggerDefinition

        # Create and return the trigger definition
        return TriggerDefinition(
            task_id=task_id,
            condition_ids=condition_ids,
            logic=self.logic,
            argument_provider=argument_provider,
        )


# Define helper functions for common trigger patterns
def on_cron(cron_expression: str) -> TriggerBuilder:
    """
    Create a trigger builder for a cron schedule.

    :param cron_expression: Standard cron expression
    :return: A trigger builder with the cron condition
    """
    return TriggerBuilder().on_cron(cron_expression)


def on_event(
    event_code: str, required_params: dict[str, Any] | None = None
) -> TriggerBuilder:
    """
    Create a trigger builder for an event.

    :param event_code: Type of event to listen for
    :param required_params: Optional parameters that must be present in the event payload
    :return: A trigger builder with the event condition
    """
    return TriggerBuilder().on_event(event_code, required_params)


def on_status(
    task: "Task",
    statuses: str
    | list[str]
    | InvocationStatus
    | list[InvocationStatus] = InvocationStatus.SUCCESS,
    call_arguments: dict[str, Any] | None = None,
) -> TriggerBuilder:
    """
    Create a trigger builder for a task status change.

    :param task: The task to monitor
    :param statuses: Status(es) that trigger execution (default: InvocationStatus.SUCCESS)
    :param call_arguments: Optional arguments to match specific calls
    :return: A trigger builder with the task status condition
    """
    return TriggerBuilder().on_status(task, statuses, call_arguments)
