import asyncio
import json
from collections.abc import Iterable
from dataclasses import dataclass
from functools import cached_property, wraps
from logging import Logger
from typing import TYPE_CHECKING, Any, Callable, Optional, Union, overload

from pynenc import context
from pynenc.arg_cache.base_arg_cache import BaseArgCache
from pynenc.broker.base_broker import BaseBroker
from pynenc.conf.config_pynenc import ConfigPynenc
from pynenc.conf.config_task import ConcurrencyControlType
from pynenc.orchestrator.base_orchestrator import BaseOrchestrator
from pynenc.runner.base_runner import BaseRunner
from pynenc.serializer.base_serializer import BaseSerializer
from pynenc.state_backend.base_state_backend import BaseStateBackend
from pynenc.task import Task
from pynenc.trigger.base_trigger import BaseTrigger
from pynenc.util.log import create_logger
from pynenc.util.subclasses import get_subclass

if TYPE_CHECKING:
    from pynenc.arguments import Arguments
    from pynenc.invocation import BaseInvocationGroup
    from pynenc.trigger import TriggerBuilder
    from pynenc.types import Args, Func, Params, Result

    # Type for the parallel function that generates arguments for parallel processing
    ParallelFuncReturn = Union[
        # Option 1: Just return an iterable of arguments (any format)
        Iterable[Union[tuple, dict, Arguments]],
        # Option 2: Return a tuple of (common_args, param_iter) for optimized processing of large shared data
        # This approach pre-serializes common_args once, reducing overhead for large arguments
        # tuple.0 Common arguments shared by all tasks
        # tuple.1 Iterable of dictionaries with task-specific arguments
        tuple[dict[str, Any], Iterable[dict]],
    ]
    ParallelFunc = Callable[[Args], ParallelFuncReturn]

    # Type for the aggregation function that combines results
    AggregateFunc = Callable[[Iterable[Result]], Result]


@dataclass(frozen=True)
class AppInfo:
    """
    Information about a Pynenc application instance.

    Stores metadata required for app discovery and re-instantiation.

    :param app_id: Unique identifier for the application
    :param module: Module path where the app is defined
    :param config_values: Configuration values for app initialization
    :param config_filepath: Path to configuration file, if any
    :param module_filepath: Absolute file path of the module
    :param app_variable: Name of the variable holding the app instance in the module
    """

    app_id: str
    module: str
    config_values: dict[str, Any] | None = None
    config_filepath: str | None = None
    module_filepath: str | None = None
    app_variable: str | None = None

    @classmethod
    def from_app(cls, app: "Pynenc") -> "AppInfo":
        """
        Create AppInfo from a Pynenc app instance.

        Captures necessary metadata for later re-instantiation.

        :param app: The Pynenc app instance
        :return: AppInfo containing app metadata
        """
        from pynenc.util.import_app import extract_module_info

        module_filepath, app_variable = extract_module_info(app)
        return cls(
            app_id=app.app_id,
            config_values=app.config_values,
            config_filepath=app.config_filepath,
            module=app.__module__,
            module_filepath=module_filepath,
            app_variable=app_variable,
        )

    def to_json(self) -> str:
        """
        Serialize AppInfo to JSON string.

        :return: JSON representation of AppInfo
        """
        return json.dumps(
            {
                "app_id": self.app_id,
                "config_values": self.config_values,
                "config_filepath": self.config_filepath,
                "module": self.module,
                "module_filepath": self.module_filepath,
                "app_variable": self.app_variable,
            }
        )

    @classmethod
    def from_json(cls, json_str: str) -> "AppInfo":
        """
        Deserialize AppInfo from JSON string.

        :param json_str: JSON string representation of AppInfo
        :return: AppInfo instance
        """
        data = json.loads(json_str)
        return cls(
            app_id=data["app_id"],
            config_values=data.get("config_values"),
            config_filepath=data.get("config_filepath"),
            module=data["module"],
            module_filepath=data.get("module_filepath"),
            app_variable=data.get("app_variable"),
        )


class Pynenc:
    """
    The main class of the Pynenc library that creates an application object.

    :param Optional[str] app_id:
        The id of the application.
    :param Optional[dict[str, Any]] config_values:
        A dictionary of configuration values.
    :param Optional[str] config_filepath:
        A path to a configuration file.

    ```{note}
    All of these base classes are abstract and cannot be used directly. If none is specified,
    they will default to `MemTaskBroker`, `MemStateBackend`, etc. These default classes do not
    actually distribute the code but are helpers for tests or for running an application on your
    localhost. They may help to parallelize to some degree but cannot be used in a production system.
    ```
    """

    def __init__(
        self,
        app_id: str | None = None,
        config_values: Optional[dict[str, Any]] = None,
        config_filepath: Optional[str] = None,
    ) -> None:
        self._app_id = app_id
        self.config_values = config_values
        self.config_filepath = config_filepath
        self.reporting = None
        self._runner_instance: Optional[BaseRunner] = None
        self._tasks: dict[str, Task] = {}
        self.logger.info(f"Initialized Pynenc app with id {self.app_id}")
        self.state_backend.store_app_info(AppInfo.from_app(self))

    @classmethod
    def from_info(cls, app_info: AppInfo) -> "Pynenc":
        """
        Create a Pynenc app instance from AppInfo.

        :param app_info: The AppInfo object containing app metadata
        :return: A new Pynenc app instance
        """
        from pynenc.util.import_app import create_app_from_info

        if app := create_app_from_info(app_info):
            return app
        return cls(
            app_id=app_info.app_id,
            config_values=app_info.config_values,
            config_filepath=app_info.config_filepath,
        )

    @property
    def app_id(self) -> str:
        return self._app_id or self.conf.app_id

    @property
    def tasks(self) -> dict[str, Task]:
        """
        Get the dictionary of registered tasks.

        :return: A dictionary mapping task_id to Task instances.
        """
        return self._tasks

    def get_task(self, task_id: str) -> Task:
        """
        Get a task by its ID.

        :param task_id: The ID of the task to retrieve.
        :return: The Task instance if found, None otherwise.

        warning it may overwrite the options
        """
        if task_id not in self._tasks:
            self._tasks[task_id] = Task.from_id(self, task_id)
        return self._tasks[task_id]

    def __getstate__(self) -> dict:
        # Return state as a dictionary and a secondary value as a tuple
        return {
            "app_id": self.app_id,
            "config_values": self.config_values,
            "config_filepath": self.config_filepath,
            "reporting": self.reporting,
            # "tasks": self._tasks,
        }

    def __setstate__(self, state: dict) -> None:
        # Restore instance attributes
        self._app_id = state["app_id"]
        object.__setattr__(self, "_app_id", self._app_id)
        self.config_values = state["config_values"]
        self.config_filepath = state["config_filepath"]
        self.reporting = state["reporting"]
        # self._tasks = state.get("tasks", {})
        self._runner_instance = None

    @cached_property
    def conf(self) -> ConfigPynenc:
        return ConfigPynenc(
            config_values=self.config_values, config_filepath=self.config_filepath
        )

    @cached_property
    def logger(self) -> Logger:
        return create_logger(self)

    @cached_property
    def orchestrator(self) -> BaseOrchestrator:
        return get_subclass(BaseOrchestrator, self.conf.orchestrator_cls)(self)  # type: ignore # mypy issue #4717

    @cached_property
    def trigger(self) -> BaseTrigger:
        return get_subclass(BaseTrigger, self.conf.trigger_cls)(self)  # type: ignore # mypy issue #4717

    @cached_property
    def broker(self) -> BaseBroker:
        return get_subclass(BaseBroker, self.conf.broker_cls)(self)  # type: ignore # mypy issue #4717

    @cached_property
    def state_backend(self) -> BaseStateBackend:
        return get_subclass(BaseStateBackend, self.conf.state_backend_cls)(self)  # type: ignore # mypy issue #4717

    @cached_property
    def serializer(self) -> BaseSerializer:
        return get_subclass(BaseSerializer, self.conf.serializer_cls)()  # type: ignore # mypy issue #4717

    @cached_property
    def arg_cache(self) -> BaseArgCache:
        return get_subclass(BaseArgCache, self.conf.arg_cache_cls)(self)  # type: ignore # mypy issue #4717

    @property
    def runner(self) -> BaseRunner:
        """
        Get the runner for this app, prioritizing thread/process-specific context.

        First, it checks the thread-local context for a runner (via get_current_runner).
        This is crucial in the MultiThreadRunner, where each process runs a ThreadRunner
        and needs to use its own runner instance rather than the app's default.

        If no context runner exists, it falls back to the
        instance-level runner. This mechanism ensures correct runner isolation
        across threads and processes.

        :return: The runner instance for the current context or the app instance.
        """
        # Check if there's a runner in the context
        if context_runner := context.get_current_runner(self.app_id):
            return context_runner

        # Fall back to instance-level runner
        if self._runner_instance is None:
            self._runner_instance = get_subclass(BaseRunner, self.conf.runner_cls)(self)  # type: ignore
        return self._runner_instance

    @runner.setter
    def runner(self, runner_instance: BaseRunner) -> None:
        self._runner_instance = runner_instance

    def purge(self) -> None:
        """Purge all data from the broker and state backend"""
        self.broker.purge()
        self.orchestrator.purge()
        self.state_backend.purge()
        self.arg_cache.purge()
        self.trigger.purge()

    @overload
    def task(
        self,
        func: "Func",
        *,
        parallel_batch_size: Optional[int] = None,
        retry_for: Optional[tuple[type[Exception], ...]] = None,
        max_retries: Optional[int] = None,
        running_concurrency: Optional[ConcurrencyControlType] = None,
        registration_concurrency: Optional[ConcurrencyControlType] = None,
        key_arguments: Optional[tuple[str, ...]] = None,
        on_diff_non_key_args_raise: Optional[bool] = None,
        call_result_cache: Optional[bool] = None,
        disable_cache_args: Optional[tuple[str, ...]] = None,
        triggers: Union["TriggerBuilder", list["TriggerBuilder"]] | None = None,
        force_new_workflow: Optional[bool] = None,
    ) -> "Task":
        ...

    @overload
    def task(
        self,
        func: None = None,
        *,
        parallel_batch_size: Optional[int] = None,
        retry_for: Optional[tuple[type[Exception], ...]] = None,
        max_retries: Optional[int] = None,
        running_concurrency: Optional[ConcurrencyControlType] = None,
        registration_concurrency: Optional[ConcurrencyControlType] = None,
        key_arguments: Optional[tuple[str, ...]] = None,
        on_diff_non_key_args_raise: Optional[bool] = None,
        call_result_cache: Optional[bool] = None,
        disable_cache_args: Optional[tuple[str, ...]] = None,
        triggers: Union["TriggerBuilder", list["TriggerBuilder"]] | None = None,
        force_new_workflow: Optional[bool] = None,
    ) -> Callable[["Func"], "Task"]:
        ...

    def task(
        self,
        func: Optional["Func"] = None,
        *,
        parallel_batch_size: Optional[int] = None,
        retry_for: Optional[tuple[type[Exception], ...]] = None,
        max_retries: Optional[int] = None,
        running_concurrency: Optional[ConcurrencyControlType] = None,
        registration_concurrency: Optional[ConcurrencyControlType] = None,
        key_arguments: Optional[tuple[str, ...]] = None,
        on_diff_non_key_args_raise: Optional[bool] = None,
        call_result_cache: Optional[bool] = None,
        disable_cache_args: Optional[tuple[str, ...]] = None,
        triggers: Union["TriggerBuilder", list["TriggerBuilder"]] | None = None,
        force_new_workflow: Optional[bool] = None,
    ) -> "Task" | Callable[["Func"], "Task"]:
        """
        The task decorator converts the function into an instance of a BaseTask. It accepts any kind of options,
        however these options will be validated with the options class assigned to the class.

        :param Optional[Callable] func:
            The function to be converted into a Task instance.
        :param Optional[int] parallel_batch_size:
            If set to 0, auto parallelization is disabled. If greater than 0, tasks with iterable
            arguments are automatically split into chunks.
        :param Optional[Tuple[Exception, ...]] retry_for:
            Exceptions for which the task should be retried.
        :param Optional[int] max_retries:
            The maximum number of retries for a task.
        :param Optional[ConcurrencyControlType] running_concurrency:
            Controls the concurrency behavior of the task.
        :param Optional[ConcurrencyControlType] registration_concurrency:
            Manages task registration concurrency.
        :param Optional[Tuple[str, ...]] key_arguments:
            Key arguments for concurrency control.
        :param Optional[bool] on_diff_non_key_args_raise:
            If True, raises an exception for task invocations with matching key arguments but
            different non-key arguments.
        :param Optional[bool] call_result_cache:
            If True, it will return the latest result of a Task with the same arguments if availble,
            otherwise it will trigger a new invocation as expected.
        :param Optional[tuple[str, ...]] disable_cache_args:
            Arguments to exclude from caching, it will accept "*" to disable caching for all arguments.
        :param Union[TriggerBuilder, list[TriggerBuilder]] | None triggers:
            Trigger definitions that determine when this task should execute automatically.
            Can be a single TriggerBuilder or a list of builders for multiple trigger conditions.
        :param Optional[bool] force_new_workflow:
            If True, this task will always create a new workflow when invoked.
            Even when called from within another workflow, it creates a subworkflow
            that maintains a reference to its parent workflow.

        :return: A Task instance or a callable that when called returns a Task instance.

        :example:
        ```python
        # Basic task with no triggers
        @app.task(max_retries=3)
        def simple_task(x: int, y: int) -> int:
            return x + y

        # Task with a single trigger using a cron schedule
        from pynenc.trigger import on_cron
        @app.task(triggers=on_cron("0 0 * * *"))  # Run daily at midnight
        def daily_report() -> None:
            # Generate daily report
            pass

        # Task with multiple triggers using different conditions
        from pynenc.trigger import on_event, on_status
        @app.task(
            triggers=[
                on_event("payment.completed", filters={"amount": {"$gt": 1000}}),
                on_status("validate_data", statuses=["SUCCESS"])
            ]
        )
        def process_important_payment(payment_id: str) -> None:
            # Process high-value payment after validation
            pass

        # Task with complex trigger condition using a builder
        from pynenc.trigger import TriggerBuilder
        from pynenc.trigger.conditions import CompositeLogic

        trigger = (
            TriggerBuilder()
            .on_event("payment.received")
            .on_status("validate_payment")
            .with_logic(CompositeLogic.AND)  # Both conditions must be met
            .with_arguments(lambda ctx: {"payment_id": ctx["event"].payload["id"]})
        )

        @app.task(triggers=trigger)
        def process_payment(payment_id: str) -> None:
            # Process payment that has been received and validated
            pass
        ```
        """
        options = {
            "parallel_batch_size": parallel_batch_size,
            "retry_for": retry_for,
            "max_retries": max_retries,
            "running_concurrency": running_concurrency,
            "registration_concurrency": registration_concurrency,
            "key_arguments": key_arguments,
            "on_diff_non_key_args_raise": on_diff_non_key_args_raise,
            "call_result_cache": call_result_cache,
            "disable_cache_args": disable_cache_args,
            "force_new_workflow": force_new_workflow,
        }
        options = {k: v for k, v in options.items() if v is not None}

        def init_task(_func: "Func") -> Task["Params", "Result"]:
            if _func.__qualname__ != _func.__name__:
                raise ValueError(
                    "Decorated function must be defined at the module level."
                )
            task: Task = Task(self, _func, options)
            self._tasks[task.task_id] = task
            self.trigger.register_task_triggers(task, triggers)
            return task

        if func is None:
            return init_task
        return init_task(func)

    @overload
    def direct_task(
        self,
        func: "Func",
        *,
        parallel_func: Optional["ParallelFunc"] = None,
        aggregate_func: Optional["AggregateFunc"] = None,
        parallel_batch_size: Optional[int] = None,
        retry_for: Optional[tuple[type[Exception], ...]] = None,
        max_retries: Optional[int] = None,
        running_concurrency: Optional[ConcurrencyControlType] = None,
        registration_concurrency: Optional[ConcurrencyControlType] = None,
        key_arguments: Optional[tuple[str, ...]] = None,
        on_diff_non_key_args_raise: Optional[bool] = None,
        call_result_cache: Optional[bool] = None,
        disable_cache_args: Optional[tuple[str, ...]] = None,
        force_new_workflow: Optional[bool] = None,
    ) -> "Func":
        ...

    @overload
    def direct_task(
        self,
        func: "Func[Params, Result]",
        *,
        parallel_func: Optional["ParallelFunc"] = None,
        aggregate_func: Optional["AggregateFunc"] = None,
        parallel_batch_size: Optional[int] = None,
        retry_for: Optional[tuple[type[Exception], ...]] = None,
        max_retries: Optional[int] = None,
        running_concurrency: Optional[ConcurrencyControlType] = None,
        registration_concurrency: Optional[ConcurrencyControlType] = None,
        key_arguments: Optional[tuple[str, ...]] = None,
        on_diff_non_key_args_raise: Optional[bool] = None,
        call_result_cache: Optional[bool] = None,
        disable_cache_args: Optional[tuple[str, ...]] = None,
        force_new_workflow: Optional[bool] = None,
    ) -> "Func":
        ...

    @overload
    def direct_task(
        self,
        func: None = None,
        *,
        parallel_func: Optional["ParallelFunc"] = None,
        aggregate_func: Optional["AggregateFunc"] = None,
        parallel_batch_size: Optional[int] = None,
        retry_for: Optional[tuple[type[Exception], ...]] = None,
        max_retries: Optional[int] = None,
        running_concurrency: Optional[ConcurrencyControlType] = None,
        registration_concurrency: Optional[ConcurrencyControlType] = None,
        key_arguments: Optional[tuple[str, ...]] = None,
        on_diff_non_key_args_raise: Optional[bool] = None,
        call_result_cache: Optional[bool] = None,
        disable_cache_args: Optional[tuple[str, ...]] = None,
        force_new_workflow: Optional[bool] = None,
    ) -> Callable[["Func[Params, Result]"], "Func[Params, Result]"]:
        ...

    def direct_task(
        self,
        func: Optional["Func[Params, Result]"] = None,
        *,
        parallel_func: Optional["ParallelFunc"] = None,
        aggregate_func: Optional["AggregateFunc"] = None,
        parallel_batch_size: Optional[int] = None,
        retry_for: Optional[tuple[type[Exception], ...]] = None,
        max_retries: Optional[int] = None,
        running_concurrency: Optional[ConcurrencyControlType] = None,
        registration_concurrency: Optional[ConcurrencyControlType] = None,
        key_arguments: Optional[tuple[str, ...]] = None,
        on_diff_non_key_args_raise: Optional[bool] = None,
        call_result_cache: Optional[bool] = None,
        disable_cache_args: Optional[tuple[str, ...]] = None,
        force_new_workflow: Optional[bool] = None,
    ) -> (
        "Func[Params, Result]"
        | Callable[["Func[Params, Result]"], "Func[Params, Result]"]
    ):
        """
        Create a task that directly returns its result rather than returning an invocation.

        This decorator maintains the original function's behavior:
        - For synchronous functions, it waits for the result and returns it directly
        - For async functions, it returns an awaitable that resolves to the result

        It also supports parallel execution via the parallel_func parameter, which takes a function
        that generates arguments for parallel processing, and aggregate_func, which combines the results.

        :param Optional[Func] func:
            The function to be converted into a Task instance that returns results directly.
        :param Optional[ParallelFunc] parallel_func:
            Function that takes a dict of key arguments and returns either:

            1. An iterable of parameters for parallel execution (can be tuples, dicts, or Arguments)
               ```python
               # Example returning just parameters
               lambda args: [(i, i+1) for i in range(5)]  # Returns tuples
               lambda args: [{"x": i, "y": i+1} for i in range(5)]  # Returns dicts
               ```

            2. A tuple containing (common_args, param_iter) for efficient handling of large shared data:
               - common_args: Dictionary of arguments shared by all parallel tasks
               - param_iter: Iterable of dictionaries with task-specific arguments

               ```python
               # Example with common arguments
               lambda args: {
                   "common_args": {"large_data": args["large_data"]},  # Shared data (serialized once)
                   "param_iter": [{"index": i} for i in range(10)]  # Task-specific args
               }
               ```

               This second approach provides major performance benefits when dealing with large shared
               arguments (20MB+) as they're serialized only once instead of for each parallel task.
        :param Optional[AggregateFunc] aggregate_func:
            Function that takes a list of results and aggregates them into a single result.
        :param Optional[int] parallel_batch_size:
            If set to 0, auto parallelization is disabled. If greater than 0, tasks with iterable
            arguments are automatically split into chunks.
        :param Optional[Tuple[Exception, ...]] retry_for:
            Exceptions for which the task should be retried.
        :param Optional[int] max_retries:
            The maximum number of retries for a task.
        :param Optional[ConcurrencyControlType] running_concurrency:
            Controls the concurrency behavior of the task.
        :param Optional[ConcurrencyControlType] registration_concurrency:
            Manages task registration concurrency.
        :param Optional[Tuple[str, ...]] key_arguments:
            Key arguments for concurrency control.
        :param Optional[bool] on_diff_non_key_args_raise:
            If True, raises an exception for task invocations with matching key arguments but
            different non-key arguments.
        :param Optional[bool] call_result_cache:
            If True, it will return the latest result of a Task with the same arguments if available,
            otherwise it will trigger a new invocation as expected.
        :param Optional[tuple[str, ...]] disable_cache_args:
            Arguments to exclude from caching, it will accept "*" to disable caching for all arguments.
        :param Optional[bool] force_new_workflow:
            If True, this task will always create a new workflow when invoked.
            Even when called from within another workflow, it creates a subworkflow
            that maintains a reference to its parent workflow.

        :return: A function that behaves like the original but is backed by a distributed task system.

        :note:
        A direct task do not have triggers, it is always executed when called.

        :example:
        ```python
        @app.direct_task(max_retries=3)
        def my_func(x, y):
            return x + y

        # This will return the result directly
        result = my_func(1, 2)  # Returns 3

        # With parallel execution
        @app.direct_task(
            parallel_func=lambda _: [(i, i+1) for i in range(5)],
            aggregate_func=sum
        )
        def add_parallel(x, y):
            return x + y

        result = add_parallel(0, 0)  # Returns sum of all parallel results

        # With optimized pre-serialization of large shared data
        @app.direct_task(
            parallel_func=lambda args: {
                "common_args": {"large_data": args["large_data"]},
                "param_iter": [{"index": i} for i in range(100)]
            },
            aggregate_func=lambda results: sum(r[0] for r in results)
        )
        def process_data(large_data: str, index: int = 0) -> tuple[int, int]:
            # Process large data with multiple parallel tasks
            return (len(large_data) + index, index)

        # Calling with 20MB of data
        huge_data = "x" * (20 * 1024 * 1024)
        result = process_data(huge_data)  # Pre-serializes huge_data only once
        ```
        """

        def _parallelize(
            task: Task, *args: "Params.args", **kwargs: "Params.kwargs"
        ) -> "BaseInvocationGroup":
            parsed_args = task.args(*args, **kwargs).kwargs
            parallel_result = parallel_func(parsed_args)  # type: ignore
            if isinstance(parallel_result, tuple) and len(parallel_result) == 2:
                common_args, param_iter = parallel_result
                assert isinstance(param_iter, Iterable)
                assert isinstance(common_args, dict)
                return task.parallelize(param_iter, common_args)
            return task.parallelize(parallel_result)

        def _aggregate_results(results: Iterable["Result"]) -> "Result":
            if aggregate_func is not None:
                return aggregate_func(results)
            # TODO try to infer aggregate function from the type
            # eg. list.concat, dict.update, etc...
            raise ValueError("Aggregation function required for parallel execution")

        def decorator(func: "Func[Params, Result]") -> "Func[Params, Result]":
            task_options = {
                "parallel_batch_size": parallel_batch_size,
                "retry_for": retry_for,
                "max_retries": max_retries,
                "running_concurrency": running_concurrency,
                "registration_concurrency": registration_concurrency,
                "key_arguments": key_arguments,
                "on_diff_non_key_args_raise": on_diff_non_key_args_raise,
                "call_result_cache": call_result_cache,
                "disable_cache_args": disable_cache_args,
                "force_new_workflow": force_new_workflow,
            }
            task_options = {k: v for k, v in task_options.items() if v is not None}
            task = self.task(func, **task_options)  # type: ignore
            is_async = asyncio.iscoroutinefunction(func)

            if is_async:

                @wraps(func)
                async def async_wrapper(
                    *args: "Params.args", **kwargs: "Params.kwargs"
                ) -> "Result":
                    if parallel_func:
                        invocation_group = _parallelize(task, *args, **kwargs)
                        results = [
                            result async for result in invocation_group.async_results()
                        ]
                        return _aggregate_results(results)
                    return await task(*args, **kwargs).async_result()

                # Attach the original function as an attribute
                async_wrapper.__inner_function__ = func  # type: ignore
                return async_wrapper  # type: ignore

            @wraps(func)
            def sync_wrapper(
                *args: "Params.args", **kwargs: "Params.kwargs"
            ) -> "Result":
                if parallel_func:
                    invocation_group = _parallelize(task, *args, **kwargs)
                    return _aggregate_results(invocation_group.results)
                return task(*args, **kwargs).result

            # Attach the original function as an attribute
            sync_wrapper.__inner_function__ = func  # type: ignore
            return sync_wrapper

        if func is None:
            return decorator
        return decorator(func)
