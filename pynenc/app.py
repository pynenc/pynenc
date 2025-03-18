from functools import cached_property
from logging import Logger
from typing import TYPE_CHECKING, Any, Callable, Optional, overload

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
from pynenc.util.log import create_logger
from pynenc.util.subclasses import get_subclass

if TYPE_CHECKING:
    from pynenc.types import Func, Params, Result


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
        self.logger.info(f"Initialized Pynenc app with id {self.app_id}")

    @property
    def app_id(self) -> str:
        return self._app_id or self.conf.app_id

    def __getstate__(self) -> dict:
        # Return state as a dictionary and a secondary value as a tuple
        return {
            "app_id": self.app_id,
            "config_values": self.config_values,
            "config_filepath": self.config_filepath,
            "reporting": self.reporting,
        }

    def __setstate__(self, state: dict) -> None:
        # Restore instance attributes
        self._app_id = state["app_id"]
        object.__setattr__(self, "_app_id", self._app_id)
        self.config_values = state["config_values"]
        self.config_filepath = state["config_filepath"]
        self.reporting = state["reporting"]
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

    @overload
    def task(
        self,
        func: "Func",
        *,
        auto_parallel_batch_size: Optional[int] = None,
        retry_for: Optional[tuple[type[Exception], ...]] = None,
        max_retries: Optional[int] = None,
        running_concurrency: Optional[ConcurrencyControlType] = None,
        registration_concurrency: Optional[ConcurrencyControlType] = None,
        key_arguments: Optional[tuple[str, ...]] = None,
        on_diff_non_key_args_raise: Optional[bool] = None,
        call_result_cache: Optional[bool] = None,
        disable_cache_args: Optional[tuple[str, ...]] = None,
    ) -> "Task":
        ...

    @overload
    def task(
        self,
        func: None = None,
        *,
        auto_parallel_batch_size: Optional[int] = None,
        retry_for: Optional[tuple[type[Exception], ...]] = None,
        max_retries: Optional[int] = None,
        running_concurrency: Optional[ConcurrencyControlType] = None,
        registration_concurrency: Optional[ConcurrencyControlType] = None,
        key_arguments: Optional[tuple[str, ...]] = None,
        on_diff_non_key_args_raise: Optional[bool] = None,
        call_result_cache: Optional[bool] = None,
        disable_cache_args: Optional[tuple[str, ...]] = None,
    ) -> Callable[["Func"], "Task"]:
        ...

    def task(
        self,
        func: Optional["Func"] = None,
        *,
        auto_parallel_batch_size: Optional[int] = None,
        retry_for: Optional[tuple[type[Exception], ...]] = None,
        max_retries: Optional[int] = None,
        running_concurrency: Optional[ConcurrencyControlType] = None,
        registration_concurrency: Optional[ConcurrencyControlType] = None,
        key_arguments: Optional[tuple[str, ...]] = None,
        on_diff_non_key_args_raise: Optional[bool] = None,
        call_result_cache: Optional[bool] = None,
        disable_cache_args: Optional[tuple[str, ...]] = None,
    ) -> "Task" | Callable[["Func"], "Task"]:
        """
        The task decorator converts the function into an instance of a BaseTask. It accepts any kind of options,
        however these options will be validated with the options class assigned to the class.

        :param Optional[Callable] func:
            The function to be converted into a Task instance.
        :param Optional[int] auto_parallel_batch_size:
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

        :return: A Task instance or a callable that when called returns a Task instance.

        :example:
        ```python
        @app.task(auto_parallel_batch_size=10, max_retries=3)
        def my_func(x, y):
            return x + y
        ```
        """
        options = {
            "auto_parallel_batch_size": auto_parallel_batch_size,
            "retry_for": retry_for,
            "max_retries": max_retries,
            "running_concurrency": running_concurrency,
            "registration_concurrency": registration_concurrency,
            "key_arguments": key_arguments,
            "on_diff_non_key_args_raise": on_diff_non_key_args_raise,
            "call_result_cache": call_result_cache,
            "disable_cache_args": disable_cache_args,
        }
        options = {k: v for k, v in options.items() if v is not None}

        def init_task(_func: "Func") -> Task["Params", "Result"]:
            if _func.__qualname__ != _func.__name__:
                raise ValueError(
                    "Decorated function must be defined at the module level."
                )
            return Task(self, _func, options)

        if func is None:
            return init_task
        return init_task(func)
