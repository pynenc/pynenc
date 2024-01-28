from functools import cached_property
from logging import Logger
from typing import TYPE_CHECKING, Any, Callable, Optional, overload

from pynenc.broker.base_broker import BaseBroker
from pynenc.conf.config_pynenc import ConfigPynenc
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

    @property
    def runner(self) -> BaseRunner:
        if self._runner_instance is None:
            self._runner_instance = get_subclass(BaseRunner, self.conf.runner_cls)(self)  # type: ignore # mypy issue #4717
        return self._runner_instance

    @runner.setter
    def runner(self, runner_instance: BaseRunner) -> None:
        self._runner_instance = runner_instance

    def purge(self) -> None:
        """Purge all data from the broker and state backend"""
        self.broker.purge()
        self.orchestrator.purge()
        self.state_backend.purge()

    @overload
    def task(self, func: "Func", **options: Any) -> "Task":
        ...

    @overload
    def task(self, func: None = None, **options: Any) -> Callable[["Func"], "Task"]:
        ...

    def task(
        self, func: Optional["Func"] = None, **options: Any
    ) -> "Task" | Callable[["Func"], "Task"]:
        """
        The task decorator converts the function into an instance of a BaseTask. It accepts any kind of options,
        however these options will be validated with the options class assigned to the class.

        Check the options reference in conf.config_task or the Pynenc documentation for a detailed explanation
        of the BaseTask instance you are applying.

        :param Optional["Func"] func:
            The function to be converted into a BaseTask instance.
        :param **options:
            Arbitrary keyword arguments representing options for the BaseTask instance.
            Each key-value pair in options corresponds to a specific configuration setting for the task.
            The available options and their meanings depend on the BaseTask implementation and configuration.

        :return: The BaseTask instance or a callable that returns a BaseTask instance.

        :example:
        ```python
            @app.task(option1='value1', option2='value2')
            def my_func(x, y):
                return x + y

            result = my_func(1, 2)
        ```
        """

        def init_task(_func: "Func") -> Task["Params", "Result"]:
            if _func.__qualname__ != _func.__name__:
                raise ValueError(
                    "Decorated function must be defined at the module level."
                )
            return Task(self, _func, options)

        if func is None:
            return init_task
        return init_task(func)
