from functools import cached_property
from logging import Logger
from typing import TYPE_CHECKING, Any, Callable, Optional, Type, overload

from .broker import BaseBroker, MemBroker
from .conf.config_pynenc import ConfigPynenc
from .exceptions import AlreadyInitializedError
from .orchestrator import BaseOrchestrator, MemOrchestrator
from .runner import BaseRunner, DummyRunner
from .serializer import BaseSerializer, JsonSerializer
from .state_backend import BaseStateBackend, MemStateBackend
from .task import Task
from .util.log import create_logger
from .util.subclasses import get_subclass

if TYPE_CHECKING:
    from .types import Func, Params, Result


class Pynenc:
    """
    The main class of the Pynenc library that creates an application object.

    Parameters
    ----------
    task_broker : BaseTaskBroker
        Handles routing of tasks for distributed execution.
    state_backend : BaseStateBackend
        Maintains the state of tasks, runners, and other relevant system states.
    orchestrator : BaseOrchestrator
        Coordinates all components and acts according to the configuration.
    reporting : list of BaseReporting
        Reports to one or more systems.

    Notes
    -----
    All of these base classes are abstract and cannot be used directly. If none is specified,
    they will default to `MemTaskBroker`, `MemStateBackend`, etc. These default classes do not
    actually distribute the code but are helpers for tests or for running an application on your
    localhost. They may help to parallelize to some degree but cannot be used in a production system.

    Examples
    --------
    Default Pynenc application for running in memory in a local environment.

    >>> app = Pynenc()
    """

    _orchestrator_cls: Type[BaseOrchestrator] = MemOrchestrator
    _broker_cls: Type[BaseBroker] = MemBroker
    _state_backend_cls: Type[BaseStateBackend] = MemStateBackend
    _serializer_cls: Type[BaseSerializer] = JsonSerializer
    _runner_cls: Type[BaseRunner] = DummyRunner

    def __init__(
        self,
        app_id: str = "pynenc",
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
        return self._app_id

    def __getstate__(self) -> dict:
        # Return state as a dictionary and a secondary value as a tuple
        return {
            "app_id": self.app_id,
            "orchestrator_cls": self._orchestrator_cls.__name__,
            "broker_cls": self._broker_cls.__name__,
            "state_backend_cls": self._state_backend_cls.__name__,
            "serializer_cls": self._serializer_cls.__name__,
            "runner_cls": self._runner_cls.__name__,
            "config_values": self.config_values,
            "config_filepath": self.config_filepath,
            "reporting": self.reporting,
        }

    def __setstate__(self, state: dict) -> None:
        # Restore instance attributes
        self._app_id = state["app_id"]
        object.__setattr__(self, "_app_id", self._app_id)
        self._orchestrator_cls = get_subclass(
            BaseOrchestrator,  # type: ignore # mypy issue #4717
            state["orchestrator_cls"],
        )
        self._broker_cls = get_subclass(
            BaseBroker,  # type: ignore # mypy issue #4717
            state["broker_cls"],
        )
        self._state_backend_cls = get_subclass(
            BaseStateBackend,  # type: ignore # mypy issue #4717
            state["state_backend_cls"],
        )
        self._serializer_cls = get_subclass(
            BaseSerializer,  # type: ignore # mypy issue #4717
            state["serializer_cls"],
        )
        self._runner_cls = get_subclass(
            BaseRunner,  # type: ignore # mypy issue #4717
            state["runner_cls"],
        )
        self.config_values = state["config_values"]
        self.config_filepath = state["config_filepath"]
        self.reporting = state["reporting"]
        self._runner_instance = None

    def is_initialized(self, property_name: str) -> bool:
        """Returns True if the given cached_property has been initialized"""
        return property_name in self.__dict__

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
        return self._orchestrator_cls(self)

    @cached_property
    def broker(self) -> BaseBroker:
        return self._broker_cls(self)

    @cached_property
    def state_backend(self) -> BaseStateBackend:
        return self._state_backend_cls(self)

    @cached_property
    def serializer(self) -> BaseSerializer:
        return self._serializer_cls()

    @property
    def runner(self) -> BaseRunner:
        if self._runner_instance is None:
            self._runner_instance = self._runner_cls(self)
        return self._runner_instance

    @runner.setter
    def runner(self, runner_instance: BaseRunner) -> None:
        self._runner_instance = runner_instance

    def set_orchestrator_cls(self, orchestrator_cls: Type[BaseOrchestrator]) -> None:
        if self.is_initialized(property_name="orchestrator"):
            raise AlreadyInitializedError(
                f"Not possible to set orchestrator instance, already initialized {self._orchestrator_cls}"
            )
        self._orchestrator_cls = orchestrator_cls

    def set_broker_cls(self, broker_cls: Type[BaseBroker]) -> None:
        if self.is_initialized(property_name="broker"):
            raise AlreadyInitializedError(
                f"Not possible to set broker, already initialized {self._broker_cls}"
            )
        self._broker_cls = broker_cls

    def set_state_backend_cls(self, state_backend_cls: Type[BaseStateBackend]) -> None:
        if self.is_initialized(property_name="state_backend"):
            raise AlreadyInitializedError(
                f"Not possible to set state backend, already initialized {self._state_backend_cls}"
            )
        self._state_backend_cls = state_backend_cls

    def set_serializer_cls(self, serializer_cls: Type[BaseSerializer]) -> None:
        if self.is_initialized(property_name="serializer"):
            raise AlreadyInitializedError(
                f"Not possible to set serializer, already initialized {self._serializer_cls}"
            )
        self._serializer_cls = serializer_cls

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

        Check the options reference in self.task_cls.options_cls or the Pynenc documentation for a detailed explanation
        of the BaseTask instance you are applying.

        Parameters
        ----------
        func : Callable, optional
            The function to be converted into a BaseTask instance.
        ``**options`` : dict
            The options to be passed to the BaseTask instance.

        Returns
        -------
        Task | Callable[..., Task]
            The BaseTask instance or a callable that returns a BaseTask instance.

        Examples
        --------
        >>> @app.task(option1='value1', option2='value2')
        ... def my_func(x, y):
        ...     return x + y
        ...
        >>> result = my_func(1, 2)
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
