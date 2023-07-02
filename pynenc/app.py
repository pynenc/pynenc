from functools import cached_property
from typing import TYPE_CHECKING, Callable, overload, Optional, Any, Type

from .task import Task
from .broker import BaseBroker, MemBroker
from .orchestrator import BaseOrchestrator, MemOrchestrator
from .state_backend import BaseStateBackend, MemStateBackend
from .serializer import BaseSerializer, JsonSerializer
from .runner import BaseRunner, DummyRunner
from .conf import Config

if TYPE_CHECKING:
    from .types import Func, Params, Result
    from .invocation import DistributedInvocation


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

    def __init__(self) -> None:
        self.conf = Config()
        self.reporting = None
        self._runner_instance: Optional[BaseRunner] = None
        self.running_invocation: Optional["DistributedInvocation"] = None

    def is_initialized(self, property_name: str) -> bool:
        """Returns True if the given cached_property has been initialized"""
        return property_name in self.__dict__

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
        if self.is_initialized(prop := "orchestrator"):
            raise Exception(
                f"Not possible to set orchestrator instance, already initialized {self._orchestrator_cls}"
            )
        self._orchestrator_cls = orchestrator_cls

    def set_broker_cls(self, broker_cls: Type[BaseBroker]) -> None:
        if self.is_initialized(prop := "broker"):
            raise Exception(
                f"Not possible to set broker, already initialized {self._broker_cls}"
            )
        self._broker_cls = broker_cls

    def set_state_backend_cls(self, state_backend_cls: Type[BaseStateBackend]) -> None:
        if self.is_initialized(prop := "state_backend"):
            raise Exception(
                f"Not possible to set state backend, already initialized {self._state_backend_cls}"
            )
        self._state_backend_cls = state_backend_cls

    def set_serializer(self, serializer_cls: Type[BaseSerializer]) -> None:
        if self.is_initialized(prop := "serializer"):
            raise Exception(
                f"Not possible to set serializer, already initialized {self._serializer_cls}"
            )
        self._serializer_cls = serializer_cls

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
        **options : dict
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
            return Task(self, _func, options)

        if func is None:
            return init_task
        return init_task(func)
