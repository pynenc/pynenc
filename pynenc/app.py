from typing import (
    TYPE_CHECKING,
    Callable,
    Type,
    TypeVar,
    overload,
    Optional,
    Any,
    Generic,
)

from .task import Task, BaseTask
from .broker import BaseBroker, MemBroker
from .orchestrator import BaseOrchestrator, MemOrchestrator
from .state_backend import BaseStateBackend, MemStateBackend
from .conf import Config

if TYPE_CHECKING:
    from typing_extensions import ParamSpec

    P = ParamSpec("P")
else:
    P = TypeVar("P")
R = TypeVar("R")
TaskG = BaseTask[P, R]
FuncG = Callable[P, R]
T = TypeVar("T", bound=TaskG)


class Pynenc(Generic[T]):
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

    def __init__(self, task_cls: Optional[Type[T]]) -> None:
        self.conf = Config()
        self.task_broker: BaseBroker = MemBroker(self)
        self.state_backend: BaseStateBackend = MemStateBackend(self)
        self.orchestrator: BaseOrchestrator = MemOrchestrator(self)
        self.reporting = None
        self.task_cls: Type[T] = task_cls if task_cls else Task

    @overload
    def task(self, func: FuncG, **options: Any) -> T:
        ...

    @overload
    def task(self, func: None = None, **options: Any) -> Callable[[FuncG], T]:
        ...

    def task(
        self, func: Optional[FuncG] = None, **options: Any
    ) -> T | Callable[[FuncG], T]:
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

        def init_task(_func: Callable[P, R]) -> T:
            return self.task_cls(self, _func, options)

        if func is None:
            return init_task
        return init_task(func)
