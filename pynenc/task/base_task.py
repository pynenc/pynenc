from __future__ import annotations
from dataclasses import dataclass
import inspect
from typing import (
    Callable,
    Type,
    TypeVar,
    ParamSpec,
    ClassVar,
    TYPE_CHECKING,
    Generic,
    Any,
    Optional,
)

from ..invocation import BaseInvocation, SynchronousInvocation

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..conf.single_invocation_pending import SingleInvocationPending

    P = ParamSpec("P")
else:
    P = TypeVar("P")
R = TypeVar("R")


@dataclass
class BaseOptions:
    """The options common to any implementation of BaseTask"""

    #: If True, only one request will be routed by the broker.
    #: Use this option for tasks that make no sense to execute multiple times in parallel or to avoid generating too much unnecessary tasks in the system.
    single_pending: Optional[SingleInvocationPending] = None

    #: Profiling will take care of storing profiling information for the task (this is a todo, will require further options).
    profiling: Optional[str] = None


class BaseTask(Generic[P, R]):
    """
    A task in the Pynenc library that represents a function that can be distributed.

    Parameters
    ----------
    app : Pynenc
        A reference to the Pynenc application.
    func : Callable
        The function to be run distributed.
    options : dict
        The options to apply.

    The `BaseTask` can be called normally and will return an instance of `BaseResult`.
    The result will be an `AsyncResult` when running normally but can be `SyncResult`
    when running eagerly in development with the `pynenc` app's `dev_mode_force_sync_tasks`
    option set to `True` (or the 'DEV_MODE_FORCE_SYNC_TASK' environment variable set).
    The option `dev_mode_force_sync_tasks` should only be used in development.

    Although it is possible to create a `BaseTask` instance directly, it is recommended to
    use the decorator provided in the `pynenc` application, i.e., `@app.task(options...)`.
    This is the expected way of instantiating a class and registering it in the app.

    Examples
    --------
    >>> @app.task(options)
    ... def func():
    ...     pass
    ...
    >>> result = func()
    """

    options_cls: ClassVar[Type[BaseOptions]] = BaseOptions

    def __init__(
        self, app: Pynenc, func: Callable[P, R], options: dict[str, Any]
    ) -> None:
        self.task_id = f"{func.__module__}.{func.__name__}"
        self.app = app
        self.func = func
        self.options: BaseOptions = self.options_cls(**options)

    def extract_arguments(self, *args: P.args, **kwargs: P.kwargs) -> dict[str, Any]:
        """Extracts the arguments of the task.func function call and returns them as a dictionary."""
        sig = inspect.signature(self.func)
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()
        return bound_args.arguments

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> BaseInvocation[R]:
        """"""
        arguments = self.extract_arguments(*args, **kwargs)
        if self.app.conf.dev_mode_force_sync_tasks:
            return SynchronousInvocation(
                value=self.func(*args, **kwargs), arguments=arguments
            )
        return self.app.orchestrator.route_task(self, arguments)
