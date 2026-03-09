from typing import Any, TYPE_CHECKING, NamedTuple
from collections.abc import Callable

from pynenc import context
from pynenc.conf.config_task import ConcurrencyControlType
from pynenc.invocation.status import InvocationStatus

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.identifiers.invocation_id import InvocationId
    from pynenc.runner.runner_context import RunnerContext


class CoreTaskFunction:
    """Wrapper for core task functions that will be bound to an app at runtime.

    This wrapper allows the function to be identified as a core task during
    deserialization, enabling proper function extraction in Task._from_json.
    """

    def __init__(self, func: Callable) -> None:
        self.func = func
        # Preserve function metadata for proper module import resolution
        self.__name__ = func.__name__
        self.__module__ = func.__module__
        self.__doc__ = func.__doc__

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.func(*args, **kwargs)


class CoreTaskDefinition(NamedTuple):
    func: Callable
    options: dict[str, Any]
    config_cron: str | None = None


class CoreTaskRegistry:
    """Registry for core tasks definitions that should run in any Pynenc instance."""

    def __init__(self) -> None:
        self.definitions: list[CoreTaskDefinition] = []

    def task(
        self,
        config_cron: str | None = None,
        **options: Any,
    ) -> Callable[[Callable], CoreTaskFunction]:
        """Deferred decorator that stores the function and options"""
        if config_cron and options.get("triggers"):
            raise ValueError(
                "Cannot specify both 'config_cron' and 'triggers' in core task definition"
            )

        def decorator(func: Callable) -> CoreTaskFunction:
            wrapped = CoreTaskFunction(func)
            self.definitions.append(CoreTaskDefinition(func, options, config_cron))
            return wrapped

        return decorator


def get_app_and_runner_ctx() -> tuple["Pynenc", "RunnerContext"]:
    """Get the app and runner context from the current execution context"""
    app = context.get_current_app()
    if not app:
        raise RuntimeError("No app context available")
    runner_ctx = context.get_runner_context(app.app_id)
    if not runner_ctx:
        raise RuntimeError("No runner context available")
    return app, runner_ctx


core_tasks_registry = CoreTaskRegistry()


@core_tasks_registry.task(
    running_concurrency=ConcurrencyControlType.TASK,
    config_cron="recover_pending_invocations_cron",
)
def recover_pending_invocations() -> None:
    """Recovers PENDING invocations that exceeded the allowed pending time"""
    # It will run as a Pynenc tasks
    app, runner_ctx = get_app_and_runner_ctx()
    invocations_to_reroute: set[InvocationId] = set()
    # Recover PENDING invocations that exceeded timeout
    for invocation_id in app.orchestrator.get_pending_invocations_for_recovery():
        invocations_to_reroute.add(invocation_id)
        app.logger.info(f"Recovering timed-out pending invocation:{invocation_id}")
        app.orchestrator.set_invocation_status(
            invocation_id, InvocationStatus.PENDING_RECOVERY, runner_ctx
        )
    app.orchestrator.reroute_invocations(invocations_to_reroute, runner_ctx)


@core_tasks_registry.task(
    running_concurrency=ConcurrencyControlType.TASK,
    config_cron="recover_running_invocations_cron",
)
def recover_running_invocations() -> None:
    """Recovers PENDING invocations that exceeded the allowed pending time"""
    app, runner_ctx = get_app_and_runner_ctx()
    invocations_to_reroute: set[InvocationId] = set()
    # Recover RUNNING invocations owned by inactive runners
    for invocation_id in app.orchestrator.get_running_invocations_for_recovery():
        invocations_to_reroute.add(invocation_id)
        app.logger.info(
            f"Recovering running invocation:{invocation_id} from inactive runner"
        )
        app.orchestrator.set_invocation_status(
            invocation_id, InvocationStatus.RUNNING_RECOVERY, runner_ctx
        )
    app.orchestrator.reroute_invocations(invocations_to_reroute, runner_ctx)
