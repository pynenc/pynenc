from .config_base import ConfigBase, ConfigField
from .config_redis import ConfigRedis


class ConfigOrchestrator(ConfigBase):
    """
    Main config of the orchestrator components.

    Attributes
    ----------
    cycle_control : ConfigField[bool]
        This boolean flag enables the orchestrator to detect cycles of calls to subtasks.
        For example, if task1 calls task2 and task2 calls back to task1, this can create
        an endless loop. The cycle control functionality is enabled by default to prevent
        such scenarios. Users can choose to disable it if needed.

    blocking_control : ConfigField[bool]
        This boolean flag activates control over tasks that are blocking on other tasks.
        If a task invocation is waiting on another invocation, it notifies the runner,
        which temporarily removes it from the processing queue and uses the slot for
        another task invocation. Once the required invocation finishes, the dependent
        invocation is placed back into the run queue. This feature also prioritizes
        invocations that have many dependencies over new ones, ensuring efficient
        task management.

    auto_final_invocation_purge_hours : ConfigField[float]
        This float value, defaulting to 24.0 hours, sets the duration after which the
        orchestrator purges all invocations older than the specified time. This purge
        mechanism helps keep the orchestrator lightweight and fast, as it should
        ideally operate with minimal latency. Detailed information about the invocations
        is stored in the result backend, which can handle more data and afford to be slower.
    """

    cycle_control = ConfigField(True)
    blocking_control = ConfigField(True)
    auto_final_invocation_purge_hours = ConfigField(24.0)


class ConfigOrchestratorRedis(ConfigOrchestrator, ConfigRedis):
    """Specific Configuration for the Redis Orchestrator"""
