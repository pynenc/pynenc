from cistell import ConfigField

from pynenc.conf.config_base import ConfigPynencBase
from pynenc.conf.config_sqlite import ConfigSQLite


class ConfigOrchestrator(ConfigPynencBase):
    """
    Main config of the orchestrator components.

    :cvar ConfigField[bool] blocking_control:
        This boolean flag activates control over tasks that are blocking on other tasks.
        If a task invocation is waiting on another invocation, it notifies the runner,
        which temporarily removes it from the processing queue and uses the slot for
        another task invocation. Once the required invocation finishes, the dependent
        invocation is placed back into the run queue. This feature also prioritizes
        invocations that have many dependencies over new ones, ensuring efficient
        task management.

    :cvar  ConfigField[float] auto_final_invocation_purge_hours:
        This float value, defaulting to 24.0 hours, sets the duration after which the
        orchestrator purges all invocations older than the specified time. This purge
        mechanism helps keep the orchestrator lightweight and fast, as it should
        ideally operate with minimal latency. Detailed information about the invocations
        is stored in the result backend, which can handle more data and afford to be slower.
    """

    blocking_control = ConfigField(True)
    auto_final_invocation_purge_hours = ConfigField(24.0)


class ConfigOrchestratorSQLite(ConfigOrchestrator, ConfigSQLite):
    """SQLite-based orchestrator configuration"""
