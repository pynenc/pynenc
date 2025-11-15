from cistell import ConfigField

from pynenc.conf.config_base import ConfigPynencBase
from pynenc.conf.config_sqlite import ConfigSQLite


class ConfigOrchestrator(ConfigPynencBase):
    """
    Main config of the orchestrator components.

    :cvar ConfigField[bool] cycle_control:
        This boolean flag enables the orchestrator to detect cycles of calls to subtasks.
        For example, if task1 calls task2 and task2 calls back to task1, this can create
        an endless loop. The cycle control functionality is enabled by default to prevent
        such scenarios. Users can choose to disable it if needed.

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

    :cvar ConfigField[float] non_atomic_status_retry_sleep_seconds:
        This float value, defaulting to 0.1 seconds, defines the sleep duration between
        retries when updating invocation statuses in non-atomic mode. In non-atomic mode,
        there is a possibility of race conditions when multiple runners attempt to update
        the status of the same invocation simultaneously. This sleep interval helps
        mitigate such race conditions by introducing a brief pause between retries.

    :cvar ConfigField[float] run_invocation_recovery_service_every_minutes:
        This float value, defaulting to 5.0 minutes, sets the interval at which the
        invocation recovery service runs. The recovery service checks for invocations
        stuck in PENDING status beyond the allowed time and recovers them.

    :cvar ConfigField[float] recovery_service_spread_margin_minutes:
        This float value, defaulting to 1.0 minutes, defines the time margin used to
        spread recovery service execution across multiple runners. This prevents all
        runners from executing recovery simultaneously and helps avoid race conditions.

    :cvar ConfigField[float] runner_heartbeat_timeout_minutes:
        This float value, defaulting to 10.0 minutes, determines how long a runner
        can be inactive before being considered dead and cleaned up from the active
        runners list.
    """

    cycle_control = ConfigField(True)
    blocking_control = ConfigField(True)
    auto_final_invocation_purge_hours = ConfigField(24.0)
    non_atomic_status_retry_sleep_seconds = ConfigField(0.1)
    run_invocation_recovery_service_every_minutes = ConfigField(5.0)
    recovery_service_spread_margin_minutes = ConfigField(1.0)
    runner_heartbeat_timeout_minutes = ConfigField(10.0)


class ConfigOrchestratorSQLite(ConfigOrchestrator, ConfigSQLite):
    """SQLite-based orchestrator configuration"""
