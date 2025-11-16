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

    :cvar ConfigField[float] run_invocation_recovery_service_every_minutes:
        This float value, defaulting to 5.0 minutes, sets the interval at which the
        invocation recovery service runs. The recovery service checks for invocations
        stuck in PENDING status beyond the allowed time and recovers them.

        ```{warning}
        This value must be coordinated with `recovery_service_check_interval_minutes`
        in the runner configuration. The runner's check interval should be significantly
        shorter than this recovery interval to ensure timely recovery execution.

        Additionally, ensure `runner_heartbeat_timeout_minutes` is longer than the
        runner's check interval to avoid marking active runners as inactive.

        Recommended ratios:
        - `recovery_service_check_interval_minutes` ≤ `run_invocation_recovery_service_every_minutes` / 10
        - `runner_heartbeat_timeout_minutes` ≥ 2 × `recovery_service_check_interval_minutes`

        Example safe configuration:
        - `recovery_service_check_interval_minutes = 0.5` (30 seconds)
        - `run_invocation_recovery_service_every_minutes = 5.0` (5 minutes)
        - `runner_heartbeat_timeout_minutes = 10.0` (10 minutes)
        ```

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
    run_invocation_recovery_service_every_minutes = ConfigField(5.0)
    recovery_service_spread_margin_minutes = ConfigField(1.0)
    runner_heartbeat_timeout_minutes = ConfigField(10.0)


class ConfigOrchestratorSQLite(ConfigOrchestrator, ConfigSQLite):
    """SQLite-based orchestrator configuration"""
