from enum import StrEnum, auto

from cistell import ConfigField

from pynenc.conf.config_base import ConfigPynencBase


class ArgumentPrintMode(StrEnum):
    """
    Controls how task arguments are displayed in logs.

    :cvar FULL:
        Show complete argument values.
    :cvar KEYS:
        Show only argument names.
    :cvar TRUNCATED:
        Show truncated argument values based on configured length.
    :cvar HIDDEN:
        Hide all argument values.
    """

    FULL = auto()
    KEYS = auto()
    TRUNCATED = auto()
    HIDDEN = auto()


class ConfigPynenc(ConfigPynencBase):
    """
    Main config of the pynenc app.

    :cvar str app_id:
        The id of the application.
    :cvar str orchestrator_cls:
        The orchestrator class to use.
    :cvar str trigger_cls:
        The trigger class to use. Default is "DisabledTrigger" which provides no trigger
        functionality. Enable triggers by setting to "MemTrigger", "SqliteTrigger", or a
        distributed trigger backend from a plugin (e.g., "RedisTrigger" from pynenc-redis).
    :cvar str broker_cls:
        The broker class to use.
    :cvar str state_backend_cls:
        The state backend class to use.
    :cvar str serializer_cls:
        The serializer class to use.
    :cvar str arg_cache_cls:
        The argument cache class to use.
    :cvar str runner_cls:
        The runner class to use.
    :cvar bool dev_mode_force_sync_tasks:
        If True, forces tasks to run synchronously, useful for development.
    :cvar float max_pending_seconds:
        Maximum time in seconds a task can remain in PENDING state before it expires.
        See :class:`~pynenc.invocation.status.InvocationStatus` for more details.
    :cvar str logging_level:
        The logging level of the application ('info', 'warning', 'error', etc.).
    :cvar bool print_arguments:
        If True, prints task arguments in logs. Default False.
    :cvar int truncate_arguments_length:
        Maximum length for printed arguments. If 0, no truncation. Default 32.
    :cvar ArgumentPrintMode argument_print_mode:
        How to print arguments: FULL (all args), KEYS (only names),
        TRUNCATED (truncated values), HIDDEN (no args). Default TRUNCATED.
    :cvar float cached_status_time:
        Time in seconds to cache invocation status for non-final states. This helps
        reduce the amount of queries by avoiding repeated status checks within this time window.
        Final statuses are cached indefinitely since they never change. Default 0.1.
    :cvar bool truncate_log_ids:
        If True, truncates long IDs (invocation_id, runner_id, etc.) in log output
        to improve readability. If False, displays full IDs. Default True.
    :cvar float atomic_service_interval_minutes:
        The total cycle interval for atomic global services (triggers, recovery, etc.).
        The interval is divided equally among all active runners, with each runner assigned
        a specific time slot within the cycle. Only one runner executes these services
        at a time across the entire system. Default 5.0 minutes.
    :cvar float atomic_service_spread_margin_minutes:
        Safety margin subtracted from each runner's time slot to prevent overlapping
        execution of atomic services across distributed runners. Default 1.0 minute.
    :cvar float atomic_service_check_interval_minutes:
        How frequently an individual runner checks if it should execute atomic global services.
        This is the polling interval - each runner checks every N minutes to see if it's within
        its assigned time slot. Should be significantly less than atomic_service_interval_minutes
        to ensure runners don't miss their execution window. Default 0.5 minutes (30 seconds).
    :cvar float atomic_service_runner_considered_dead_after_minutes:
        Timeout period for considering a runner inactive/dead based on heartbeat silence.

        This value determines the heartbeat timeout that is used for two purposes:
        1. **Invocation Recovery**: Any invocations in RUNNING status assigned to runners
           that haven't sent a heartbeat within this period will be recovered by other runners.
        2. **Atomic Service Scheduling**: Only runners that have sent a heartbeat within
           this period are considered "active" and eligible to participate in atomic service
           time slot scheduling.

        In both cases, the logic is the same: a runner is considered inactive if it hasn't
        sent a heartbeat within this timeout period.
    """

    app_id = ConfigField("pynenc")
    orchestrator_cls = ConfigField("MemOrchestrator")
    trigger_cls = ConfigField("DisabledTrigger")
    broker_cls = ConfigField("MemBroker")
    state_backend_cls = ConfigField("MemStateBackend")
    serializer_cls = ConfigField("JsonPickleSerializer")
    arg_cache_cls = ConfigField("DisabledArgCache")
    runner_cls = ConfigField("DummyRunner")
    dev_mode_force_sync_tasks = ConfigField(False)
    max_pending_seconds = ConfigField(5.0)
    logging_level = ConfigField("info")
    print_arguments = ConfigField(True)
    truncate_arguments_length = ConfigField(32)
    argument_print_mode = ConfigField(ArgumentPrintMode.TRUNCATED)
    cached_status_time = ConfigField(0.1)
    truncate_log_ids = ConfigField(True)
    atomic_service_interval_minutes = ConfigField(5.0)
    atomic_service_spread_margin_minutes = ConfigField(1.0)
    atomic_service_check_interval_minutes = ConfigField(0.5)
    atomic_service_runner_considered_dead_after_minutes = ConfigField(10.0)
