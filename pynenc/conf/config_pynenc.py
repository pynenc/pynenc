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


class LogFormat(StrEnum):
    """
    Controls the log output format.

    :cvar TEXT:
        Human-readable text output (default). Includes optional ANSI colors
        for terminal environments and context prefixes with runner/invocation info.
    :cvar JSON:
        Structured JSON output for container and cloud environments.
        Each log record is emitted as a single JSON object per line with structured
        fields (severity, timestamp, logger, message, runner/invocation context).
        Compatible with Google Cloud Logging, AWS CloudWatch, Datadog, and other
        log aggregators. Includes a ``text`` field with the human-readable
        representation for pynmon log explorer compatibility.
    """

    TEXT = auto()
    JSON = auto()


class ConfigPynenc(ConfigPynencBase):
    """
    Main configuration for the Pynenc app.

    =============================
    Core Classes Configuration
    =============================
    :cvar str app_id:
        The id of the application.
    :cvar str orchestrator_cls:
        The orchestrator class to use.
    :cvar str trigger_cls:
        The trigger class to use.
    :cvar str broker_cls:
        The broker class to use.
    :cvar str state_backend_cls:
        The state backend class to use.
    :cvar str serializer_cls:
        The serializer class to use.
    :cvar str client_data_store_cls:
        The client data store class to use.
    :cvar str runner_cls:
        The runner class to use.

    =============================
    Triggering Configuration
    =============================
    :cvar set[str] trigger_task_modules:
        Set of module names (as strings) containing tasks with trigger conditions (e.g., cron, event, status).
        These modules will be imported by runners at startup to ensure all trigger-dependent tasks are registered.
        Only modules listed here will have their trigger tasks considered for automatic execution.
        Example: {"myapp.tasks.scheduled", "myapp.tasks.event_driven"}

    =============================
    Development & Logging
    =============================
    :cvar bool dev_mode_force_sync_tasks:
        If True, forces tasks to run synchronously, useful for development.
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
    :cvar bool compact_log_context:
        If True, truncates IDs (first 7 chars) and compacts class names (e.g., PPR for
        PersistentProcessRunner) for shorter log output. Default True.
    :cvar bool log_use_colors:
        Controls ANSI color output in logs. True forces colors on, False disables
        them. Auto-detection follows the convention used by uvicorn, click, and rich. Default True.
    :cvar str log_stream:
        Output stream for the log handler: "stderr" (default, Python convention)
        or "stdout". Container log collectors (GKE, CloudWatch) typically classify
        all stderr output as ERROR severity, so "stdout" is recommended for
        containerized deployments.
    :cvar LogFormat log_format:
        Log output format: TEXT (human-readable, default) or JSON (structured).
        JSON format emits one JSON object per line with severity, timestamp, logger,
        message, and context fields as top-level keys. Recommended for container and
        cloud environments where log aggregators parse structured output.

    =============================
    Atomic Global Services
    =============================
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
    :cvar float atomic_service_execution_retention_minutes:
        Age-based retention for recorded atomic-service execution windows used by
        pynmon. Records whose ``end_time`` is older than this many minutes are
        purged on the next ``finalize_atomic_service_execution`` call. ``0`` keeps
        records forever (still capped by ``atomic_service_execution_max_records``).
        Default is 60.0 minutes.
    :cvar int atomic_service_execution_max_records:
        Capacity-based retention for atomic-service execution windows. When greater
        than zero, only the most recent N records (across all runners) are kept.
        ``0`` disables the capacity check. Default is 1000.
    :cvar float atomic_service_max_start_slot_fraction:
        Fraction of the runner's assigned slot that may elapse between
        ``slot_start`` and the scheduled run's ``started_at`` timestamp before
        the run is aborted as a late start. Values must be in ``(0, 1]``.
        A value of ``1.0`` disables the check. Default is ``0.5`` (abort if more
        than half the slot is consumed before the run begins). Late-start aborts
        are recorded as ``AtomicServiceDecisionReason.LATE_START`` events.
    :cvar float atomic_service_membership_stabilization_minutes:
        How long a newly registered atomic-service-eligible runner stays in the
        scheduling membership set (so its slot is reserved) without being
        eligible to start runs. The runner is recognised in slot assignment
        from its first heartbeat but cannot start a scheduled run until
        ``now - creation_time >= stabilization_minutes``. A
        value of ``0`` disables the grace window (runners are runnable as
        soon as they appear). Slots assigned to runners still in the
        grace window emit ``AtomicServiceDecisionReason.SCHEDULED_RUNNER_IN_GRACE``
        events. Default ``0.0`` minutes (disabled).
    :cvar float atomic_service_min_run_margin_seconds:
        Minimum number of seconds that must remain in the runner's slot
        between the ``RUNNING`` write and ``slot_end`` for the run to
        actually execute. If the remaining margin is below this value the
        runner immediately marks the execution ``ABANDONED:no_margin``
        without doing any work. This prevents a runner that barely scraped
        into its slot from overrunning into the next runner's window.
        Default ``0.05`` seconds. Set to ``0`` to disable the check.

    =============================
    Recovery & Heartbeat
    =============================
    :cvar str recover_pending_invocations_cron:
        Cron expression defining how often to run the recover_pending_invocations core task.
    :cvar float max_pending_seconds:
        Maximum time in seconds a task can remain in PENDING state before it expires.
        See :class:`~pynenc.invocation.status.InvocationStatus` for more details.
    :cvar str recover_running_invocations_cron:
        Cron expression defining how often to run the recover_running_invocations core task.
    :cvar float runner_considered_dead_after_minutes:
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

    # Core Classes Configuration
    orchestrator_cls = ConfigField("MemOrchestrator")
    trigger_cls = ConfigField("MemTrigger")
    broker_cls = ConfigField("MemBroker")
    state_backend_cls = ConfigField("MemStateBackend")
    serializer_cls = ConfigField("JsonPickleSerializer")
    client_data_store_cls = ConfigField("MemClientDataStore")
    runner_cls = ConfigField("DummyRunner")

    # Trigger task module declaration (if using triggers)
    trigger_task_modules: ConfigField[set[str]] = ConfigField(set())

    # Development Mode
    dev_mode_force_sync_tasks = ConfigField(False)

    # Logging Configuration
    logging_level = ConfigField("info")
    print_arguments = ConfigField(True)
    truncate_arguments_length = ConfigField(32)
    argument_print_mode = ConfigField(ArgumentPrintMode.TRUNCATED)
    cached_status_time = ConfigField(0.1)
    compact_log_context = ConfigField(True)
    log_use_colors = ConfigField(True)
    log_stream = ConfigField("stderr")
    log_format = ConfigField(LogFormat.TEXT)

    # Atomic Global Services Configuration
    atomic_service_interval_minutes = ConfigField(5.0)
    atomic_service_spread_margin_minutes = ConfigField(1.0)
    atomic_service_check_interval_minutes = ConfigField(0.5)
    atomic_service_execution_retention_minutes = ConfigField(60.0)
    atomic_service_execution_max_records = ConfigField(1000)
    atomic_service_max_start_slot_fraction = ConfigField(0.5)
    atomic_service_membership_stabilization_minutes = ConfigField(0.0)
    atomic_service_min_run_margin_seconds = ConfigField(0.05)

    # Pending Invocation Recovery Service
    recover_pending_invocations_cron = ConfigField("*/5 * * * *")  # Every 5 minutes
    max_pending_seconds = ConfigField(5.0)

    # Running Invocation Recovery Service
    recover_running_invocations_cron = ConfigField("*/15 * * * *")  # Every 15 minutes
    runner_considered_dead_after_minutes = ConfigField(10.0)
