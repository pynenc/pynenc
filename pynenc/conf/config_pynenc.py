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
        Maximum length for printed arguments. If 0, no truncation. Default 100.
    :cvar ArgumentPrintMode argument_print_mode:
        How to print arguments: FULL (all args), KEYS (only names),
        TRUNCATED (truncated values), HIDDEN (no args). Default TRUNCATED.
    """

    app_id = ConfigField("pynenc")
    orchestrator_cls = ConfigField("MemOrchestrator")
    broker_cls = ConfigField("MemBroker")
    state_backend_cls = ConfigField("MemStateBackend")
    serializer_cls = ConfigField("JsonSerializer")
    arg_cache_cls = ConfigField("DisabledArgCache")
    runner_cls = ConfigField("DummyRunner")
    dev_mode_force_sync_tasks = ConfigField(False)
    max_pending_seconds = ConfigField(5.0)
    logging_level = ConfigField("info")
    print_arguments = ConfigField(True)
    truncate_arguments_length = ConfigField(32)
    argument_print_mode = ConfigField(ArgumentPrintMode.TRUNCATED)
