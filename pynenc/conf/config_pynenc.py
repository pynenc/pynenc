from pynenc.conf.config_base import ConfigBase, ConfigField


class ConfigPynenc(ConfigBase):
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
    :cvar str runner_cls:
        The runner class to use.
    :cvar bool dev_mode_force_sync_tasks:
        If True, forces tasks to run synchronously, useful for development.
    :cvar float max_pending_seconds:
        Maximum time in seconds a task can remain in PENDING state before it expires.
        See :class:`~pynenc.invocation.status.InvocationStatus` for more details.
    :cvar str logging_level:
        The logging level of the application ('info', 'warning', 'error', etc.).
    """

    app_id = ConfigField("pynenc")
    orchestrator_cls = ConfigField("MemOrchestrator")
    broker_cls = ConfigField("MemBroker")
    state_backend_cls = ConfigField("MemStateBackend")
    serializer_cls = ConfigField("JsonSerializer")
    runner_cls = ConfigField("DummyRunner")
    dev_mode_force_sync_tasks = ConfigField(False)
    max_pending_seconds = ConfigField(5.0)
    logging_level = ConfigField("info")
