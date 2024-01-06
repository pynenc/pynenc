from .config_base import ConfigBase, ConfigField


class ConfigPynenc(ConfigBase):
    """
    Main config of the pynenc app.

    Attributes
    ----------
    app_id : str
        The id of the application.
    orchestrator_cls : str
        The orchestrator class to use.
    broker_cls : str
        The broker class to use.
    state_backend_cls : str
        The state backend class to use.
    serializer_cls : str
        The serializer class to use.
    runner_cls : str
        The runner class to use.
    dev_mode_force_sync_tasks : bool
        If True, forces tasks to run synchronously, useful for development.
    max_pending_seconds : float
        Maximum time in seconds a task can remain in PENDING state before it expires.
        See :class:`~pynenc.invocation.status.InvocationStatus` for more details.
    logging_level : str
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
