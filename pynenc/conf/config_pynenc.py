from .config_base import ConfigBase, ConfigField


class ConfigPynenc(ConfigBase):
    """
    Main config of the pynenc app.

    Attributes
    ----------
    dev_mode_force_sync_tasks : ConfigField
        If True, forces tasks to run synchronously, useful for development.
    max_pending_seconds : ConfigField
        Maximum time in seconds a task can remain in PENDING state before it expires.
        See :class:`~pynenc.invocation.status.InvocationStatus` for more details.
    logging_level : ConfigField
        The logging level of the application ('info', 'warning', 'error', etc.).
    """

    dev_mode_force_sync_tasks = ConfigField(False)
    max_pending_seconds = ConfigField(5.0)
    logging_level = ConfigField("info")
