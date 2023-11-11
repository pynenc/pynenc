from .config_base import ConfigBase, ConfigField


class ConfigPynenc(ConfigBase):
    """Main config of the pynenc app"""

    dev_mode_force_sync_tasks = ConfigField(False)
    max_pending_seconds = ConfigField(5.0)
    logging_level = ConfigField("info")
