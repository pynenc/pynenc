from .config_base import ConfigBase, ConfigField


class ConfigRunner(ConfigBase):
    """Specific Configuration for any Runner"""

    invocation_wait_results_sleep_time_sec = ConfigField(0.1)
    runner_loop_sleep_time_sec = ConfigField(0.1)


class ConfigMemRunner(ConfigRunner):
    """Specific Configuration for the MemRunner"""

    invocation_wait_results_sleep_time_sec = ConfigField(0.01)
    runner_loop_sleep_time_sec = ConfigField(0.01)
