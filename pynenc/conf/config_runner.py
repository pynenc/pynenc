from .config_base import ConfigBase, ConfigField


class ConfigRunner(ConfigBase):
    """
    Specific configuration settings for any Runner in the system.

    Attributes
    ----------
    invocation_wait_results_sleep_time_sec : ConfigField[float]
        Time in seconds that the runner waits before checking again for the results of a dependent invocation.
        This setting is used when an invocation is waiting for the results of another invocation, providing a
        delay between checks to avoid excessive polling.

    runner_loop_sleep_time_sec : ConfigField[float]
        Time in seconds that the runner sleeps between iterations of its main loop. This delay is used to control
        the frequency at which the runner checks for new tasks to execute, and for any updates on the currently
        waiting tasks.

    min_parallel_slots : ConfigField[int]
        Minimum number of parallel execution slots for tasks. This setting determines the minimum number of tasks
        that can run in parallel, regardless of the implementation (threads or processes). For instance, on a
        single-core machine, setting this to 2 would allow at least two tasks to run concurrently.
    """

    invocation_wait_results_sleep_time_sec = ConfigField(0.1)
    runner_loop_sleep_time_sec = ConfigField(0.1)
    min_parallel_slots = ConfigField(1)


class ConfigThreadRunner(ConfigRunner):
    """Specific Configuration for the ThreadRunner"""

    invocation_wait_results_sleep_time_sec = ConfigField(0.01)
    runner_loop_sleep_time_sec = ConfigField(0.01)
