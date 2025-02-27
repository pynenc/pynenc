from cistell import ConfigField

from pynenc.conf.config_base import ConfigPynencBase


class ConfigRunner(ConfigPynencBase):
    """
    Specific configuration settings for any Runner in the system.

    :cvar ConfigField[float] invocation_wait_results_sleep_time_sec:
        Time in seconds that the runner waits before checking again for the results of a dependent invocation.
        This setting is used when an invocation is waiting for the results of another invocation, providing a
        delay between checks to avoid excessive polling.

    :cvar ConfigField[float] runner_loop_sleep_time_sec:
        Time in seconds that the runner sleeps between iterations of its main loop. This delay is used to control
        the frequency at which the runner checks for new tasks to execute, and for any updates on the currently
        waiting tasks.

    :cvar ConfigField[int] min_parallel_slots:
        Minimum number of parallel execution slots for tasks. This setting determines the minimum number of tasks
        that can run in parallel, regardless of the implementation (threads or processes). For instance, on a
        single-core machine, setting this to 2 would allow at least two tasks to run concurrently.
    """

    invocation_wait_results_sleep_time_sec = ConfigField(0.1)
    runner_loop_sleep_time_sec = ConfigField(0.1)
    min_parallel_slots = ConfigField(1)


class ConfigThreadRunner(ConfigRunner):
    """Specific Configuration for the ThreadRunner

    :cvar ConfigField[float] invocation_wait_results_sleep_time_sec:
        Time in seconds that the runner waits before checking again for the results of a dependent invocation.
        This setting is used when an invocation is waiting for the results of another invocation, providing a
        delay between checks to avoid excessive polling.

    :cvar ConfigField[float] runner_loop_sleep_time_sec:
        Time in seconds that the runner sleeps between iterations of its main loop. This delay is used to control
        the frequency at which the runner checks for new tasks to execute, and for any updates on the currently
        waiting tasks.

    :cvar ConfigField[int] min_threads:
        Minimum number of threads that the runner can handle. This setting determines the minimum number of threads
        that the runner can spawn to execute tasks. It is used to ensure that the runner has a minimum number of threads
        available to execute tasks.

    :cvar ConfigField[int] max_threads:
        Maximum number of threads that the runner can handle. This setting determines the maximum number of threads
        that the runner can spawn to execute tasks. It is used to limit the number of threads created by the runner.
        default: 0 will set the max_threads to multiprocessing.cpu_count()
    """

    invocation_wait_results_sleep_time_sec = ConfigField(0.01)
    runner_loop_sleep_time_sec = ConfigField(0.01)
    min_threads = ConfigField(1)
    max_threads = ConfigField(0)


class ConfigMultiThreadRunner(ConfigThreadRunner):
    """Specific Configuration for the MultiThreadRunner

    :cvar ConfigField[int] max_threads:
        Override the default max_threads value from ConfigThreadRunner. In the ThreadRunner, the default value is
        multiprocessing.cpu_count(). But in the MultiThreadRunner, the number of ThreadRunner processes will depend
        by default on the number of CPU cores available. If the max_threads also depends on the CPU by default, may
        generate too much tasks in each ThreadRunner generating unnecessary overhead and memory consumption.

    :cvar ConfigField[int] min_processes:
        Minimum number of processes that the runner can handle. This setting determines the minimum number of processes
        that the runner can spawn to execute ThreadRunners. It is used to ensure that the runner has a minimum number of
        ThreadRunner processes available to execute tasks.

    :cvar ConfigField[int] max_processes:
        Maximum number of processes that the runner can handle. This setting determines the maximum number of processes
        that the runner can spawn to execute ThreadRunners. It is used to limit the number of processes created by the
        runner.
        default: 0 will set the max_processes to multiprocessing.cpu_count()

    :cvar ConfigField[int] idle_timeout_process_sec:
        Time in seconds that the runner waits before killing an idle thread. This setting is used to control the
        behavior of the runner when there are no tasks to execute. If a thread is idle for longer than the specified
        time, the runner will kill the thread to free up resources.

    :cvar ConfigField[bool] enforce_max_processes:
        Flag to enforce the maximum number of threads. If set to True, the runner will keep the number of threads
        spawned to the maximum number of threads specified in the configuration or cpu_count, regardless of the
        threads load. If set to False, the runner will scale the number of threads based on the thread load.
    """

    max_threads = ConfigField(4)
    min_processes = ConfigField(1)
    max_processes = ConfigField(0)
    idle_timeout_process_sec = ConfigField(4)
    enforce_max_processes = ConfigField(False)
