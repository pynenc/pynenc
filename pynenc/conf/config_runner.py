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

    :cvar ConfigField[int] final_invocation_cache_size:
        Maximum number of final invocation entries stored in the runner's local cache (`final_invocations`).
        This cache tracks completed invocations to avoid repeated Redis queries, and when the size exceeds
        this limit, the oldest entries are evicted to maintain bounded memory usage. A larger value reduces
        database pressure but increases memory consumption, while a smaller value saves memory at the cost
        of more frequent status checks.
        Default: 10,000.
    """

    invocation_wait_results_sleep_time_sec = ConfigField(0.01)
    runner_loop_sleep_time_sec = ConfigField(0.01)
    min_threads = ConfigField(1)
    max_threads = ConfigField(0)
    final_invocation_cache_size = ConfigField(10000)


class ConfigMultiThreadRunner(ConfigThreadRunner):
    """Specific Configuration for the MultiThreadRunner

    :cvar ConfigFiled[int] min_threads:
        Defines the default value of the ThreadRunner min_threads configuration. The default value is 1.
        It should scale up by processes not threads, leaving only one thread running per process.

    :cvar ConfigField[int] max_threads:
        Override the default max_threads value from ConfigThreadRunner to 1. In the ThreadRunner, the default value is
        multiprocessing.cpu_count(). But in the MultiThreadRunner, we create new independent ThreadRunners in separate
        processes, so we should limit the number of threads to 1 per process to avoid overhead.

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

    min_threads = ConfigField(1)
    max_threads = ConfigField(1)
    min_processes = ConfigField(1)
    max_processes = ConfigField(0)
    idle_timeout_process_sec = ConfigField(4)
    enforce_max_processes = ConfigField(True)


class ConfigPersistentProcessRunner(ConfigThreadRunner):
    """Specific Configuration for the PersistentProcessRunner

    :cvar ConfigField[int] num_processes:
        Number number of processes that the runner can handle. This setting determines the number of processes
        that the runner will spawn to execute Tasks. It is used to enforce the number of processes created.
        default: 0 will set the max_processes to multiprocessing.cpu_count()
    """

    num_processes = ConfigField(0)
