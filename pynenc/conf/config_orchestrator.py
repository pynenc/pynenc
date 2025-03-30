from cistell import ConfigField

from pynenc.conf.config_base import ConfigPynencBase
from pynenc.conf.config_redis import ConfigRedis


class ConfigOrchestrator(ConfigPynencBase):
    """
    Main config of the orchestrator components.

    :cvar ConfigField[bool] cycle_control:
        This boolean flag enables the orchestrator to detect cycles of calls to subtasks.
        For example, if task1 calls task2 and task2 calls back to task1, this can create
        an endless loop. The cycle control functionality is enabled by default to prevent
        such scenarios. Users can choose to disable it if needed.

    :cvar ConfigField[bool] blocking_control:
        This boolean flag activates control over tasks that are blocking on other tasks.
        If a task invocation is waiting on another invocation, it notifies the runner,
        which temporarily removes it from the processing queue and uses the slot for
        another task invocation. Once the required invocation finishes, the dependent
        invocation is placed back into the run queue. This feature also prioritizes
        invocations that have many dependencies over new ones, ensuring efficient
        task management.

    :cvar  ConfigField[float] auto_final_invocation_purge_hours:
        This float value, defaulting to 24.0 hours, sets the duration after which the
        orchestrator purges all invocations older than the specified time. This purge
        mechanism helps keep the orchestrator lightweight and fast, as it should
        ideally operate with minimal latency. Detailed information about the invocations
        is stored in the result backend, which can handle more data and afford to be slower.
    """

    cycle_control = ConfigField(True)
    blocking_control = ConfigField(True)
    auto_final_invocation_purge_hours = ConfigField(24.0)


class ConfigOrchestratorRedis(ConfigOrchestrator, ConfigRedis):
    """Specific Configuration for the Redis Orchestrator

    :cvar ConfigField[int] max_pending_resolution_threads:
        This integer value controls the maximum number of worker threads used by the
        ThreadPoolExecutor for resolving PENDING status invocations. When an invocation
        enters PENDING state, a background thread is used to check if it needs to be
        resolved (when the pending timeout expires). This limits the number of concurrent
        Redis connections used for this process, helping to prevent connection pool
        exhaustion while still allowing enough parallelism to handle many pending
        invocations. Default is 50 threads.

    :cvar ConfigField[int] redis_retry_max_attempts:
        Maximum number of retry attempts for Redis operations that encounter specific
        exceptions such as StatusNotFound. After this many failed attempts, the
        operation will raise the exception. Default is 3 attempts.

    :cvar ConfigField[float] redis_retry_base_delay_sec:
        Base delay in seconds between retry attempts. The retry system uses
        exponential backoff, where each retry waits progressively longer
        (base_delay * 2^attempt). Default is 0.1 seconds.

    :cvar ConfigField[float] redis_retry_max_delay_sec:
        Maximum delay between retry attempts in seconds, regardless of the
        exponential backoff calculation. This ensures that even after many
        retries, the delay won't exceed a reasonable upper bound. Default is 1.0 second.
    """

    max_pending_resolution_threads = ConfigField(50)
    redis_retry_max_attempts = ConfigField(3)
    redis_retry_base_delay_sec = ConfigField(0.1)
    redis_retry_max_delay_sec = ConfigField(1.0)
