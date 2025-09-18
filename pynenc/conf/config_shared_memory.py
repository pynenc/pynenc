from cistell import ConfigField

from pynenc.conf.config_base import ConfigPynencBase


class ConfigSharedMemory(ConfigPynencBase):
    """Configuration for shared memory components.

    This configuration provides settings for components that use
    multiprocessing.shared_memory for inter-process communication.
    Primarily used for testing process runners.

    :cvar int shared_memory_size_mb:
        Size of shared memory allocation in megabytes (default: 64MB).
    :cvar str shared_memory_name_prefix:
        Prefix for shared memory segment names (default: "pynenc").
    :cvar float shared_memory_cleanup_timeout_sec:
        Timeout for cleanup operations in seconds (default: 5.0).
    """

    shared_memory_size_mb = ConfigField(10)  # 10MB for testing
    shared_memory_name_prefix = ConfigField("pynenc")
    shared_memory_cleanup_timeout_sec = ConfigField(1.0)  # 1 second
