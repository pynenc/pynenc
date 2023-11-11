from .config_base import ConfigBase
from .config_redis import ConfigRedis


class ConfigStateBackend(ConfigBase):
    """Main config of the boker components"""


class ConfigStateBackendRedis(ConfigStateBackend, ConfigRedis):
    """Specific Configuration for the Redis Broker"""
