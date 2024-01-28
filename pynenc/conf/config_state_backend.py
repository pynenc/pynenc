from pynenc.conf.config_base import ConfigBase
from pynenc.conf.config_redis import ConfigRedis


class ConfigStateBackend(ConfigBase):
    """Main config of the boker components"""


class ConfigStateBackendRedis(ConfigStateBackend, ConfigRedis):
    """Specific Configuration for the Redis Broker"""
