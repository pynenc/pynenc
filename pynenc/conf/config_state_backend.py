from pynenc.conf.config_base import ConfigPynencBase
from pynenc.conf.config_redis import ConfigRedis


class ConfigStateBackend(ConfigPynencBase):
    """Main config of the boker components"""


class ConfigStateBackendRedis(ConfigStateBackend, ConfigRedis):
    """Specific Configuration for the Redis Broker"""
