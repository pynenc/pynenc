from .config_base import ConfigBase
from .config_redis import ConfigRedis


class ConfigBroker(ConfigBase):
    """Main config of the boker components"""


class ConfigBrokerRedis(ConfigBroker, ConfigRedis):
    """Specific Configuration for the Redis Broker"""
