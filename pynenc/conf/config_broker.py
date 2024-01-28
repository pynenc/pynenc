from pynenc.conf.config_base import ConfigBase
from pynenc.conf.config_redis import ConfigRedis


class ConfigBroker(ConfigBase):
    """Main config of the boker components"""


class ConfigBrokerRedis(ConfigBroker, ConfigRedis):
    """Specific Configuration for the Redis Broker"""
