from .config_base import ConfigBase, ConfigField


class ConfigRedis(ConfigBase):
    """Specific Configuration for any Redis client"""

    redis_host = ConfigField("localhost")
    redis_port = ConfigField(6379)
    redis_db = ConfigField(0)
