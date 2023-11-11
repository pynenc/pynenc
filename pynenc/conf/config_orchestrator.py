from .config_base import ConfigBase, ConfigField
from .config_redis import ConfigRedis


class ConfigOrchestrator(ConfigBase):
    """Main config of the orchestrator components"""

    cycle_control = ConfigField(True)
    blocking_control = ConfigField(True)
    auto_final_invocation_purge_hours = ConfigField(24.0)


class ConfigOrchestratorRedis(ConfigOrchestrator, ConfigRedis):
    """Specific Configuration for the Redis Orchestrator"""
