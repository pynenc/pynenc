import os
from unittest.mock import patch

from pynenc.conf import config_base
from pynenc.conf.config_broker import ConfigBrokerRedis
from pynenc.conf.config_orchestrator import ConfigOrchestratorRedis
from pynenc.conf.config_redis import ConfigRedis


class TestConfig(config_base.ConfigBase):
    test_value = config_base.ConfigField(0)


def test_env_var() -> None:
    """Test that returns the default"""
    with patch.dict(
        os.environ,
        {
            "PYNENC__TESTCONFIG__TEST_VALUE": "13",
        },
    ):
        conf = TestConfig()
    assert conf.test_value == 13


def test_config_redis() -> None:
    """Test that returns the default"""
    conf_redis = ConfigRedis()
    conf_redis_orchestrator = ConfigOrchestratorRedis()
    conf_redis_broker = ConfigBrokerRedis()
    assert conf_redis.redis_host == "localhost"
    assert conf_redis_orchestrator.redis_host == "localhost"
    assert conf_redis_broker.redis_host == "localhost"
    with patch.dict(
        os.environ,
        {
            "PYNENC__CONFIGREDIS__REDIS_HOST": "redis",
        },
    ):
        conf_redis = ConfigRedis()
        conf_redis_orchestrator = ConfigOrchestratorRedis()
        conf_redis_broker = ConfigBrokerRedis()
    assert conf_redis.redis_host == "redis"
    assert conf_redis_orchestrator.redis_host == "redis"
    assert conf_redis_broker.redis_host == "redis"


def test_general_config_redis() -> None:
    """Test that returns the default"""
    conf_redis = ConfigRedis()
    conf_redis_orchestrator = ConfigOrchestratorRedis()
    conf_redis_broker = ConfigBrokerRedis()
    assert conf_redis.redis_host == "localhost"
    assert conf_redis_orchestrator.redis_host == "localhost"
    assert conf_redis_broker.redis_host == "localhost"
    with patch.dict(
        os.environ,
        {
            "PYNENC__REDIS_HOST": "redis",
        },
    ):
        conf_redis = ConfigRedis()
        conf_redis_orchestrator = ConfigOrchestratorRedis()
        conf_redis_broker = ConfigBrokerRedis()
    assert conf_redis.redis_host == "redis"
    assert conf_redis_orchestrator.redis_host == "redis"
    assert conf_redis_broker.redis_host == "redis"


def test_specific_config_redis_isolated() -> None:
    """Test that returns the default"""
    conf_redis_orchestrator = ConfigOrchestratorRedis()
    assert conf_redis_orchestrator.redis_host == "localhost"
    with patch.dict(
        os.environ,
        {
            "PYNENC__CONFIGORCHESTRATORREDIS__REDIS_HOST": "redis",
        },
    ):
        conf_redis_orchestrator = ConfigOrchestratorRedis()
    assert conf_redis_orchestrator.redis_host == "redis"


def test_specific_config_redis() -> None:
    """Test that returns the default"""
    conf_redis = ConfigRedis()
    conf_redis_orchestrator = ConfigOrchestratorRedis()
    conf_redis_broker = ConfigBrokerRedis()
    assert conf_redis.redis_host == "localhost"
    assert conf_redis_orchestrator.redis_host == "localhost"
    assert conf_redis_broker.redis_host == "localhost"
    with patch.dict(
        os.environ,
        {
            "PYNENC__CONFIGORCHESTRATORREDIS__REDIS_HOST": "redis",
        },
    ):
        conf_redis = ConfigRedis()
        conf_redis_orchestrator = ConfigOrchestratorRedis()
        conf_redis_broker = ConfigBrokerRedis()
    assert conf_redis.redis_host == "localhost"
    assert conf_redis_orchestrator.redis_host == "redis"
    assert conf_redis_broker.redis_host == "localhost"
