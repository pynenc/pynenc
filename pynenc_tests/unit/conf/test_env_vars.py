import os
from unittest.mock import patch

from cistell import ConfigField

from pynenc.conf import config_base


class SomeConfig(config_base.ConfigPynencBase):
    test_value = ConfigField(0)


class MockDbConfig(config_base.ConfigPynencBase):
    db_host = ConfigField("localhost")
    db_port = ConfigField(6379)
    db_name = ConfigField(0)


class MockOrchestratorConfig(config_base.ConfigPynencBase):
    max_pending_invocations = ConfigField(100)


class MockBrokerConfig(config_base.ConfigPynencBase):
    queue_timeout_sec = ConfigField(0.1)


class MockOrchestratorDb(MockOrchestratorConfig, MockDbConfig):
    """Mock configuration combining orchestrator and database configs."""

    pass


class MockBrokerDb(MockBrokerConfig, MockDbConfig):
    """Mock configuration combining broker and database configs."""

    pass


def test_env_var_should_override_default_value_when_env_var_set() -> None:
    """Test that environment variable overrides the default value."""
    with patch.dict(
        os.environ,
        {
            "PYNENC__SOMECONFIG__TEST_VALUE": "13",
        },
    ):
        conf = SomeConfig()
    assert conf.test_value == 13


def test_config_db_should_use_default_values_when_no_env_vars() -> None:
    """Test that database configs return default values."""
    conf_db = MockDbConfig()
    conf_db_orchestrator = MockOrchestratorDb()
    conf_db_broker = MockBrokerDb()

    assert conf_db.db_host == "localhost"
    assert conf_db_orchestrator.db_host == "localhost"
    assert conf_db_broker.db_host == "localhost"


def test_config_db_should_use_env_var_when_specific_class_env_var_set() -> None:
    """Test that specific class environment variables override defaults."""
    with patch.dict(
        os.environ,
        {
            "PYNENC__MOCKDBCONFIG__DB_HOST": "database",
        },
    ):
        conf_db = MockDbConfig()
        conf_db_orchestrator = MockOrchestratorDb()
        conf_db_broker = MockBrokerDb()

    assert conf_db.db_host == "database"
    assert conf_db_orchestrator.db_host == "database"
    assert conf_db_broker.db_host == "database"


def test_general_config_db_should_use_general_env_var_when_set() -> None:
    """Test that general environment variables apply to all database configs."""
    conf_db = MockDbConfig()
    conf_db_orchestrator = MockOrchestratorDb()
    conf_db_broker = MockBrokerDb()

    assert conf_db.db_host == "localhost"
    assert conf_db_orchestrator.db_host == "localhost"
    assert conf_db_broker.db_host == "localhost"

    with patch.dict(
        os.environ,
        {
            "PYNENC__DB_HOST": "database",
        },
    ):
        conf_db = MockDbConfig()
        conf_db_orchestrator = MockOrchestratorDb()
        conf_db_broker = MockBrokerDb()

    assert conf_db.db_host == "database"
    assert conf_db_orchestrator.db_host == "database"
    assert conf_db_broker.db_host == "database"


def test_specific_config_db_isolated_should_only_affect_target_class() -> None:
    """Test that specific config only affects the targeted class."""
    conf_db_orchestrator = MockOrchestratorDb()
    assert conf_db_orchestrator.db_host == "localhost"

    with patch.dict(
        os.environ,
        {
            "PYNENC__MOCKORCHESTRATORDB__DB_HOST": "database",
        },
    ):
        conf_db_orchestrator = MockOrchestratorDb()

    assert conf_db_orchestrator.db_host == "database"


def test_specific_config_db_should_only_affect_target_class_not_others() -> None:
    """Test that specific config inheritance works correctly."""
    conf_db = MockDbConfig()
    conf_db_orchestrator = MockOrchestratorDb()
    conf_db_broker = MockBrokerDb()

    assert conf_db.db_host == "localhost"
    assert conf_db_orchestrator.db_host == "localhost"
    assert conf_db_broker.db_host == "localhost"

    with patch.dict(
        os.environ,
        {
            "PYNENC__MOCKORCHESTRATORDB__DB_HOST": "database",
        },
    ):
        conf_db = MockDbConfig()
        conf_db_orchestrator = MockOrchestratorDb()
        conf_db_broker = MockBrokerDb()

    assert conf_db.db_host == "localhost"
    assert conf_db_orchestrator.db_host == "database"
    assert conf_db_broker.db_host == "localhost"


def test_multi_inheritance_should_combine_fields_from_parent_classes() -> None:
    """Test that multi-inheritance correctly combines configuration fields."""
    conf_orchestrator_db = MockOrchestratorDb()
    conf_broker_db = MockBrokerDb()

    # Should have fields from both parent classes
    assert hasattr(conf_orchestrator_db, "db_host")
    assert hasattr(conf_orchestrator_db, "max_pending_invocations")
    assert hasattr(conf_broker_db, "db_host")
    assert hasattr(conf_broker_db, "queue_timeout_sec")

    # Default values should be correct
    assert conf_orchestrator_db.db_host == "localhost"
    assert conf_orchestrator_db.max_pending_invocations == 100
    assert conf_broker_db.db_host == "localhost"
    assert conf_broker_db.queue_timeout_sec == 0.1
