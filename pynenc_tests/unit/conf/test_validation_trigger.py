import pytest

from pynenc import Pynenc
from pynenc.conf.config_trigger import ConfigTrigger
from pynenc.conf.validation_trigger import TriggerConfigError, validate_trigger_config


def _conf(**overrides: object) -> ConfigTrigger:
    return ConfigTrigger(config_values=overrides)


def test_default_trigger_config_is_valid() -> None:
    validate_trigger_config(_conf())


def test_validation_is_skipped_when_scheduler_is_disabled() -> None:
    validate_trigger_config(
        _conf(
            enable_scheduler=False,
            event_retention_days=0,
        )
    )


def test_event_retention_days_must_keep_at_least_one_day() -> None:
    with pytest.raises(TriggerConfigError, match="event_retention_days"):
        validate_trigger_config(_conf(event_retention_days=0))


def test_zero_scheduler_interval_warns() -> None:
    with pytest.warns(UserWarning, match="conf.scheduler_interval_seconds"):
        validate_trigger_config(_conf(scheduler_interval_seconds=0))


def test_event_max_records_one_warns() -> None:
    with pytest.warns(UserWarning, match="conf.event_max_records"):
        validate_trigger_config(_conf(event_max_records=1))


def test_trigger_run_max_records_one_warns() -> None:
    with pytest.warns(UserWarning, match="conf.trigger_run_max_records"):
        validate_trigger_config(_conf(trigger_run_max_records=1))


def test_trigger_conf_validates_when_cached_first_time() -> None:
    app = Pynenc(
        config_values={
            "app_id": "invalid-trigger-config-test",
            "event_retention_days": 0,
        }
    )

    with pytest.raises(TriggerConfigError, match="event_retention_days"):
        _ = app.trigger.conf


def test_app_instantiation_does_not_validate_trigger_config() -> None:
    app = Pynenc(
        config_values={
            "app_id": "invalid-trigger-config-test",
            "event_retention_days": 0,
        }
    )

    assert app.app_id == "invalid-trigger-config-test"
