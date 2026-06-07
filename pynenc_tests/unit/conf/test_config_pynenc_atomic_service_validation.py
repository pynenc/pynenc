import pytest

from pynenc import Pynenc
from pynenc.conf.config_runner import ConfigRunner
from pynenc.conf.validation_atomic_service import (
    AtomicServiceConfigError,
    validate_atomic_service_config,
)


def _conf(**overrides: object):  # type: ignore[no-untyped-def]
    return Pynenc(
        config_values={"app_id": "atomic-service-validation-test", **overrides}
    ).conf


def _runner_conf(**overrides: object) -> ConfigRunner:
    return ConfigRunner(config_values=overrides)


# --- hard failures ---


def test_default_config_is_valid() -> None:
    validate_atomic_service_config(_conf(), _runner_conf())


def test_zero_interval_raises() -> None:
    with pytest.raises(
        AtomicServiceConfigError, match="atomic_service_interval_minutes"
    ):
        validate_atomic_service_config(
            _conf(atomic_service_interval_minutes=0), _runner_conf()
        )


def test_negative_interval_raises() -> None:
    with pytest.raises(
        AtomicServiceConfigError, match="atomic_service_interval_minutes"
    ):
        validate_atomic_service_config(
            _conf(atomic_service_interval_minutes=-1), _runner_conf()
        )


def test_zero_check_interval_raises() -> None:
    with pytest.raises(
        AtomicServiceConfigError, match="atomic_service_check_interval_minutes"
    ):
        validate_atomic_service_config(
            _conf(atomic_service_check_interval_minutes=0), _runner_conf()
        )


def test_spread_margin_equal_to_interval_raises() -> None:
    with pytest.raises(
        AtomicServiceConfigError, match="atomic_service_spread_margin_minutes"
    ):
        validate_atomic_service_config(
            _conf(
                atomic_service_interval_minutes=1.0,
                atomic_service_spread_margin_minutes=1.0,
            ),
            _runner_conf(),
        )


def test_spread_margin_greater_than_interval_raises() -> None:
    with pytest.raises(
        AtomicServiceConfigError, match="atomic_service_spread_margin_minutes"
    ):
        validate_atomic_service_config(
            _conf(
                atomic_service_interval_minutes=1.0,
                atomic_service_spread_margin_minutes=2.0,
            ),
            _runner_conf(),
        )


# --- warnings ---


def test_warns_when_two_runner_slot_is_zero() -> None:
    with pytest.warns(UserWarning, match="two-runner slot"):
        validate_atomic_service_config(
            _conf(
                atomic_service_interval_minutes=1.0,
                atomic_service_spread_margin_minutes=0.5,
            ),
            _runner_conf(),
        )


def test_warns_when_two_runner_slot_is_very_small() -> None:
    with pytest.warns(UserWarning, match="very small"):
        validate_atomic_service_config(
            _conf(
                atomic_service_interval_minutes=0.1,
                atomic_service_spread_margin_minutes=0.0,
                atomic_service_check_interval_minutes=0.01,
            ),
            _runner_conf(),
        )


def test_warns_when_check_interval_exceeds_slot() -> None:
    with pytest.warns(UserWarning, match="conf.atomic_service_check_interval_minutes"):
        validate_atomic_service_config(
            _conf(
                atomic_service_interval_minutes=2.0,
                atomic_service_spread_margin_minutes=0.0,
                atomic_service_check_interval_minutes=1.5,
            ),
            _runner_conf(),
        )


def test_warns_when_runner_loop_sleep_exceeds_slot() -> None:
    with pytest.warns(UserWarning, match="runner_conf.runner_loop_sleep_time_sec"):
        validate_atomic_service_config(
            _conf(
                atomic_service_interval_minutes=2.0,
                atomic_service_spread_margin_minutes=0.0,
            ),
            _runner_conf(runner_loop_sleep_time_sec=90.0),
        )


# --- app construction is still lazy ---


def test_app_instantiation_does_not_validate_atomic_service_config() -> None:
    app = Pynenc(
        config_values={
            "app_id": "invalid-until-runner-start",
            "atomic_service_interval_minutes": 0,
        }
    )

    assert app.conf.atomic_service_interval_minutes == 0
