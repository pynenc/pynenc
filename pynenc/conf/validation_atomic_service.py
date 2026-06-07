"""
Validation for atomic-service scheduling configuration.

Runs once when a real runner starts. Raises AtomicServiceConfigError only when
the configuration makes it impossible for atomic services to execute. Emits
UserWarnings when execution is technically possible but windows are tight enough
to cause frequent skips.
"""

import warnings
from typing import TYPE_CHECKING

from pynenc.exceptions import ConfigError

if TYPE_CHECKING:
    from pynenc.conf.config_pynenc import ConfigPynenc
    from pynenc.conf.config_runner import ConfigRunner


class AtomicServiceConfigError(ConfigError):
    """Raised when atomic-service configuration cannot produce runnable slots."""


# Warn when the two-runner slot shrinks below this threshold.
_WARN_TWO_RUNNER_SLOT_MINUTES = 5.0 / 60.0


def validate_atomic_service_config(
    conf: "ConfigPynenc", runner_conf: "ConfigRunner"
) -> None:
    """
    Raise AtomicServiceConfigError if the atomic-service schedule cannot run.
    Emit UserWarnings when the schedule is tight enough to cause frequent skips.

    Hard failures (raises):
    - atomic_service_interval_minutes <= 0
    - atomic_service_check_interval_minutes <= 0
    - atomic_service_spread_margin_minutes >= atomic_service_interval_minutes

    Warnings:
    - two-runner slot <= 0
    - two-runner slot very small (< _WARN_TWO_RUNNER_SLOT_MINUTES)
    - atomic_service_check_interval_minutes > two-runner slot
    - runner_loop_sleep_time_sec > two-runner slot
    """
    if conf.atomic_service_interval_minutes <= 0:
        raise AtomicServiceConfigError(
            "atomic_service_interval_minutes must be > 0; "
            f"{conf.atomic_service_interval_minutes=}"
        )

    if conf.atomic_service_check_interval_minutes <= 0:
        raise AtomicServiceConfigError(
            "atomic_service_check_interval_minutes must be > 0; "
            f"{conf.atomic_service_check_interval_minutes=}"
        )

    if (
        conf.atomic_service_spread_margin_minutes
        >= conf.atomic_service_interval_minutes
    ):
        raise AtomicServiceConfigError(
            "atomic_service_spread_margin_minutes must be less than "
            "atomic_service_interval_minutes; "
            f"{conf.atomic_service_spread_margin_minutes=}; "
            f"{conf.atomic_service_interval_minutes=}; "
            "no slot remains even for a single runner"
        )

    two_runner_slot_minutes = (
        conf.atomic_service_interval_minutes / 2.0
        - conf.atomic_service_spread_margin_minutes
    )

    if two_runner_slot_minutes <= 0:
        warnings.warn(
            "atomic service: two-runner slot <= 0; "
            f"{conf.atomic_service_interval_minutes=}; "
            f"{conf.atomic_service_spread_margin_minutes=}; "
            "with two or more active runners every execution will be skipped",
            UserWarning,
            stacklevel=2,
        )
    elif two_runner_slot_minutes < _WARN_TWO_RUNNER_SLOT_MINUTES:
        warnings.warn(
            "atomic service: two-runner slot is very small; "
            f"{conf.atomic_service_interval_minutes=}; "
            f"{conf.atomic_service_spread_margin_minutes=}; "
            f"{_WARN_TWO_RUNNER_SLOT_MINUTES=}; "
            "runners may frequently miss their execution window",
            UserWarning,
            stacklevel=2,
        )

    if two_runner_slot_minutes > 0:
        if conf.atomic_service_check_interval_minutes > two_runner_slot_minutes:
            warnings.warn(
                "atomic service: atomic_service_check_interval_minutes exceeds "
                "the two-runner slot; "
                f"{conf.atomic_service_check_interval_minutes=}; "
                f"{conf.atomic_service_interval_minutes=}; "
                f"{conf.atomic_service_spread_margin_minutes=}; "
                "runners are unlikely to catch their window in time",
                UserWarning,
                stacklevel=2,
            )

        if runner_conf.runner_loop_sleep_time_sec > two_runner_slot_minutes * 60.0:
            warnings.warn(
                "atomic service: runner_loop_sleep_time_sec exceeds the two-runner "
                "slot; "
                f"{runner_conf.runner_loop_sleep_time_sec=}; "
                f"{conf.atomic_service_interval_minutes=}; "
                f"{conf.atomic_service_spread_margin_minutes=}; "
                "the runner may poll outside its assigned slot and be skipped",
                UserWarning,
                stacklevel=2,
            )
