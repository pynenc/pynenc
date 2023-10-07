import logging
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from ..app import Pynenc


def create_logger(app: "Pynenc") -> logging.Logger:
    """Creates a logger for the given app"""
    logger = logging.getLogger(f"pynenc.{app.app_id}")
    if level_name := app.conf.logging_level:
        numeric_level = getattr(logging, level_name.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError(f"Invalid log level: {level_name}")
        logger.setLevel(numeric_level)
    return logger
