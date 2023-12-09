import logging
from unittest.mock import MagicMock

import pytest

from pynenc.util.log import create_logger  # Replace with your actual module path


@pytest.fixture
def mock_app() -> MagicMock:
    app = MagicMock()
    app.app_id = "test_app"
    app.conf.logging_level = "info"
    return app


def test_create_logger_valid_level(mock_app: MagicMock) -> None:
    # Call the function with the mock object
    logger = create_logger(mock_app)

    # Assertions
    assert logger.name == "pynenc.test_app"
    assert logger.level == logging.INFO


def test_create_logger_invalid_level(mock_app: MagicMock) -> None:
    # Test with an invalid log level
    mock_app.conf.logging_level = "invalid_level"
    with pytest.raises(ValueError):
        create_logger(mock_app)
