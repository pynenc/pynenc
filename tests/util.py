import hashlib
import logging
import uuid
from contextlib import contextmanager
from io import StringIO
from typing import TYPE_CHECKING, Generator

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest


def get_unique_id(length: int = 16) -> str:
    _id = uuid.uuid4()
    return hashlib.sha256(_id.bytes).hexdigest()[:length]


def get_module_name(request: "FixtureRequest") -> tuple[str, str]:
    """Get the module name and test name from a pytest request."""
    test_name = request.node.name.replace("[", "(").replace("]", ")")
    test_module = request.node.module.__name__
    return test_module, test_name


@contextmanager
def capture_logs(
    logger: logging.Logger, level: int = logging.DEBUG
) -> Generator[StringIO, None, None]:
    """Capture logs from a specific logger."""
    log_buffer = StringIO()
    handler = logging.StreamHandler(log_buffer)
    handler.setFormatter(logger.handlers[0].formatter)
    logger.addHandler(handler)

    original_level = logger.level
    logger.setLevel(level)

    try:
        yield log_buffer
    finally:
        logger.removeHandler(handler)
        logger.setLevel(original_level)
