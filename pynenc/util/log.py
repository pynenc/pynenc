import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pynenc.app import Pynenc


def create_logger(app: "Pynenc") -> logging.Logger:
    """
    Creates a logger for the specified app.

    :param Pynenc app: The app instance for which the logger is created.
    :return: The created logger.
    :raises ValueError: If the logging level is invalid.
    """
    logger = logging.getLogger(f"pynenc.{app.app_id}")
    if level_name := app.conf.logging_level:
        numeric_level = getattr(logging, app.conf.logging_level.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError(f"Invalid log level: {level_name}")
        logger.setLevel(numeric_level)
    return logger


class TaskLoggerAdapter(logging.LoggerAdapter):
    """
    Logger adapter for tasks.

    This adapter adds task and invocation context to log messages.
    """

    def __init__(
        self, logger: logging.Logger, task_id: str, invocation_id: str | None = None
    ):
        super().__init__(logger, {})
        self.set_context(task_id, invocation_id)

    def set_context(self, task_id: str, invocation_id: str | None) -> None:
        """
        Sets the context for logging.

        :param str task_id: The ID of the task.
        :param Optional[str] invocation_id: The ID of the invocation.
        """
        self.task_id = task_id
        self.invocation_id = invocation_id

    def process(self, msg: Any, kwargs: Any) -> Any:
        """
        Processes a log message, adding task and invocation context.

        :param Any msg: The log message.
        :param Any kwargs: Additional keyword arguments.
        :return: The processed message.
        """
        if self.invocation_id:
            prefix = f"[{self.task_id}: {self.invocation_id}]"
        else:
            prefix = f"[{self.task_id}]"
        return f"{prefix} {msg}", kwargs


class RunnerLogAdapter(logging.LoggerAdapter):
    """
    Logger adapter for runners.

    This adapter adds runner context to log messages.
    """

    def __init__(self, logger: logging.Logger, runner_id: str):
        super().__init__(logger, {})
        self.runner_id = runner_id

    def process(self, msg: Any, kwargs: Any) -> Any:
        """
        Processes a log message, adding runner context.

        :param Any msg: The log message.
        :param Any kwargs: Additional keyword arguments.
        :return: The processed message.
        """
        prefix = f"[runner: {self.runner_id}]"
        return f"{prefix} {msg}", kwargs
