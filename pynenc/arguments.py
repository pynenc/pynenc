import inspect
from typing import TYPE_CHECKING, Any

from pynenc.conf.config_pynenc import ArgumentPrintMode, ConfigPynenc

if TYPE_CHECKING:
    from pynenc import Pynenc
    from pynenc.types import Args, Func


class Arguments:
    """
    Represents a distinctive set of arguments used in a task call.

    This class facilitates the handling of arguments by providing functionalities such as
    generating a unique identifier for a set of arguments, and supporting hashability and equality checks.

    :param Args | None kwargs:
        Keyword arguments to initialize the Arguments object.
        Defaults to an empty dictionary if None is provided.
    """

    def __init__(
        self, kwargs: "Args | None" = None, *, app: "Pynenc | None" = None
    ) -> None:
        self.kwargs: Args = kwargs or {}
        self._app = app

    @classmethod
    def from_call(cls, func: "Func", *args: Any, **kwargs: Any) -> "Arguments":
        """
        Creates an Arguments object from a function call.

        It uses inspect.signature to determine the function's signature and binds the provided arguments to it.

        :param Func func: The function whose arguments are to be processed.
        :param Any args: Positional arguments of the function call.
        :param Any kwargs: Keyword arguments of the function call.
        :return: An instance of Arguments representing the arguments of the call.
        """
        sig = inspect.signature(func)
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()
        return cls(bound_args.arguments)

    def __eq__(self, __value: object) -> bool:
        raise NotImplementedError(
            "Arguments objects are not meant to be compared for equality. "
            "Use the args_id property of the associated Call object for comparison instead."
        )

    def _format_value(self, conf: "ConfigPynenc", value: Any) -> str:
        """Format a single argument value based on configuration."""
        str_value = str(value)
        if not conf.truncate_arguments_length:
            return str_value
        if len(str_value) <= conf.truncate_arguments_length:
            return str_value
        return f"{str_value[: conf.truncate_arguments_length]}..."

    def __str__(self) -> str:
        if not self.kwargs:
            return "<no_args>"

        # Fallback to default config if app is not set
        conf = self._app.conf if self._app else ConfigPynenc()

        if not conf.print_arguments:
            return "<arguments hidden>"

        mode = conf.argument_print_mode
        if mode == ArgumentPrintMode.HIDDEN:
            return "<arguments hidden>"

        if mode == ArgumentPrintMode.KEYS:
            return "{" + ", ".join(self.kwargs.keys()) + "}"
        items = []
        for k, v in self.kwargs.items():
            if mode == ArgumentPrintMode.FULL:
                items.append(f"{k}:{v}")
            else:  # TRUNCATED
                items.append(f"{k}:{self._format_value(conf, v)}")

        return "{" + ", ".join(items) + "}"
