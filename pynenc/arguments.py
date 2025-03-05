import hashlib
import inspect
from functools import cached_property
from typing import TYPE_CHECKING, Any, Optional

from pynenc.conf.config_pynenc import ArgumentPrintMode

if TYPE_CHECKING:
    from pynenc import Pynenc
    from pynenc.conf.config_pynenc import ConfigPynenc
    from pynenc.types import Args, Func


class Arguments:
    """
    Represents a distinctive set of arguments used in a task call.

    This class facilitates the handling of arguments by providing functionalities such as
    generating a unique identifier for a set of arguments, and supporting hashability and equality checks.

    :param Optional[Args] kwargs:
        Keyword arguments to initialize the Arguments object.
        Defaults to an empty dictionary if None is provided.
    """

    def __init__(
        self, kwargs: Optional["Args"] = None, *, app: Optional["Pynenc"] = None
    ) -> None:
        self.kwargs: "Args" = kwargs or {}
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

    @cached_property
    def args_id(self) -> str:
        """
        Generates a unique identifier for the set of arguments.

        The identifier is a SHA-256 hash of the sorted argument names and values, ensuring uniqueness.

        :return: A string representing the unique identifier of the arguments.
        """
        if not self.kwargs:
            return "no_args"
        sorted_items = sorted(self.kwargs.items())
        args_str = "".join([f"{k}:{v}" for k, v in sorted_items])
        return hashlib.sha256(args_str.encode()).hexdigest()

    def __hash__(self) -> int:
        return hash(self.args_id)

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, Arguments):
            return False
        return self.args_id == __value.args_id

    def _format_value(self, conf: "ConfigPynenc", value: Any) -> str:
        """Format a single argument value based on configuration."""
        str_value = str(value)
        if not conf.truncate_arguments_length:
            return str_value
        if len(str_value) <= conf.truncate_arguments_length:
            return str_value
        return f"{str_value[:conf.truncate_arguments_length]}..."

    def __str__(self) -> str:
        if not self._app:
            # Fallback behavior when no app is available
            return f"args({', '.join(f'{k}=...' for k in self.kwargs)})"
        conf = self._app.conf

        if not conf.print_arguments:
            return "<arguments hidden>"

        if not self.kwargs:
            return "<no_args>"

        mode = conf.argument_print_mode
        if mode == ArgumentPrintMode.HIDDEN:
            return "<arguments hidden>"

        if mode == ArgumentPrintMode.KEYS:
            return f"args({', '.join(self.kwargs.keys())})"

        items = []
        for k, v in self.kwargs.items():
            if mode == ArgumentPrintMode.FULL:
                items.append(f"{k}={v}")
            else:  # TRUNCATED
                items.append(f"{k}={self._format_value(conf, v)}")

        return f"args({', '.join(items)})"

    def __repr__(self) -> str:
        return f"Arguments({self.kwargs})"
