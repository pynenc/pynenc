from functools import cached_property
import hashlib
import inspect
from typing import Any, Generic, TYPE_CHECKING

if TYPE_CHECKING:
    from .types import Func, Args


class Arguments:
    def __init__(self, func: "Func", *args: Any, **kwargs: Any) -> None:
        sig = inspect.signature(func)
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()
        self.kwargs: "Args" = bound_args.arguments

    @cached_property
    def args_id(self) -> str:
        """Generate a unique id for these arguments"""
        sorted_items = sorted(self.kwargs.items())
        args_str = "".join([f"{k}:{v}" for k, v in sorted_items])
        return hashlib.sha256(args_str.encode()).hexdigest()

    def __hash__(self) -> int:
        return hash(self.args_id)

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, Arguments):
            return False
        return self.args_id == __value.args_id
