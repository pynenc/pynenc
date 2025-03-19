import typing

from typing_extensions import ParamSpec

Params = ParamSpec("Params")
Result = typing.TypeVar("Result")
Func = typing.Callable[Params, Result]
Args = dict[str, typing.Any]
