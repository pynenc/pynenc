from abc import ABC, abstractmethod
from typing import Any, TypeVar

from ..util.subclasses import get_subclass

T = TypeVar("T", bound="BaseConfigOption")


class BaseConfigOption(ABC):
    """Base class for all configuration options."""

    @abstractmethod
    def _to_dict(self) -> dict[str, Any]:
        """Returns a dictionary with the option"""
        pass

    @classmethod
    @abstractmethod
    def _from_dict(cls: type[T], options_dict: dict[str, Any]) -> T:
        """Returns a new option from a dictionary"""
        pass

    def to_dict(self) -> dict[str, Any]:
        """Returns a dictionary with the option"""
        option_dict = self._to_dict()
        option_dict["type"] = self.__class__.__name__
        return option_dict

    @classmethod
    def from_dict(cls: type[T], options_dict: dict[str, Any]) -> T:
        """Returns a new option from a dictionary"""
        _type = options_dict.pop("type")
        subclass = get_subclass(cls, _type)
        return subclass._from_dict(options_dict)
