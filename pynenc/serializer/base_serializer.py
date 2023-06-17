from abc import ABC, abstractmethod
from typing import Any


class BaseSerializer(ABC):
    @staticmethod
    @abstractmethod
    def serialize(obj: Any) -> str:
        ...

    @staticmethod
    @abstractmethod
    def deserialize(obj: str) -> Any:
        ...
