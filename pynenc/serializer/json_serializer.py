import json
from typing import Any

from .base_serializer import BaseSerializer


class JsonSerializer(BaseSerializer):
    @staticmethod
    def serialize(obj: Any) -> str:
        return json.dumps(obj)

    @staticmethod
    def deserialize(obj: str) -> Any:
        return json.loads(obj)
