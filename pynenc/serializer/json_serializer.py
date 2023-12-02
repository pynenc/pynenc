import builtins
import json
from enum import StrEnum
from typing import Any

from .base_serializer import BaseSerializer


class ReservedKeys(StrEnum):
    """Keys that are reserved for internal use"""

    ERROR = "__pynenc__std_py_exc__"


class DefaultJSONEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, Exception):
            return {
                ReservedKeys.ERROR.value: {
                    "type": type(obj).__name__,
                    "args": obj.args,
                    "message": str(obj),
                }
            }
        return super().default(obj)


class JsonSerializer(BaseSerializer):
    @staticmethod
    def serialize(obj: Any) -> str:
        return json.dumps(obj, cls=DefaultJSONEncoder)

    @staticmethod
    def deserialize(obj: str) -> Any:
        data = json.loads(obj)
        if isinstance(data, dict):
            if error_data := data.get(ReservedKeys.ERROR.value):
                error_type = error_data["type"]
                error_args = error_data["args"]
                if hasattr(builtins, error_type):
                    return getattr(builtins, error_type)(*error_args)
        return data
