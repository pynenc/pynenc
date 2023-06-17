import codecs
import pickle
from typing import Any

from .base_serializer import BaseSerializer


class PickleSerializer(BaseSerializer):
    @staticmethod
    def serialize(obj: Any) -> str:
        return codecs.encode(pickle.dumps(obj), "base64").decode()

    @staticmethod
    def deserialize(serialized_obj: str) -> Any:
        return pickle.loads(codecs.decode(serialized_obj.encode(), "base64"))
