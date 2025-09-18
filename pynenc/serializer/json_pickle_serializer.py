from typing import Any

import jsonpickle

from pynenc.serializer.base_serializer import BaseSerializer


class JsonPickleSerializer(BaseSerializer):
    """
    Serializer using jsonpickle to preserve Python types (NamedTuple, etc.).

    Note: jsonpickle can execute arbitrary code on load for some object types.
    Only use for trusted internal data.
    """

    @staticmethod
    def serialize(obj: Any) -> str:
        """
        Serialize object to string using jsonpickle.

        :param Any obj: Object to serialize
        :return: Serialized string
        """
        return jsonpickle.dumps(obj)

    @staticmethod
    def deserialize(serialized_obj: str) -> Any:
        """
        Deserialize string back to object using jsonpickle.

        :param str serialized_obj: Serialized string
        :return: Deserialized object
        """
        return jsonpickle.loads(serialized_obj)
