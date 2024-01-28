import codecs
import pickle
from typing import Any

from pynenc.serializer.base_serializer import BaseSerializer


class PickleSerializer(BaseSerializer):
    """
    Implements the BaseSerializer for serialization using Python's pickle module.

    This class provides methods to serialize and deserialize objects to and from strings using pickle,
    with the serialized data encoded in base64 for safe text transmission.
    """

    @staticmethod
    def serialize(obj: Any) -> str:
        """
        Serializes an object into a string using pickle and encodes it in base64.

        :param Any obj: The object to serialize.
        :return: A base64 encoded string representation of 'obj'.
        """
        return codecs.encode(pickle.dumps(obj), "base64").decode()

    @staticmethod
    def deserialize(serialized_obj: str) -> Any:
        """
        Deserializes a base64 encoded string back into an object using pickle.

        :param str serialized_obj: The base64 encoded string to deserialize.
        :return: The deserialized object.
        """
        return pickle.loads(codecs.decode(serialized_obj.encode(), "base64"))
