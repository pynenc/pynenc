from enum import StrEnum


class ReservedKeys(StrEnum):
    """
    Defines keys reserved for internal use in the serialization process.

    This enumeration ensures that specific keys are uniquely identifiable
    and not confused with user-defined data.

    :cvar ERROR: Reserved key for storing error information in serialized data.
    :cvar CLIENT_DATA: Key prefix for client data store references.
    """

    ERROR = "__pynenc__std_py_exc__"
    CLIENT_DATA = "__pynenc__client_data__"
