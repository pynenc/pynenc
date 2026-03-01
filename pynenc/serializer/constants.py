from enum import StrEnum


class ReservedKeys(StrEnum):
    """
    Defines keys reserved for internal use in the serialization process.

    This enumeration ensures that specific keys are uniquely identifiable
    and not confused with user-defined data.

    :cvar ERROR: Reserved key for storing builtin exception information in serialized data.
    :cvar CLIENT_DATA: Key prefix for client data store references.
    :cvar CLIENT_EXCEPTION: Reserved key for non-builtin (user-defined) exceptions,
        carrying module and qualname for full round-trip reconstruction.
    """

    ERROR = "__pynenc__std_py_exc__"
    CLIENT_DATA = "__pynenc__client_data__"
    JSON_SERIALIZABLE = "__pynenc__json_serializable__"
    ENUM = "__pynenc__enum__"
    CLIENT_EXCEPTION = "__pynenc__client_exception__"
