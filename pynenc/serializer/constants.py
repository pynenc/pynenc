from enum import StrEnum


class ReservedKeys(StrEnum):
    """
    Defines keys reserved for internal use in the serialization process.

    This enumeration ensures that specific keys are uniquely identifiable and not confused with user-defined data.

    :cvar ERROR: Reserved key for storing error information in serialized data.
    """

    ERROR = "__pynenc__std_py_exc__"
    ARG_CACHE = "__pynenc__arg_cache__"
