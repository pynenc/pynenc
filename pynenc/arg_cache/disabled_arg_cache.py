from typing import TYPE_CHECKING, Any

from pynenc.arg_cache.base_arg_cache import BaseArgCache

if TYPE_CHECKING:
    from pynenc.app import Pynenc


class DisabledArgCache(BaseArgCache):
    """
    A no-op implementation of argument caching that passes through to the serializer.

    This is the default implementation used when argument caching is not needed.
    """

    def __init__(self, app: "Pynenc") -> None:
        self.app = app
        # Do not call super().__init__(app) to avoid runner cache initialization

    def serialize(self, obj: Any, disable_cache: bool = False) -> str:
        """Direct passthrough to serializer."""
        return self.app.serializer.serialize(obj)

    def deserialize(self, data: str) -> Any:
        """Direct passthrough to serializer."""
        return self.app.serializer.deserialize(data)

    def _store(self, key: str, value: str) -> None:
        """Not used in disabled cache."""
        raise NotImplementedError("DisabledArgCache does not support storing values.")

    def _retrieve(self, key: str) -> str:
        """Not used in disabled cache."""
        raise NotImplementedError(
            "DisabledArgCache does not support retrieving values."
        )

    def _purge(self) -> None:
        """No-op for disabled cache."""

    def purge(self) -> None:
        """Override to prevent access to runner cache."""

    def is_cache_key(self, value: str) -> bool:
        """Always returns False for disabled cache."""
        return False
